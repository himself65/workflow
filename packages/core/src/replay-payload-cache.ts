import type { Event, WorkflowRun } from '@workflow/world';
import type { PayloadKey } from './serialization/encryption.js';
import {
  type PreparedReplayPayload,
  prepareReplayPayload,
  type ReplayPayloadPreparer,
} from './serialization.js';

const MAX_MEMOIZED_PRIMITIVE_LENGTH = 4096;
type ReplayPayloadField = 'result' | 'error' | 'payload';
type EncryptionKeySource =
  | PayloadKey
  | undefined
  | Promise<PayloadKey | undefined>;

interface ScheduledPreparation {
  value: Uint8Array;
  resolve: (value: PreparedReplayPayload) => void;
  reject: (reason?: unknown) => void;
}

function isMemoizablePrimitive(value: unknown): boolean {
  if (value === null) return true;
  const type = typeof value;
  if (type === 'object' || type === 'function') return false;
  if (type === 'string') {
    return (value as string).length <= MAX_MEMOIZED_PRIMITIVE_LENGTH;
  }
  if (type === 'bigint') {
    return (value as bigint).toString().length <= MAX_MEMOIZED_PRIMITIVE_LENGTH;
  }
  return true;
}

/**
 * Invocation-scoped cache for replay payload hydration.
 *
 * A workflow invocation may replay the same event log through several fresh
 * VMs. This cache keeps the VM-independent decrypt/decompress result across
 * those replays. Deserialization still runs against each VM's globals so every
 * replay receives fresh object graphs and correctly revived Workflow objects.
 *
 * Successful prepared plaintext remains resident for the invocation lifetime.
 * Its memory cost is the sum of decrypted and decompressed payload sizes, but
 * it never crosses workflow runs or queue deliveries.
 */
export class ReplayPayloadCache {
  private readonly preparedPayloads = new Map<
    string,
    Promise<PreparedReplayPayload>
  >();
  private readonly pendingPreparations = new Set<
    Promise<PreparedReplayPayload>
  >();
  private readonly primitiveValues = new Map<string, unknown>();
  private readonly scheduledPreparations: ScheduledPreparation[] = [];
  private encryptionKey:
    | { state: 'pending'; promise: Promise<PayloadKey | undefined> }
    | { state: 'ready'; value: PayloadKey | undefined }
    | { state: 'failed'; error: unknown };
  private preparationTurnScheduled = false;
  private nextUnscannedEventIndex = 0;

  constructor(
    encryptionKey: EncryptionKeySource,
    private readonly preparer: ReplayPayloadPreparer = prepareReplayPayload
  ) {
    if (encryptionKey instanceof Promise) {
      this.encryptionKey = { state: 'pending', promise: encryptionKey };
      void encryptionKey.then(
        (value) => {
          this.encryptionKey = { state: 'ready', value };
          this.schedulePreparationTurn();
        },
        (error) => {
          this.encryptionKey = { state: 'failed', error };
          this.rejectScheduledPreparations(error);
        }
      );
    } else {
      this.encryptionKey = { state: 'ready', value: encryptionKey };
    }
  }

  /**
   * Observe one decoded event while its response body is still arriving.
   * Preparation is queued onto a later event-loop turn so frame parsing itself
   * never waits on synchronous AES/zstd work.
   */
  observeEvent(event: Event): Promise<PreparedReplayPayload> | undefined {
    switch (event.eventType) {
      case 'run_created':
        return this.startPreparation(
          this.workflowInputKey(event.runId),
          event.eventData.input
        );
      case 'run_started':
        return event.eventData?.input === undefined
          ? undefined
          : this.startPreparation(
              this.workflowInputKey(event.runId),
              event.eventData.input
            );
      case 'step_completed':
        return this.startPreparation(
          this.eventPayloadKey(event.eventId, 'result'),
          event.eventData?.result
        );
      case 'step_failed':
        return this.startPreparation(
          this.eventPayloadKey(event.eventId, 'error'),
          event.eventData?.error
        );
      case 'hook_received':
        return this.startPreparation(
          this.eventPayloadKey(event.eventId, 'payload'),
          event.eventData?.payload
        );
      default:
        return undefined;
    }
  }

  /**
   * Start every missing binary preparation before workflow execution. Failures
   * are intentionally retained: the ordered event consumer must observe the
   * original rejection before that entry becomes retryable.
   */
  async prewarm(workflowRun: WorkflowRun, events: Event[]): Promise<void> {
    const start = (cacheKey: string, value: unknown): void => {
      // Legacy flattened values may be mutated by devalue's unflatten and are
      // therefore prepared only by their eventual consumer, never cached.
      if (!(value instanceof Uint8Array)) return;

      this.ensurePreparation(cacheKey, value);
    };

    start(this.workflowInputKey(workflowRun.runId), workflowRun.input);
    // This cache is scoped to one invocation. Incremental loads and write
    // response deltas only ever append, so the scanned length locates the
    // events added since the previous replay. A reload that can insert events
    // BELOW that length — a stale-snapshot restart replacing the log with a
    // corrected one — must call `resetScan()` first, or the inserted events are
    // never scanned. Prepared entries stay valid across that: they are keyed by
    // event id, not by position.
    for (
      let index = this.nextUnscannedEventIndex;
      index < events.length;
      index++
    ) {
      const event = events[index];
      switch (event.eventType) {
        case 'step_completed':
          start(
            this.eventPayloadKey(event.eventId, 'result'),
            event.eventData?.result
          );
          break;
        case 'step_failed':
          start(
            this.eventPayloadKey(event.eventId, 'error'),
            event.eventData?.error
          );
          break;
        case 'hook_received':
          start(
            this.eventPayloadKey(event.eventId, 'payload'),
            event.eventData?.payload
          );
          break;
      }
    }
    this.nextUnscannedEventIndex = events.length;

    // Wait only for work that is still pending. Completed cache entries are not
    // re-added on later replay passes, avoiding O(N^2) promise reactions.
    // Failures remain speculative until the ordered consumer asks for them.
    await Promise.allSettled([...this.pendingPreparations]);
  }

  /**
   * Forget how much of the event log has been scanned, so the next
   * {@link prewarm} walks it from the start again.
   *
   * Required before a replay whose event log was reloaded rather than extended:
   * a corrected log inserts the events the previous load was missing, which
   * shifts every later position, so a positional resume would skip exactly the
   * events the reload was for. Already-prepared payloads are kept — they are
   * keyed by event id, so re-scanning re-observes them for free.
   */
  resetScan(): void {
    this.nextUnscannedEventIndex = 0;
  }

  /** Return the workflow input after shared host-side preparation. */
  prepareWorkflowInput(
    workflowRun: WorkflowRun
  ): Promise<PreparedReplayPayload> {
    return this.consumePreparation(
      this.workflowInputKey(workflowRun.runId),
      workflowRun.input
    );
  }

  /**
   * Return an event payload after shared host-side preparation. A rejected
   * preparation is evicted only after this ordered consumer requests it, so a
   * later replay can retry without hiding the original failure.
   */
  prepareEventPayload(
    eventId: string,
    field: ReplayPayloadField,
    value: unknown
  ): Promise<PreparedReplayPayload> {
    return this.consumePreparation(this.eventPayloadKey(eventId, field), value);
  }

  /**
   * Reuse final event values only when sharing them across VMs is unobservable.
   * Objects and large strings/bigints always run `hydrate` again, producing a
   * fresh VM-specific value from the separately cached prepared payload.
   */
  async getPrimitiveValue(
    eventId: string,
    field: ReplayPayloadField,
    hydrate: () => Promise<unknown>
  ): Promise<unknown> {
    const cacheKey = this.eventPayloadKey(eventId, field);
    if (this.primitiveValues.has(cacheKey)) {
      return this.primitiveValues.get(cacheKey);
    }

    const value = await hydrate();
    if (isMemoizablePrimitive(value)) {
      this.primitiveValues.set(cacheKey, value);
    }
    return value;
  }

  /**
   * Consumer-facing lookup. Binary payloads share preparation; legacy values
   * bypass the cache because their flattened representation may be mutated.
   */
  private consumePreparation(
    cacheKey: string,
    value: unknown
  ): Promise<PreparedReplayPayload> {
    if (!(value instanceof Uint8Array)) return this.runPreparation(value);

    const preparation = this.ensurePreparation(cacheKey, value);
    void preparation.catch(() => {
      if (this.preparedPayloads.get(cacheKey) === preparation) {
        this.preparedPayloads.delete(cacheKey);
      }
    });
    return preparation;
  }

  /** Start preparation once and share the exact in-flight promise. */
  private ensurePreparation(
    cacheKey: string,
    value: Uint8Array,
    defer = false
  ): Promise<PreparedReplayPayload> {
    const cached = this.preparedPayloads.get(cacheKey);
    if (cached) return cached;
    if (this.encryptionKey.state === 'failed') {
      const failed = Promise.reject<PreparedReplayPayload>(
        this.encryptionKey.error
      );
      void failed.catch(() => {});
      return failed;
    }

    let resolve!: (value: PreparedReplayPayload) => void;
    let reject!: (reason?: unknown) => void;
    const preparation = new Promise<PreparedReplayPayload>(
      (resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
      }
    );
    this.preparedPayloads.set(cacheKey, preparation);
    this.pendingPreparations.add(preparation);
    // Speculative work may fail before its ordered consumer exists. Attach a
    // handler immediately; consumePreparation still sees the original promise.
    void preparation.catch(() => {});
    void preparation.then(
      () => this.pendingPreparations.delete(preparation),
      () => this.pendingPreparations.delete(preparation)
    );
    const scheduled = { value, resolve, reject };
    if (!defer && this.encryptionKey.state === 'ready') {
      this.launchPreparation(scheduled);
    } else {
      this.scheduledPreparations.push(scheduled);
      this.schedulePreparationTurn();
    }
    return preparation;
  }

  private startPreparation(
    cacheKey: string,
    value: unknown
  ): Promise<PreparedReplayPayload> | undefined {
    if (!(value instanceof Uint8Array)) return undefined;
    return this.ensurePreparation(cacheKey, value, true);
  }

  /** Consumer-only path for legacy non-binary values. */
  private async runPreparation(value: unknown): Promise<PreparedReplayPayload> {
    switch (this.encryptionKey.state) {
      case 'ready':
        return this.preparer(value, this.encryptionKey.value);
      case 'pending':
        return this.preparer(value, await this.encryptionKey.promise);
      case 'failed':
        throw this.encryptionKey.error;
    }
  }

  /**
   * Run a short preparation slice, then yield back to I/O. One response can
   * decode hundreds of frames from a buffered chunk; doing all synchronous
   * decrypt/decompress work inside that callback would manufacture
   * backpressure even when the network had capacity left.
   */
  private schedulePreparationTurn(): void {
    if (
      this.preparationTurnScheduled ||
      this.scheduledPreparations.length === 0 ||
      this.encryptionKey.state !== 'ready'
    ) {
      return;
    }
    this.preparationTurnScheduled = true;
    setImmediate(() => {
      this.preparationTurnScheduled = false;
      if (this.encryptionKey.state !== 'ready') return;

      const startedAt = performance.now();
      let launched = 0;
      while (
        this.scheduledPreparations.length > 0 &&
        launched < 16 &&
        performance.now() - startedAt < 1
      ) {
        const scheduled = this.scheduledPreparations.shift();
        if (!scheduled) break;
        launched++;
        this.launchPreparation(scheduled);
      }
      this.schedulePreparationTurn();
    });
  }

  private launchPreparation(scheduled: ScheduledPreparation): void {
    if (this.encryptionKey.state !== 'ready') {
      this.scheduledPreparations.unshift(scheduled);
      return;
    }
    try {
      const result = this.preparer(scheduled.value, this.encryptionKey.value);
      if (result instanceof Promise) {
        void result.then(scheduled.resolve, scheduled.reject);
      } else {
        scheduled.resolve(result);
      }
    } catch (error) {
      scheduled.reject(error);
    }
  }

  private rejectScheduledPreparations(error: unknown): void {
    for (const scheduled of this.scheduledPreparations.splice(0)) {
      scheduled.reject(error);
    }
  }

  private workflowInputKey(runId: string): string {
    return `run:${runId}:input`;
  }

  private eventPayloadKey(eventId: string, field: ReplayPayloadField): string {
    return `event:${eventId}:${field}`;
  }
}
