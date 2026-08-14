import { type PromiseWithResolvers, withResolvers } from '@workflow/utils';
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

interface PendingPreparation
  extends PromiseWithResolvers<PreparedReplayPayload> {
  value: Uint8Array;
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
  private readonly preparationsWaitingForKey: PendingPreparation[] = [];
  private encryptionKey:
    | { state: 'pending'; promise: Promise<PayloadKey | undefined> }
    | { state: 'ready'; value: PayloadKey | undefined }
    | { state: 'failed'; error: unknown };
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
          this.launchPreparationsWaitingForKey();
        },
        (error) => {
          this.encryptionKey = { state: 'failed', error };
          this.rejectPreparationsWaitingForKey(error);
        }
      );
    } else {
      this.encryptionKey = { state: 'ready', value: encryptionKey };
    }
  }

  /**
   * Observe one decoded event while its response body is still arriving.
   * When the run key is ready, synchronous AES/zstd preparation happens here,
   * directly after frame validation. If key resolution is still in flight,
   * preparation starts synchronously when that shared promise settles.
   */
  observeEvent(
    event: Event,
    onPreparationStart?: () => void
  ): Promise<PreparedReplayPayload> | undefined {
    switch (event.eventType) {
      case 'run_created':
        return this.startPreparation(
          this.workflowInputKey(event.runId),
          event.eventData.input,
          onPreparationStart
        );
      case 'run_started':
        return event.eventData?.input === undefined
          ? undefined
          : this.startPreparation(
              this.workflowInputKey(event.runId),
              event.eventData.input,
              onPreparationStart
            );
      case 'step_completed':
        return this.startPreparation(
          this.eventPayloadKey(event.eventId, 'result'),
          event.eventData?.result,
          onPreparationStart
        );
      case 'step_failed':
        return this.startPreparation(
          this.eventPayloadKey(event.eventId, 'error'),
          event.eventData?.error,
          onPreparationStart
        );
      case 'hook_received':
        return this.startPreparation(
          this.eventPayloadKey(event.eventId, 'payload'),
          event.eventData?.payload,
          onPreparationStart
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
    this.startPreparation(
      this.workflowInputKey(workflowRun.runId),
      workflowRun.input
    );
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
      this.observeEvent(events[index]);
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
    onPreparationStart?: () => void
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

    // Mark the boundary before synchronous AES/zstd starts. When the key is
    // pending, the overlap span deliberately includes that wait.
    onPreparationStart?.();
    if (this.encryptionKey.state === 'ready') {
      try {
        const result = this.preparer(value, this.encryptionKey.value);
        const preparation =
          result instanceof Promise ? result : Promise.resolve(result);
        this.preparedPayloads.set(cacheKey, preparation);
        if (result instanceof Promise) {
          this.trackPending(preparation);
        }
        return preparation;
      } catch (error) {
        const preparation = Promise.reject<PreparedReplayPayload>(error);
        this.preparedPayloads.set(cacheKey, preparation);
        // Speculative synchronous failures may precede their ordered consumer.
        void preparation.catch(() => {});
        return preparation;
      }
    }

    const pending = withResolvers<PreparedReplayPayload>();
    const preparation = pending.promise;
    this.preparedPayloads.set(cacheKey, preparation);
    this.trackPending(preparation);
    this.preparationsWaitingForKey.push({ value, ...pending });
    return preparation;
  }

  private trackPending(preparation: Promise<PreparedReplayPayload>): void {
    this.pendingPreparations.add(preparation);
    // The rejection callback is also the speculative rejection handler;
    // consumePreparation still sees the original promise.
    void preparation.then(
      () => this.pendingPreparations.delete(preparation),
      () => this.pendingPreparations.delete(preparation)
    );
  }

  private startPreparation(
    cacheKey: string,
    value: unknown,
    onPreparationStart?: () => void
  ): Promise<PreparedReplayPayload> | undefined {
    if (!(value instanceof Uint8Array)) return undefined;
    return this.ensurePreparation(cacheKey, value, onPreparationStart);
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

  private launchPreparationsWaitingForKey(): void {
    if (this.encryptionKey.state !== 'ready') return;
    const key = this.encryptionKey.value;
    for (const preparation of this.preparationsWaitingForKey.splice(0)) {
      this.launchPreparation(preparation, key);
    }
  }

  private launchPreparation(
    scheduled: PendingPreparation,
    key: PayloadKey | undefined
  ): void {
    try {
      const result = this.preparer(scheduled.value, key);
      if (result instanceof Promise) {
        void result.then(scheduled.resolve, scheduled.reject);
      } else {
        scheduled.resolve(result);
      }
    } catch (error) {
      scheduled.reject(error);
    }
  }

  private rejectPreparationsWaitingForKey(error: unknown): void {
    for (const scheduled of this.preparationsWaitingForKey.splice(0)) {
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
