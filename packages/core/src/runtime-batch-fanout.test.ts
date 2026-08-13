import {
  type Event,
  SPEC_VERSION_CURRENT,
  type WorkflowRun,
} from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { registerStepFunction } from './private.js';
import { setWorld } from './runtime/world.js';
import { workflowEntrypoint } from './runtime.js';
import { dehydrateWorkflowArguments } from './serialization.js';

vi.mock('@vercel/functions', () => ({
  waitUntil: vi.fn((p: Promise<unknown>) => {
    p.catch(() => {});
  }),
}));

// Collect-mode (v2 full suspension batching): a suspension that produces
// MULTIPLE batchable frames at once — a fan-out of steps, or steps plus a wait
// — is committed as ONE fenced `createBatch` (the Temporal suspension-committing
// analog) rather than one write per frame. This exercises the runtime
// caller-side reorder: assemble the whole frame set, commit once, then dispatch
// (queue the non-inline fan-out steps AFTER the commit, arm the wait
// continuation), and run the inline born-running steps via `preStarted`.
const bodyRuns: Record<string, number> = {
  ffan1: 0,
  ffan2: 0,
  ffan3: 0,
  ffan4: 0,
  ffan5: 0,
  ffanW1: 0,
};
for (const name of Object.keys(bodyRuns)) {
  registerStepFunction(name, async () => {
    bodyRuns[name] += 1;
    return `r-${name}`;
  });
}

const xform = (name: string) =>
  `;globalThis.__private_workflows = new Map();
   globalThis.__private_workflows.set(${JSON.stringify(name)}, ${name});`;

// Two steps launched together (no inter-step await) → ONE suspension with two
// pending steps, both inline-eligible (<= MAX_INLINE_STEPS).
const twoWayFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan1");
  const b = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan2");
  async function workflow() {
    const [x, y] = await Promise.all([a(), b()]);
    return [x, y];
  }${xform('workflow')}`;

// Five steps at once with the default inline cap of 3 → 3 inline born-running
// pairs + 2 bare pending creates, all in one batch; the 2 pending are enqueued
// after the commit.
const fiveWayFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan1");
  const b = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan2");
  const c = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan3");
  const d = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan4");
  const e = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan5");
  async function workflow() {
    return await Promise.all([a(), b(), c(), d(), e()]);
  }${xform('workflow')}`;

// One step plus a sleep launched together → ONE suspension with a step and a
// wait; the batch folds [step_created, step_started, wait_created].
const stepAndWaitFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffanW1");
  const sleep = globalThis[Symbol.for("WORKFLOW_SLEEP")];
  async function workflow() {
    const [x] = await Promise.all([a(), sleep('60s')]);
    return x;
  }${xform('workflow')}`;

// Two sequential steps, THEN a fan-out. a() is the first step (single path);
// b()'s completion is deferred and folded into the c()/d() fan-out's collect
// batch as its leading outcome → a collect batch that carries a leading
// step_completed(b) AND two born-running pairs. Exercises the collect-mode
// send site's positive logicalCreatedAt branch (guard present).
const seqThenFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan1");
  const b = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan2");
  const c = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan3");
  const d = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("ffan4");
  async function workflow() {
    await a();
    await b();
    const [x, y] = await Promise.all([c(), d()]);
    return [x, y];
  }${xform('workflow')}`;

interface BatchCall {
  events: Array<{ eventType: string; correlationId?: string }>;
  params?: {
    sinceCursor?: string;
    stateUpdatedAt?: number;
    requestId?: string;
    expectedRunVersion?: number;
    batchId?: string;
    logicalCreatedAt?: number;
  };
}

async function driveFanout(opts: {
  runId: string;
  source: string;
  withBatch: boolean;
  maxDeliveries?: number;
  batchImpl?: (
    events: any[],
    durable: Event[],
    rec: (data: any) => Event,
    runningStep: (data: any, input?: unknown) => any
  ) => Promise<any>;
}) {
  const { runId, source } = opts;
  const durable: Event[] = [];
  let seq = 0;
  const rec = (data: any): Event => {
    seq += 1;
    const e = {
      eventId: `e-${seq}`,
      runId,
      createdAt: new Date(),
      ...data,
    } as Event;
    durable.push(e);
    return e;
  };
  const runEntity: WorkflowRun = {
    runId,
    workflowName: 'workflow',
    status: 'running',
    input: await dehydrateWorkflowArguments([], runId, undefined, []),
    createdAt: new Date('2024-01-01T00:00:00.000Z'),
    updatedAt: new Date('2024-01-01T00:00:00.000Z'),
    startedAt: new Date('2024-01-01T00:00:00.000Z'),
    deploymentId: 'test-deployment',
    runVersion: 0,
  };

  const runningStep = (data: any, input?: unknown) => ({
    runId,
    stepId: data.correlationId,
    stepName: data.eventData?.stepName,
    status: 'running' as const,
    attempt: 1,
    input: input !== undefined ? input : data.eventData?.input,
    startedAt: new Date(),
    createdAt: new Date(),
    updatedAt: new Date(),
  });

  const create = vi.fn(async (_runId: string, data: any) => {
    if (data.eventType === 'run_started') {
      return { run: runEntity, events: [] as Event[] };
    }
    if (data.eventType === 'step_started') {
      const d = data.eventData as { stepName?: string; input?: unknown };
      if (d?.input !== undefined) {
        rec({
          eventType: 'step_created',
          specVersion: SPEC_VERSION_CURRENT,
          correlationId: data.correlationId,
          eventData: { stepName: d.stepName, input: d.input },
        });
      }
      return {
        event: rec(data),
        step: runningStep(data),
        ...(d?.input !== undefined ? { stepCreated: true } : {}),
      };
    }
    if (data.eventType === 'step_completed') {
      return {
        event: rec(data),
        step: { ...runningStep(data), status: 'completed' as const },
      };
    }
    return { event: rec(data) };
  });

  const batchCalls: BatchCall[] = [];
  const createBatch = vi.fn(
    async (_runId: string, events: any[], params: any) => {
      batchCalls.push({
        events: events.map((e) => ({
          eventType: e.eventType,
          correlationId: e.correlationId,
        })),
        params,
      });
      if (opts.batchImpl) {
        return opts.batchImpl(events, durable, rec, runningStep);
      }
      // Default happy path: apply every event positionally. A step_started
      // carries the born-running entity + `stepCreated` (fresh commit); the
      // folded step_created supplies its input. Non-step frames (wait_created)
      // just record + echo a benign result (never inspected by the caller).
      const inputByStep = new Map<string, unknown>();
      for (const e of events) {
        if (e.eventType === 'step_created') {
          inputByStep.set(e.correlationId, e.eventData?.input);
        }
      }
      const results = events.map((data: any) => {
        if (data.eventType === 'step_completed') {
          return {
            event: rec(data),
            step: { ...runningStep(data), status: 'completed' as const },
          };
        }
        if (data.eventType === 'step_created') {
          return { event: rec(data), step: runningStep(data) };
        }
        if (data.eventType === 'step_started') {
          return {
            event: rec(data),
            step: runningStep(data, inputByStep.get(data.correlationId)),
            stepCreated: true,
          };
        }
        // wait_created / other frames: record, no step semantics.
        return { event: rec(data) };
      });
      return {
        results,
        events: [...durable],
        cursor: `cursor-${durable.length}`,
        hasMore: false,
        runVersion: (params.expectedRunVersion ?? 0) + 1,
      };
    }
  );

  const queued: any[] = [];
  const returns: unknown[] = [];
  const maxDeliveries = opts.maxDeliveries ?? 12;
  const world: any = {
    specVersion: SPEC_VERSION_CURRENT,
    createQueueHandler: vi.fn(
      (_p: string, handler: (m: unknown, md: unknown) => Promise<unknown>) => {
        return async () => {
          const firstInput = {
            input: await dehydrateWorkflowArguments([], runId, undefined, []),
            deploymentId: 'test-deployment',
            workflowName: 'workflow',
            specVersion: SPEC_VERSION_CURRENT,
            executionContext: {},
          };
          for (let attempt = 1; attempt <= maxDeliveries; attempt++) {
            const before = queued.length;
            const ret = await handler(
              attempt === 1
                ? {
                    runId,
                    requestedAt: new Date('2024-01-01T00:00:00.000Z'),
                    runInput: firstInput,
                  }
                : { runId, requestedAt: new Date() },
              {
                requestId: 'req_fanout',
                attempt,
                queueName: '__wkf_workflow_workflow',
                messageId: 'msg_fanout',
              }
            );
            returns.push(ret);
            const terminal = durable.some(
              (e) =>
                e.eventType === 'run_completed' ||
                e.eventType === 'run_failed' ||
                e.eventType === 'run_cancelled'
            );
            if (terminal) break;
            const nacked =
              ret !== null &&
              typeof ret === 'object' &&
              'timeoutSeconds' in (ret as Record<string, unknown>);
            const enqueuedContinuation = queued
              .slice(before)
              .some((m) => m && m.stepId === undefined);
            if (!nacked && !enqueuedContinuation) break;
          }
          return new Response(null, { status: 204 });
        };
      }
    ),
    events: {
      create,
      list: vi.fn(async () => ({
        data: [...durable],
        hasMore: false,
        cursor: `cursor-${durable.length}`,
      })),
    },
    runs: { get: vi.fn(async () => runEntity) },
    queue: vi.fn(async (_name: string, message: any) => {
      queued.push(message);
      return { messageId: null };
    }),
    getEncryptionKeyForRun: vi.fn(async () => undefined),
  };
  if (opts.withBatch) world.events.createBatch = createBatch;

  setWorld(world);
  await workflowEntrypoint(source)(new Request('https://example.test'));

  const created = create.mock.calls.map((c) => c[1] as any);
  return { created, batchCalls, createBatch, durable, returns, queued };
}

const startedStepIds = (created: any[]) => {
  const ids: string[] = [];
  for (const d of created) {
    if (d.eventType === 'step_started' && !ids.includes(d.correlationId)) {
      ids.push(d.correlationId);
    }
  }
  return ids;
};

describe('runtime collect-mode fan-out batching', () => {
  const ORIG_TURBO = process.env.WORKFLOW_TURBO;
  const ORIG_BATCH = process.env.WORKFLOW_BATCH_TRANSITIONS;

  beforeEach(() => {
    process.env.WORKFLOW_TURBO = '0';
    delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    for (const k of Object.keys(bodyRuns)) bodyRuns[k] = 0;
  });
  afterEach(() => {
    if (ORIG_TURBO === undefined) delete process.env.WORKFLOW_TURBO;
    else process.env.WORKFLOW_TURBO = ORIG_TURBO;
    if (ORIG_BATCH === undefined) delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    else process.env.WORKFLOW_BATCH_TRANSITIONS = ORIG_BATCH;
    setWorld(undefined);
    vi.clearAllMocks();
  });

  it('two-way fan-out: commits ONE batch with both born-running pairs and runs both bodies once', async () => {
    const { batchCalls, created, durable } = await driveFanout({
      runId: 'wrun_fanout_two',
      source: twoWayFanout,
      withBatch: true,
    });

    // Exactly one batch, carrying both inline pairs adjacently (no leading
    // outcome — this is the first suspension).
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_created',
      'step_started',
      'step_created',
      'step_started',
    ]);
    // Each pair shares a correlationId; the two steps are distinct.
    const ev = batchCalls[0].events;
    expect(ev[0].correlationId).toBe(ev[1].correlationId);
    expect(ev[2].correlationId).toBe(ev[3].correlationId);
    expect(ev[0].correlationId).not.toBe(ev[2].correlationId);
    // Pure fan-out (first suspension, no deferred leading outcome): the batch
    // has no index-0 completion to pin, so logicalCreatedAt is OMITTED — the
    // server must never mis-stamp a non-outcome primary frame.
    expect(batchCalls[0].params?.logicalCreatedAt).toBeUndefined();
    // Neither inline step issued a separate step_started create (both came from
    // the batch), and both bodies ran exactly once via preStarted.
    expect(startedStepIds(created)).toHaveLength(0);
    expect(bodyRuns.ffan1).toBe(1);
    expect(bodyRuns.ffan2).toBe(1);
    // The run completes: Promise.all resolves once both inline steps are done.
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(true);
  });

  it('leading outcome + fan-out: the collect batch sends logicalCreatedAt = the deferred completion source (step_started(b).createdAt)', async () => {
    // a() single path; b() deferred; the c()/d() fan-out commits a collect
    // batch whose index-0 frame is the deferred step_completed(b). The collect
    // send site (guard present) must carry logicalCreatedAt = the value the
    // synthetic step_completed(b) used = step_started(b).createdAt — same field
    // the discovery replay consumed (pending.syntheticCompleted).
    const { batchCalls, durable } = await driveFanout({
      runId: 'wrun_fanout_leading',
      source: seqThenFanout,
      withBatch: true,
    });

    const leadingBatch = batchCalls.find((b) =>
      b.events.some((e) => e.eventType === 'step_completed')
    );
    expect(leadingBatch).toBeDefined();
    // index-0 is the leading completion, followed by the two born-running pairs.
    expect(leadingBatch!.events[0].eventType).toBe('step_completed');
    const bId = leadingBatch!.events[0].correlationId!;
    const startedB = durable.find(
      (e) => e.eventType === 'step_started' && e.correlationId === bId
    );
    expect(startedB).toBeDefined();
    expect(leadingBatch!.params?.logicalCreatedAt).toBe(
      startedB!.createdAt.getTime()
    );
  });

  it('v2 fence: the fan-out batch carries expectedRunVersion 0 and a bat_ id', async () => {
    const { batchCalls } = await driveFanout({
      runId: 'wrun_fanout_fence',
      source: twoWayFanout,
      withBatch: true,
    });
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].params?.expectedRunVersion).toBe(0);
    expect(batchCalls[0].params?.batchId).toMatch(/^bat_[0-9A-HJKMNP-TV-Z]+$/);
  });

  it('fan-out beyond the inline cap: batches 3 inline pairs + 2 pending creates, enqueues the 2 pending after commit', async () => {
    const { batchCalls, queued } = await driveFanout({
      runId: 'wrun_fanout_five',
      source: fiveWayFanout,
      withBatch: true,
      maxDeliveries: 2,
    });

    expect(batchCalls).toHaveLength(1);
    // 3 born-running pairs (6 events) followed by 2 bare pending creates.
    const types = batchCalls[0].events.map((e) => e.eventType);
    expect(types).toEqual([
      'step_created',
      'step_started',
      'step_created',
      'step_started',
      'step_created',
      'step_started',
      'step_created',
      'step_created',
    ]);
    // The 3 inline bodies ran once each; the 2 buffered fan-out steps did NOT
    // run inline (they were queued to background handlers, which the harness
    // does not dispatch).
    const inlineRuns =
      bodyRuns.ffan1 +
      bodyRuns.ffan2 +
      bodyRuns.ffan3 +
      bodyRuns.ffan4 +
      bodyRuns.ffan5;
    expect(inlineRuns).toBe(3);
    // The 2 pending fan-out steps were enqueued as background step messages
    // (with a stepId) — AFTER the batch made their step_created durable.
    const inlineIds = new Set(
      batchCalls[0].events
        .filter((_e, i) => i % 2 === 1 && i < 6)
        .map((e) => e.correlationId)
    );
    const queuedStepIds = new Set(
      queued.filter((m) => m && m.stepId !== undefined).map((m) => m.stepId)
    );
    expect(queuedStepIds.size).toBe(2);
    for (const id of queuedStepIds) {
      expect(inlineIds.has(id)).toBe(false);
    }
  });

  it('step + wait fan-out: folds [step_created, step_started, wait_created] into one batch and arms the wait continuation', async () => {
    const { batchCalls, queued, durable } = await driveFanout({
      runId: 'wrun_fanout_wait',
      source: stepAndWaitFanout,
      withBatch: true,
      maxDeliveries: 2,
    });

    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_created',
      'step_started',
      'wait_created',
    ]);
    // The step body ran once (via preStarted); the run is NOT complete — it is
    // still waiting on the sleep.
    expect(bodyRuns.ffanW1).toBe(1);
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(false);
    // A wait continuation (an orchestrator message with no stepId) was armed
    // after the commit.
    expect(queued.some((m) => m && m.stepId === undefined)).toBe(true);
  });

  it('already-applied 200 (no stepCreated on any inline started): runs NO body, abandons and nacks', async () => {
    const { created, batchCalls, returns } = await driveFanout({
      runId: 'wrun_fanout_already',
      source: twoWayFanout,
      withBatch: true,
      maxDeliveries: 1,
      // A concurrent/redelivered writer committed the whole fan-out first: echo
      // the current entities but with NO stepCreated on any step_started
      // result. The client must run no body and re-derive from a fresh replay.
      batchImpl: async (events, durable, rec, runningStep) => {
        const inputByStep = new Map<string, unknown>();
        for (const e of events) {
          if (e.eventType === 'step_created') {
            inputByStep.set(e.correlationId, e.eventData?.input);
          }
        }
        for (const e of events) rec(e);
        const results = events.map((data: any) => {
          if (data.eventType === 'step_created') {
            return { eventWasCreated: false, step: runningStep(data) };
          }
          if (data.eventType === 'step_started') {
            return {
              eventWasCreated: false,
              step: runningStep(data, inputByStep.get(data.correlationId)),
            };
          }
          return { eventWasCreated: false };
        });
        return {
          results,
          events: [...durable],
          cursor: `cursor-${durable.length}`,
          hasMore: false,
        };
      },
    });

    expect(batchCalls).toHaveLength(1);
    // No inline body ran, no terminal event, and the invocation nacked
    // (reinvoke) for a fresh replay.
    expect(bodyRuns.ffan1).toBe(0);
    expect(bodyRuns.ffan2).toBe(0);
    expect(created.some((e) => e.eventType === 'run_completed')).toBe(false);
    expect(created.some((e) => e.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });

  it('kill switch (WORKFLOW_BATCH_TRANSITIONS=0): fan-out never batches, runs via single-event writes', async () => {
    process.env.WORKFLOW_BATCH_TRANSITIONS = '0';
    const { createBatch, created, durable } = await driveFanout({
      runId: 'wrun_fanout_off',
      source: twoWayFanout,
      withBatch: true,
    });
    expect(createBatch).not.toHaveBeenCalled();
    // Both steps got their own step_started on the single-event path.
    expect(startedStepIds(created)).toHaveLength(2);
    expect(bodyRuns.ffan1).toBe(1);
    expect(bodyRuns.ffan2).toBe(1);
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(true);
  });

  it('world lacks createBatch: fan-out falls back to the single-event path', async () => {
    const { created, durable } = await driveFanout({
      runId: 'wrun_fanout_no_batch',
      source: twoWayFanout,
      withBatch: false,
    });
    expect(startedStepIds(created)).toHaveLength(2);
    expect(bodyRuns.ffan1).toBe(1);
    expect(bodyRuns.ffan2).toBe(1);
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(true);
  });
});
