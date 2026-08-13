import {
  EntityConflictError,
  RunExpiredError,
  WorkflowWorldError,
} from '@workflow/errors';
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

// Three sequential inline steps. With WORKFLOW_BATCH_TRANSITIONS on, the first
// step stays on the single-write path, the second step's completion is
// deferred, and the second→third transition commits as ONE batch
// [step_completed(s2), step_created(s3), step_started(s3)]. The third step's
// own completion is the run's final step, so it flushes on the single path.
// Body-execution counters so a test can assert exactly-once semantics — a
// step whose claim this invocation did NOT win must never run its body here.
const bodyRuns: Record<string, number> = { bstep1: 0, bstep2: 0, bstep3: 0 };
registerStepFunction('bstep1', async () => {
  bodyRuns.bstep1 += 1;
  return 'r1';
});
registerStepFunction('bstep2', async () => {
  bodyRuns.bstep2 += 1;
  return 'r2';
});
registerStepFunction('bstep3', async () => {
  bodyRuns.bstep3 += 1;
  return 'r3';
});

const xform = (name: string) =>
  `;globalThis.__private_workflows = new Map();
   globalThis.__private_workflows.set(${JSON.stringify(name)}, ${name});`;

const threeStepWorkflow = `const s1 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("bstep1");
  const s2 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("bstep2");
  const s3 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("bstep3");
  async function workflow() {
    const a = await s1();
    const b = await s2();
    const c = await s3();
    return [a, b, c];
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

/**
 * Drive the three-step workflow against a mock World whose durable event log
 * grows as events are created (so the inline replay loop makes progress over
 * its own writes). Turbo is disabled so the non-turbo await-then-run inline
 * path (the batch's home) is exercised.
 *
 * The queue is simulated: any orchestrator continuation the runtime enqueues
 * (a `reinvoke`, i.e. a message with no `stepId`) is redelivered as a fresh
 * non-turbo invocation, up to a bound, so a run that falls back via reinvoke
 * still runs to completion exactly as a real queue would drive it.
 */
async function driveRun(opts: {
  runId: string;
  withBatch: boolean;
  maxDeliveries?: number;
  /** Omit runVersion from the run entity to model a run created before v2. */
  preV2Run?: boolean;
  batchImpl?: (
    events: any[],
    durable: Event[],
    rec: (data: any) => Event,
    runningStep: (data: any, input?: unknown) => any
  ) => Promise<any>;
}) {
  const { runId } = opts;
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
    // v2 fence: a v2 run starts at version 0 (run_created seeds it). The
    // runtime reads this off the loaded run to seed expectedRunVersion; a run
    // WITHOUT runVersion (opts.preV2Run) latches batching off permanently.
    ...(opts.preV2Run ? {} : { runVersion: 0 }),
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
      // Default happy path: apply all events, seed entities (the born-running
      // step carries its input from the folded step_created), return the log as
      // the inline delta.
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
        return {
          event: rec(data),
          step: runningStep(data, inputByStep.get(data.correlationId)),
          stepCreated: true,
        };
      });
      return {
        results,
        events: [...durable],
        cursor: `cursor-${durable.length}`,
        hasMore: false,
        // v2 fence echo: the server sets runVersion to expected+1 on a fresh
        // apply. The runtime advances its local copy to this for the next batch.
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
        // Redelivery loop: deliver the first-delivery message, then keep
        // redelivering any orchestrator continuation the runtime enqueues
        // until the run is terminal or a bound is hit.
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
                requestId: 'req_batch',
                attempt,
                queueName: '__wkf_workflow_workflow',
                messageId: 'msg_batch',
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
            // Redeliver when the invocation asked to be re-run: in non-turbo
            // mode `reinvoke` nacks by returning `{ timeoutSeconds }` (the
            // queue redelivers) rather than self-enqueuing; a broken loop may
            // also enqueue an orchestrator continuation (no stepId).
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
  await workflowEntrypoint(threeStepWorkflow)(
    new Request('https://example.test')
  );

  const created = create.mock.calls.map((c) => c[1] as any);
  return { created, batchCalls, createBatch, durable, returns };
}

const startedFor = (created: any[], stepId: string) =>
  created.filter(
    (d) => d.eventType === 'step_started' && d.correlationId === stepId
  );
const completedFor = (created: any[], stepId: string) =>
  created.filter(
    (d) => d.eventType === 'step_completed' && d.correlationId === stepId
  );
const startedStepIds = (created: any[]) => {
  const ids: string[] = [];
  for (const d of created) {
    if (d.eventType === 'step_started' && !ids.includes(d.correlationId)) {
      ids.push(d.correlationId);
    }
  }
  return ids;
};

describe('runtime batch step transitions', () => {
  const ORIG_TURBO = process.env.WORKFLOW_TURBO;
  const ORIG_BATCH = process.env.WORKFLOW_BATCH_TRANSITIONS;

  beforeEach(() => {
    // Turbo is mutually exclusive with the (await-then-run) batch path.
    process.env.WORKFLOW_TURBO = '0';
    // Batching is default-ON; clearing the var establishes that true default as
    // the baseline for every test, so a test only opts OUT by throwing the
    // kill switch ('0'). No test needs an explicit '1' to enable it.
    delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    bodyRuns.bstep1 = 0;
    bodyRuns.bstep2 = 0;
    bodyRuns.bstep3 = 0;
  });
  afterEach(() => {
    if (ORIG_TURBO === undefined) delete process.env.WORKFLOW_TURBO;
    else process.env.WORKFLOW_TURBO = ORIG_TURBO;
    if (ORIG_BATCH === undefined) delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    else process.env.WORKFLOW_BATCH_TRANSITIONS = ORIG_BATCH;
    setWorld(undefined);
    vi.clearAllMocks();
  });

  it('kill switch (WORKFLOW_BATCH_TRANSITIONS=0): never calls createBatch and keeps the two-POST-per-step pattern', async () => {
    // The emergency kill switch restores the exact single-event two-POST path —
    // byte-identical to pre-feature behavior even when the World implements
    // createBatch. '0' and 'false' both disable (constants.ts parses either);
    // this asserts with '0', the 'false' equivalence is asserted below.
    process.env.WORKFLOW_BATCH_TRANSITIONS = '0';
    const { created, createBatch } = await driveRun({
      runId: 'wrun_batch_off',
      withBatch: true,
    });

    expect(createBatch).not.toHaveBeenCalled();
    const ids = startedStepIds(created);
    expect(ids).toHaveLength(3);
    // Every step: its own step_started AND its own step_completed via create().
    for (const id of ids) {
      expect(startedFor(created, id)).toHaveLength(1);
      expect(completedFor(created, id)).toHaveLength(1);
    }
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  it("kill switch (WORKFLOW_BATCH_TRANSITIONS=false): the literal string 'false' also disables batching", async () => {
    // constants.ts disables on raw === '0' || raw.toLowerCase() === 'false', so
    // the word form must behave identically to '0' — proven, not just asserted
    // in a comment.
    process.env.WORKFLOW_BATCH_TRANSITIONS = 'false';
    const { created, createBatch } = await driveRun({
      runId: 'wrun_batch_off_false',
      withBatch: true,
    });

    expect(createBatch).not.toHaveBeenCalled();
    expect(startedStepIds(created)).toHaveLength(3);
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  it('default (no env var, batching ON): folds the middle transition into one batch [completed, created, started]', async () => {
    // No WORKFLOW_BATCH_TRANSITIONS set (beforeEach cleared it): this asserts the
    // default-ON behavior directly, not an explicitly-enabled path.
    const { created, batchCalls } = await driveRun({
      runId: 'wrun_batch_on',
      withBatch: true,
    });

    // Exactly one batch (the s2 → s3 transition).
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
    ]);
    const [completed, createdEv, started] = batchCalls[0].events;
    // create + start target the same (next) step; completed targets a
    // different (previous) step.
    expect(createdEv.correlationId).toBe(started.correlationId);
    expect(completed.correlationId).not.toBe(started.correlationId);

    const s1 = startedStepIds(created)[0];
    const s2 = completed.correlationId!;
    const s3 = started.correlationId!;

    // s1: single-write path (the first step is never deferred).
    expect(startedFor(created, s1)).toHaveLength(1);
    expect(completedFor(created, s1)).toHaveLength(1);
    // s2: started via create, but its completion came via the batch (deferred).
    expect(startedFor(created, s2)).toHaveLength(1);
    expect(completedFor(created, s2)).toHaveLength(0);
    // s3: started via the batch (NOT a separate step_started create); its
    // completion is the run's final step, flushed on the single path.
    expect(startedFor(created, s3)).toHaveLength(0);
    expect(completedFor(created, s3)).toHaveLength(1);
    // The batch's step_completed rode a sinceCursor for the inline delta.
    expect(typeof batchCalls[0].params?.sinceCursor).toBe('string');
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
    // Positive control for the already-applied test below: on a fresh commit
    // the batch WON s3's claim (stepCreated:true), so its body runs exactly
    // once via preStarted.
    expect(bodyRuns.bstep3).toBe(1);
  });

  it('leading completion: sends logicalCreatedAt = the last durable event createdAt (the synthetic source); honoring it lands completed(N).createdAt == step_started(N).createdAt', async () => {
    // The batched leading step_completed(N) is deferred: the runtime advances
    // its VM past step N during the pre-commit replay by consuming a SYNTHETIC
    // step_completed(N) timestamped with the last durable event's createdAt
    // (= step_started(N).createdAt — the VM clock advances to it). It sends
    // that SAME value as `logicalCreatedAt` on the batch's primary frame, so a
    // server honoring it stamps the durable completion with the identical
    // timestamp the discovery replay (and every later replay) observed. This
    // locks the client half (the sent value == the synthetic's source) AND the
    // coordinated end-state (completed(N).createdAt == step_started(N).createdAt).
    const { batchCalls, durable } = await driveRun({
      runId: 'wrun_batch_logical_ts',
      withBatch: true,
      // Simulate workflow-server#646 honoring logicalCreatedAt: stamp the
      // leading step_completed's durable createdAt from the run's last durable
      // event (which, per the send assertion below, equals logicalCreatedAt),
      // instead of a fresh new Date().
      batchImpl: async (events, dur, recFn, mkStep) => {
        const preBatchLast = dur[dur.length - 1];
        const inputByStep = new Map<string, unknown>();
        for (const e of events) {
          if (e.eventType === 'step_created') {
            inputByStep.set(e.correlationId, e.eventData?.input);
          }
        }
        const results = events.map((data: any) => {
          if (data.eventType === 'step_completed') {
            return {
              event: recFn({ ...data, createdAt: preBatchLast.createdAt }),
              step: { ...mkStep(data), status: 'completed' as const },
            };
          }
          if (data.eventType === 'step_created') {
            return { event: recFn(data), step: mkStep(data) };
          }
          return {
            event: recFn(data),
            step: mkStep(data, inputByStep.get(data.correlationId)),
            stepCreated: true,
          };
        });
        return {
          results,
          events: [...dur],
          cursor: `cursor-${dur.length}`,
          hasMore: false,
          runVersion: 1,
        };
      },
    });

    expect(batchCalls).toHaveLength(1);
    const completed = batchCalls[0].events.find(
      (e) => e.eventType === 'step_completed'
    );
    expect(completed).toBeDefined();
    const s2 = completed!.correlationId!;
    // The last durable event before the batch is step_started(s2) — exactly
    // what the synthetic completion (and thus the VM clock) consumed.
    const startedS2 = durable.find(
      (e) => e.eventType === 'step_started' && e.correlationId === s2
    );
    expect(startedS2).toBeDefined();

    // Client half: the batch carried logicalCreatedAt = step_started(s2)'s
    // createdAt in epoch ms — the same value the synthetic pre-commit
    // completion used. This is the "send byte-equals consume" invariant.
    expect(batchCalls[0].params?.logicalCreatedAt).toBe(
      startedS2!.createdAt.getTime()
    );

    // Coordinated end-state: honoring it, the durable step_completed(s2) landed
    // on step_started(s2)'s createdAt — completed(N).createdAt == started(N).
    const durableCompletedS2 = durable.find(
      (e) => e.eventType === 'step_completed' && e.correlationId === s2
    );
    expect(durableCompletedS2).toBeDefined();
    expect(durableCompletedS2!.createdAt.getTime()).toBe(
      startedS2!.createdAt.getTime()
    );
  });

  it('world lacks createBatch: falls back to the single-POST path entirely', async () => {
    const { created } = await driveRun({
      runId: 'wrun_batch_absent',
      withBatch: false,
    });
    const ids = startedStepIds(created);
    expect(ids).toHaveLength(3);
    for (const id of ids) {
      expect(startedFor(created, id)).toHaveLength(1);
      expect(completedFor(created, id)).toHaveLength(1);
    }
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  it('404 endpoint absent: disables batching for the invocation, flushes, and completes via single POSTs', async () => {
    const { created, createBatch } = await driveRun({
      runId: 'wrun_batch_404',
      withBatch: true,
      batchImpl: async () => {
        throw new WorkflowWorldError('no batch route', { status: 404 });
      },
    });
    // Attempted once; then disabled + reinvoked, and the run completes with
    // every step written via single POSTs.
    expect(createBatch).toHaveBeenCalledTimes(1);
    const ids = startedStepIds(created);
    expect(ids).toHaveLength(3);
    for (const id of ids) {
      expect(completedFor(created, id)).toHaveLength(1);
    }
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  // For 409/410 the batch aborts all-or-nothing: nothing was written, so the
  // runtime must ABANDON the deferred completion and re-derive from a fresh
  // replay (nack), never failing the run and never leaking a partial write.
  // We assert that contract on the failing delivery — a single delivery — since
  // the subsequent re-derivation exercises the ordinary single-POST/owned-
  // recovery paths, which are covered elsewhere.
  it('409 conflict: abandons the deferred completion and nacks without failing the run', async () => {
    const { created, batchCalls, returns } = await driveRun({
      runId: 'wrun_batch_409',
      withBatch: true,
      maxDeliveries: 1,
      batchImpl: async () => {
        throw new EntityConflictError('batch conflict');
      },
    });
    // The batch was attempted with the full transition shape.
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
    ]);
    const deferredStep = batchCalls[0].events[0].correlationId!;
    // All-or-nothing abandonment: the deferred step_completed was NOT committed
    // via any path in this delivery.
    expect(completedFor(created, deferredStep)).toHaveLength(0);
    // The run was not failed; the invocation nacked (reinvoke) for redelivery.
    expect(created.some((d) => d.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });

  it('410 run-not-running: abandons the deferred completion and nacks without failing the run', async () => {
    const { created, batchCalls, returns } = await driveRun({
      runId: 'wrun_batch_410',
      withBatch: true,
      maxDeliveries: 1,
      batchImpl: async () => {
        throw new RunExpiredError('run not running');
      },
    });
    expect(batchCalls).toHaveLength(1);
    const deferredStep = batchCalls[0].events[0].correlationId!;
    expect(completedFor(created, deferredStep)).toHaveLength(0);
    expect(created.some((d) => d.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });

  // Idempotent already-applied 200: a concurrent/redelivered writer committed
  // this exact transition first, so the server returns the current entities
  // with eventWasCreated=false and NO `stepCreated` on the step_started result
  // (workflow-server events.ts:4808-4824) — this invocation did NOT win the
  // create-claim. The client MUST NOT run step N+1's body via `preStarted` (the
  // single-event lazy path skips a lost claim); it abandons and re-derives from
  // a fresh replay, which runs the body only if this invocation truly owns it.
  it('already-applied 200 (no stepCreated): does not run step N+1 body, abandons and nacks', async () => {
    const { created, batchCalls, returns } = await driveRun({
      runId: 'wrun_batch_already_applied',
      withBatch: true,
      maxDeliveries: 1,
      // The winner already committed the whole transition: record its durable
      // writes (as the winner would have) and return the current entities, but
      // with eventWasCreated:false and — critically — NO stepCreated on the
      // step_started result, exactly like resolveStepTransitionBatchCancellation.
      batchImpl: async (events, durable, rec, runningStep) => {
        const inputByStep = new Map<string, unknown>();
        for (const e of events) {
          if (e.eventType === 'step_created') {
            inputByStep.set(e.correlationId, e.eventData?.input);
          }
        }
        for (const e of events) rec(e);
        const results = events.map((data: any) => {
          if (data.eventType === 'step_completed') {
            return {
              eventWasCreated: false,
              step: { ...runningStep(data), status: 'completed' as const },
            };
          }
          if (data.eventType === 'step_created') {
            return { eventWasCreated: false, step: runningStep(data) };
          }
          // step_started (the claim): already-applied => NO stepCreated.
          return {
            eventWasCreated: false,
            step: runningStep(data, inputByStep.get(data.correlationId)),
          };
        });
        return {
          results,
          events: [...durable],
          cursor: `cursor-${durable.length}`,
          hasMore: false,
        };
      },
    });

    // The batch was attempted with the full transition shape.
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
    ]);
    const nextStep = batchCalls[0].events[2].correlationId!;

    // The non-committer neither ran step N+1's body nor flushed its completion:
    // no step_completed(next) via the single path, and the body never executed.
    expect(bodyRuns.bstep3).toBe(0);
    expect(completedFor(created, nextStep)).toHaveLength(0);
    // The run was not completed or failed on this delivery; the invocation
    // abandoned the deferred completion and nacked (reinvoke) for a fresh
    // replay that observes the durable transition and applies ownership logic.
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(false);
    expect(created.some((d) => d.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });

  // ---- v2 suspension-batch fence ----------------------------------------

  it('v2 fence: the batch carries the run version (seeded 0) and a fresh bat_ batchId', async () => {
    const { batchCalls } = await driveRun({
      runId: 'wrun_batch_fence',
      withBatch: true,
    });
    expect(batchCalls).toHaveLength(1);
    // The run loaded at runVersion 0 (run_created seeds it), so the first
    // batch of the invocation asserts expectedRunVersion 0.
    expect(batchCalls[0].params?.expectedRunVersion).toBe(0);
    // A unique per-attempt idempotency id in the reserved bat_ namespace.
    expect(batchCalls[0].params?.batchId).toMatch(/^bat_[0-9A-HJKMNP-TV-Z]+$/);
  });

  it('pre-v2 run (no runVersion): latches batching off for the whole run, completes via single POSTs', async () => {
    // A run created before the fence has no runVersion. The runtime reads that
    // off the loaded run and never attempts a batch — permanently, since
    // pre-v2-ness is immutable and every invocation re-derives the same seed.
    const { created, createBatch } = await driveRun({
      runId: 'wrun_batch_pre_v2',
      withBatch: true,
      preV2Run: true,
    });
    expect(createBatch).not.toHaveBeenCalled();
    const ids = startedStepIds(created);
    expect(ids).toHaveLength(3);
    for (const id of ids) {
      expect(startedFor(created, id)).toHaveLength(1);
      expect(completedFor(created, id)).toHaveLength(1);
    }
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  it('run-not-versioned (409 backstop): flushes the deferred completion on the single path and nacks without failing the run', async () => {
    // Backstop for a run whose loaded snapshot carried a runVersion but the
    // server rejects the batch as unversioned. Unlike the transient 409 (which
    // ABANDONS the deferred completion), the run-not-versioned latch FLUSHES it
    // via the single path so nothing is dropped, then nacks for a fresh replay.
    const { created, batchCalls, returns } = await driveRun({
      runId: 'wrun_batch_not_versioned',
      withBatch: true,
      maxDeliveries: 1,
      batchImpl: async () => {
        throw new WorkflowWorldError('run not versioned', {
          status: 409,
          code: 'run-not-versioned',
        });
      },
    });
    expect(batchCalls).toHaveLength(1);
    const deferredStep = batchCalls[0].events[0].correlationId!;
    // Flushed (written), NOT abandoned — the distinguishing behavior from the
    // transient 409 test above (which asserts length 0 here).
    expect(completedFor(created, deferredStep)).toHaveLength(1);
    expect(created.some((d) => d.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });
});
