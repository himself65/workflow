"""Python ports of the fixtures in `workbench/example/workflows/99_e2e.ts`.

The TypeScript e2e suite (`packages/core/e2e/e2e.test.ts`) is the single source
of truth for cross-language conformance: the driver stays in TypeScript, and the
app side gets reimplemented per language. This file is the Python app side.

Two conventions that look wrong and are not:

- **The module is named `99_e2e.py`**, matching the TS fixture file. That is not
  importable with an `import` statement, so `app.py` loads it with
  `importlib.import_module`. The name has to match because the harness looks
  fixtures up by file path (`workflows/99_e2e.ts`) and matches manifest keys by
  suffix.

- **Functions keep the TS fixtures' camelCase names** instead of being
  snake_cased. This keeps the manifest emitter mechanical — it can publish
  `qualname -> workflow_id` straight from the registry, with no TS-name to
  Python-name table to maintain and let rot. The camelCase names *are* the
  conformance contract.

Ported fixtures are listed in `../e2e-conformance.json`; a fixture missing from
that list is skipped by the suite. Only add a name there once its test passes.
"""

import asyncio
import dataclasses
import json
import random
import re
import time
from typing import Any, Awaitable, TypeVar

from vercel.workflow import (
    BaseHook,
    FatalError,
    Run,
    WorkflowWritable,
    Workflows,
    get_step_metadata,
    get_writable,
    sleep,
    start,
    time_ns,
)

# `as_vercel_job=False` because `app.py` wires the queue entrypoints itself: the
# default constructor creates them and discards the HTTP handlers, and calling
# the entrypoints again to recover a reference would register a second
# subscriber on the same topic and consumer group.
app = Workflows(as_vercel_job=False)


##########################################################
# nullByteWorkflow — 99_e2e.ts:291
#
# A NUL byte surviving the step return is a real cross-language signal: it has
# to round-trip the devalue codec and whatever the world writes to disk without
# being treated as a string terminator.


@app.step
async def nullByteStep() -> str:
    return "null byte \0"


@app.workflow
async def nullByteWorkflow() -> str:
    return await nullByteStep()


##########################################################
# addTenWorkflow — 99_e2e.ts:28
#
# The suite starts this one with a positional input array (`[123]`), and the
# step takes two positional parameters. Both directions of the argument
# encoding are in play: a TS-written `[123]` decoded into a Python call, and
# Python's own `add(a, 2)` written back out the way JS writes it.


@app.step
async def add(a: int, b: int) -> int:
    return a + b


@app.workflow
async def addTenWorkflow(input: int) -> int:
    a = await add(input, 2)
    b = await add(a, 3)
    return await add(b, 5)


##########################################################
# promiseAllWorkflow — 99_e2e.ts:44
#
# `asyncio.gather` is Python's `Promise.all`, and the interesting part is the
# same in both: three steps suspend in a single turn, so the orchestrator has to
# create three pending events before the replay yields.


@app.step
async def randomDelay(v: str) -> str:
    await asyncio.sleep(random.random() * 3)
    return v.upper()


@app.workflow
async def promiseAllWorkflow() -> str:
    a, b, c = await asyncio.gather(
        randomDelay("a"),
        randomDelay("b"),
        randomDelay("c"),
    )
    return a + b + c


##########################################################
# sleepingWorkflow — 99_e2e.ts:201
# parallelSleepWorkflow — 99_e2e.ts:209
# sleepInLoopWorkflow — 99_e2e.ts:3045
#
# `sleep` is the one workflow primitive with no step behind it: the orchestrator
# suspends on a `wait_created` event and the world redelivers the run when the
# timer fires. Porting it checks that Python emits a wait the TypeScript driver
# recognises — `cancelRun` waits for exactly that event type before cancelling.
#
# The clock needs care. The tests do arithmetic on the returned timestamps and
# compare against millisecond thresholds, so these return `time_ns() // 1e6`
# rather than `now()`: `vercel.workflow.now()` hands back a `datetime`, which is
# the right Python type and the wrong wire type for `endTime - startTime`.


@app.workflow
async def sleepingWorkflow(durationMs: int = 10_000) -> dict:
    startTime = time_ns() // 1_000_000
    await sleep(durationMs)
    endTime = time_ns() // 1_000_000
    return {"startTime": startTime, "endTime": endTime}


@app.workflow
async def parallelSleepWorkflow() -> dict:
    startTime = time_ns() // 1_000_000
    await asyncio.gather(*(sleep("1s") for _ in range(10)))
    endTime = time_ns() // 1_000_000
    return {"startTime": startTime, "endTime": endTime}


@app.step
async def noopStep(iteration: int) -> dict:
    # Wall clock, not the deterministic workflow clock: this runs in the step
    # context, and the test reads these timestamps to prove the sleeps between
    # iterations were really honoured rather than replayed away.
    return {"iteration": iteration, "ts": time.time_ns() // 1_000_000}


@app.workflow
async def sleepInLoopWorkflow() -> dict:
    iterations = 3
    sleepMs = 3_000
    timestamps = []

    for i in range(iterations):
        result = await noopStep(i)
        timestamps.append(result["ts"])
        if i < iterations - 1:
            await sleep(sleepMs)

    return {"timestamps": timestamps, "totalElapsed": timestamps[-1] - timestamps[0]}


##########################################################
# Racing suspensions
#
# `Promise.race` and `Promise.any` have no asyncio spelling that takes bare
# awaitables, so the five race fixtures below share these two helpers. Both
# resolve ties by the order the awaitables were passed rather than by set
# iteration order — `asyncio.wait` returns a `set`, and a workflow body has to
# be deterministic across replays, so picking `next(iter(done))` would be a
# latent replay divergence the moment two steps land in the same turn.
#
# Nothing cancels the losers, matching JS: a race that resolves leaves the
# other steps running, the body returns, and `_run_in_loop` cancels the
# orphaned tasks on its way out. Their step invocations are already in flight
# and complete against a run that has finished, exactly as they do on the
# TypeScript side.

_T = TypeVar("_T")


async def _race(*awaitables: Awaitable[_T]) -> _T:
    """`Promise.race`: settle with the first to settle, error included."""
    tasks = [asyncio.ensure_future(a) for a in awaitables]
    done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in tasks:
        if task in done:
            return task.result()
    raise AssertionError("asyncio.wait returned no completed task")


async def _any(*awaitables: Awaitable[_T]) -> _T:
    """`Promise.any`: the first to *succeed*; failures are skipped."""
    tasks = [asyncio.ensure_future(a) for a in awaitables]
    pending = set(tasks)
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in tasks:
            if task in done and task.exception() is None:
                return task.result()
    raise RuntimeError("all awaitables rejected")


##########################################################
# promiseRaceWorkflow — 99_e2e.ts:62
# promiseAnyWorkflow — 99_e2e.ts:79
#
# The counterpart to `promiseAllWorkflow`: three steps suspend in one turn, but
# the body resumes on the first one instead of all three. That is the case
# where replay and completion overlap — the run finishes while two step
# invocations are still outstanding.
#
# `promiseAnyWorkflow` additionally needs a step failure to be *skippable*
# rather than fatal. Python surfaces a failed step to the body as a
# `RuntimeError` carrying the step's error text (see the note on
# `errorRetryDisabled` below), which is all `_any` needs — it only asks whether
# the task raised.


@app.step
async def specificDelay(delay: int, v: str) -> str:
    await asyncio.sleep(delay / 1000)
    return v.upper()


@app.workflow
async def promiseRaceWorkflow() -> str:
    return await _race(
        specificDelay(10_000, "a"),
        specificDelay(100, "b"),  # "b" should always win
        specificDelay(20_000, "c"),
    )


@app.step
async def stepThatFails() -> str:
    raise FatalError("step failed")


@app.workflow
async def promiseAnyWorkflow() -> str:
    return await _any(
        stepThatFails(),
        specificDelay(100, "b"),  # "b" should always win
        specificDelay(6_000, "c"),
    )


##########################################################
# sleepWinsRaceWorkflow — 99_e2e.ts:225
# stepWinsRaceWorkflow — 99_e2e.ts:236
#
# A race between the two suspension kinds. `sleep` resumes from a timer the
# world owns and a step resumes from an event the step handler writes, so these
# assert that the orchestrator resolves whichever lands first without waiting
# for the other — the driver bounds `durationMs` at 5s against a 10s loser.
#
# Both are listed under `unsupported` in `../e2e-conformance.json` even though
# they pick the right winner every time, and the 5s bound is why: these are the
# only two tests in the suite that bound a run's *elapsed* time from above, so
# they are the only two that notice the ~5s `world-local` waits before
# redelivering a first delivery the app 500'd on a run row it could not read
# yet. That is the same upstream gap as the `resilient start` entry, and all
# three exemptions come out together when it is fixed.


@app.step
async def delayMsStep(ms: int, label: str) -> str:
    await asyncio.sleep(ms / 1000)
    return label


async def _sleepThen(duration: str, label: str) -> str:
    """`sleep(...).then(() => label)` — a plain coroutine, not a step."""
    await sleep(duration)
    return label


@app.workflow
async def sleepWinsRaceWorkflow() -> dict:
    startTime = time_ns() // 1_000_000
    winner = await _race(delayMsStep(10_000, "step"), _sleepThen("1s", "sleep"))
    endTime = time_ns() // 1_000_000
    return {"winner": winner, "durationMs": endTime - startTime}


@app.workflow
async def stepWinsRaceWorkflow() -> dict:
    startTime = time_ns() // 1_000_000
    winner = await _race(delayMsStep(1_000, "step"), _sleepThen("10s", "sleep"))
    endTime = time_ns() // 1_000_000
    return {"winner": winner, "durationMs": endTime - startTime}


##########################################################
# promiseRaceStressTestWorkflow — 99_e2e.ts:478
#
# Five steps 5s apart, raced and retired one at a time, so the body re-enters
# `_race` over a shrinking set across five separate replays. Each replay has to
# rebuild the same suspensions in the same order and match them to the events
# already in the log; a single misaligned slot shows up as a missing or
# duplicated entry in the returned list.


@app.step
async def promiseRaceStressTestDelayStep(dur: int, resp: int) -> int:
    await asyncio.sleep(dur / 1000)
    return resp


@app.workflow
async def promiseRaceStressTestWorkflow() -> list:
    # Tasks rather than coroutines: unlike a JS promise, a coroutine can only
    # be awaited once, and every loop iteration races the survivors again.
    promises = {
        i: asyncio.ensure_future(promiseRaceStressTestDelayStep(1000 * 5 * i, i))
        for i in range(5)
    }
    done = []

    while promises:
        res = await _race(*promises.values())
        done.append(res)
        del promises[res]

    return done


##########################################################
# errorRetrySuccess — 99_e2e.ts:1279
# errorRetryDisabled — 99_e2e.ts:1329
#
# The two halves of the step retry policy: the default (`DEFAULT_MAX_RETRIES`,
# 3 in both SDKs) and `max_retries=0`. `get_step_metadata().attempt` is what
# makes them observable from inside the step, and it is the same 1-based
# counter the driver reads back off the step entity through the CLI.
#
# These two work because neither inspects the exception. `errorRetryDisabled`
# reads its attempt number back out of the message *text*, which is the one part
# of a thrown error that survives the event log — see the note on
# `errorFatalCatchable` below for what does not.
#
# `errorRetryCustomDelay`, the third fixture in the same TypeScript block, is
# not here at all: it needs `RetryableError(retryAfter=…)`, which vercel-py does
# not export, and `StepInfo` carries no `step_started_at` for the test's
# duration assertion either.


@app.step
async def retryUntilAttempt3() -> int:
    attempt = get_step_metadata().attempt
    if attempt < 3:
        raise RuntimeError(f"Failed on attempt {attempt}")
    return attempt


@app.workflow
async def errorRetrySuccess() -> dict:
    return {"finalAttempt": await retryUntilAttempt3()}


@app.step(max_retries=0)
async def throwWithNoRetries() -> None:
    raise RuntimeError(f"Failed on attempt {get_step_metadata().attempt}")


@app.workflow
async def errorRetryDisabled() -> dict:
    try:
        await throwWithNoRetries()
        return {"failed": False, "attempt": None}
    except Exception as e:
        match = re.search(r"attempt (\d+)", str(e))
        return {"failed": True, "attempt": int(match.group(1)) if match else None}


##########################################################
# outputStreamWorkflow — 99_e2e.ts:355
# outputStreamInsideStepWorkflow — 99_e2e.ts:403
# utf8StreamWorkflow — 99_e2e.ts:441
#
# Run-scoped streams, which are the one part of the protocol that does not
# travel through the event log: chunks are appended to a separate per-run log
# that a reader tails live. The driver reads them with `run.getReadable()`, so
# these fixtures assert that Python's framing and payload encoding are the ones
# `@workflow/core`'s `getDeserializeStream` expects.
#
# Both spellings of the same API are covered, because they take different paths
# through the SDK. `get_writable()` in the *workflow body* returns a
# `WorkflowStreamHandle` — the body replays and has no network, so it cannot
# write — and passing that handle into a step's arguments is what turns it into
# a writer, via the serialization layer. `get_writable()` in a *step* returns
# the writer directly. The two have to name the same stream for the workflow to
# be able to hand one to a step at all.
#
# A `bytes` chunk arrives on the TypeScript side as a `Uint8Array` and anything
# else as its devalue value, which is why the driver reads chunk 0 as binary
# and chunk 1 as an object.
#
# Nothing closes a stream implicitly in either SDK, and the driver asserts the
# reader sees `done` — hence the explicit `stepCloseOutputStream` at the end of
# each fixture.


@app.step
async def stepWithOutputStreamBinary(writable: WorkflowWritable, text: str) -> None:
    await writable.write(text.encode())


@app.step
async def stepWithOutputStreamObject(writable: WorkflowWritable, obj: Any) -> None:
    await writable.write(obj)


@app.step
async def stepCloseOutputStream(writable: WorkflowWritable) -> None:
    await writable.close()


@app.workflow
async def outputStreamWorkflow() -> str:
    writable = get_writable()
    namedWritable = get_writable(namespace="test")
    await sleep("1s")
    await stepWithOutputStreamBinary(writable, "Hello, world!")
    await sleep("1s")
    await stepWithOutputStreamBinary(namedWritable, "Hello, named stream!")
    await sleep("1s")
    await stepWithOutputStreamObject(writable, {"foo": "test"})
    await sleep("1s")
    await stepWithOutputStreamObject(namedWritable, {"foo": "bar"})
    await sleep("1s")
    await stepCloseOutputStream(writable)
    await stepCloseOutputStream(namedWritable)
    return "done"


@app.step
async def stepWithOutputStreamInsideStep(text: str) -> None:
    await get_writable().write(text.encode())


@app.step
async def stepWithNamedOutputStreamInsideStep(namespace: str, obj: Any) -> None:
    await get_writable(namespace=namespace).write(obj)


@app.step
async def stepCloseOutputStreamInsideStep(namespace: str | None = None) -> None:
    await get_writable(namespace=namespace).close()


@app.workflow
async def outputStreamInsideStepWorkflow() -> str:
    await sleep("1s")
    await stepWithOutputStreamInsideStep("Hello from step!")
    await sleep("1s")
    await stepWithNamedOutputStreamInsideStep(
        "step-ns", {"message": "Hello from named stream in step!"}
    )
    await sleep("1s")
    await stepWithOutputStreamInsideStep("Second message")
    await sleep("1s")
    await stepWithNamedOutputStreamInsideStep("step-ns", {"counter": 42})
    await sleep("1s")
    await stepCloseOutputStreamInsideStep()
    await stepCloseOutputStreamInsideStep("step-ns")
    return "done"


@app.step
async def stepWriteUtf8Text(writable: WorkflowWritable, text: str) -> None:
    await writable.write(text.encode())


@app.step
async def stepWriteUtf8Json(writable: WorkflowWritable, value: Any) -> None:
    await writable.write(json.dumps(value, ensure_ascii=False).encode())


@app.workflow
async def utf8StreamWorkflow() -> str:
    writable = get_writable()
    await sleep("1s")
    await stepWriteUtf8Text(writable, "Hello, world!")
    await stepWriteUtf8Text(writable, "Café — naïve résumé")
    await stepWriteUtf8Text(writable, "你好，世界！🌍✨")
    await stepWriteUtf8Text(writable, "مرحبا بالعالم")
    await stepWriteUtf8Json(writable, {"greeting": "안녕하세요", "emoji": "🎉"})
    await stepCloseOutputStream(writable)
    return "done"


##########################################################
# errorRetryFatal — 99_e2e.ts:1293
# errorFatalCatchable — 99_e2e.ts:1347
#
# `FatalError` is the one retry-control error vercel-py exports, and the step
# handler honours it: `fatal or attempt >= max_retries + 1` is what decides
# whether to write `step_failed` instead of `step_retrying`, so a step that
# raises it burns exactly one attempt.
#
# Both tests are under `unsupported`, and for the half of each that Python
# cannot do rather than the half it can. What works is the lifecycle above — the
# one attempt, and the run failing on it. What does not is the error's
# *identity*: `step_failed.error` is written as text and comes back to the
# workflow body as `RuntimeError(<text>)`, so the `except` below sees no
# `FatalError`, and the resulting `run_failed.code` is `type(e).__name__` on
# that `RuntimeError` where the driver expects `USER_ERROR`. Upstream needs the
# serialized-error pipeline (`SerializedData` in place of the pre-#1851
# `StructuredError` shape); the two `RoundTrip` fixtures in the same TypeScript
# block wait on the same change and are not ported at all, since they assert on
# the cause chain and there would be nothing to assert.


@app.step
async def throwFatalError() -> None:
    raise FatalError("Fatal step error")


@app.workflow
async def errorRetryFatal() -> str:
    await throwFatalError()
    return "never reached"


@app.workflow
async def errorFatalCatchable() -> dict:
    try:
        await throwFatalError()
        return {"caught": False, "isFatal": False}
    except Exception as e:
        return {"caught": True, "isFatal": isinstance(e, FatalError)}


##########################################################
# metadataFromHelperWorkflow — 99_e2e.ts:3148
#
# Upstream's #1577 regression test: the metadata accessors have to work from a
# helper defined at module level rather than inline in the step body. The
# mechanism differs — `AsyncLocalStorage` there, a `contextvars.ContextVar`
# here — but the failure mode it guards against is the same one, a context
# that only propagates as far as the decorated function.
#
# Only the step half is checked. `getStepMetadata()`'s counterpart
# `getWorkflowMetadata()` has no Python equivalent, so `workflowRunId` comes
# off `StepInfo.run_id`, which is the same run id the TypeScript fixture reads
# out of the workflow metadata. That also makes `workflowAndStepMetadataWorkflow`
# — which asserts the two metadata objects against each other — unportable for
# now, so it is not in this file.


async def _withStrictMetadataCheck(fn):
    stepMetadata = get_step_metadata()
    return await fn(), stepMetadata


@app.step
async def metadataHelperStep(label: str) -> dict:
    async def _produce() -> str:
        return label

    _result, stepMetadata = await _withStrictMetadataCheck(_produce)

    return {
        "label": label,
        "workflowRunId": stepMetadata.run_id,
        "stepId": stepMetadata.step_id,
        "attempt": stepMetadata.attempt,
    }


@app.workflow
async def metadataFromHelperWorkflow(label: str) -> dict:
    return await metadataHelperStep(label)


##########################################################
# spawnWorkflowFromStepWorkflow — 99_e2e.ts:1112
#
# A run that starts another run. `start()` is a world write, so it can only
# happen in a step — the workflow body replays and its sandbox has no network,
# which is the same restriction the TypeScript fixture states in a comment.
# Waiting for the child is a step for the same reason.
#
# `Run(run_id).return_value()` polls the child's status; the TypeScript
# `getRun(runId).returnValue` is the same shape. Both hold the parent's step
# open for as long as the child takes, which is the caveat the TS fixture's
# `fibonacciWorkflow` neighbour documents at length — worth remembering before
# porting that one, since its recursion needs the worker pool to be deep enough
# for every waiting parent.


@app.step
async def doubleValue(value: int) -> int:
    return value * 2


@app.workflow
async def childWorkflow(value: int) -> dict:
    return {"childResult": await doubleValue(value), "originalValue": value}


@app.step
async def spawnChildWorkflow(value: int) -> str:
    childRun = await start(childWorkflow, value)
    return childRun.run_id


@app.step
async def awaitWorkflowResult(runId: str) -> Any:
    return await Run(runId).return_value()


@app.workflow
async def spawnWorkflowFromStepWorkflow(inputValue: int) -> dict:
    childRunId = await spawnChildWorkflow(inputValue)
    childResult = await awaitWorkflowResult(childRunId)
    return {
        "parentInput": inputValue,
        "childRunId": childRunId,
        "childResult": childResult,
    }


##########################################################
# hookWithSleepWorkflow — 99_e2e.ts:2960
# hookWithSleepFinalStepWorkflow — 99_e2e.ts:2995
# hookTokenReuseLoopWorkflow — 99_e2e.ts:990
# sleepWithSequentialStepsWorkflow — 99_e2e.ts:3071
#
# The four fixtures in the TypeScript file's hook/sleep-interaction cluster,
# and the first hooks on this side. The bucket they used to sit in was labelled
# "vercel-py's `BaseHook.wait()` has a different shape than the async-iterable
# the fixtures use", which turns out to be wrong: `HookEvent` implements both
# `__await__` (one payload) and `__aiter__` / `__anext__` (a stream of them), so
# `for await (const p of hook)` ports to `async for payload in hook` directly.
#
# What is really different is the payload type. `Hook.set_result` requires the
# class handed to `wait()` to be a dataclass or a pydantic model, and calls
# `hook_cls(**raw)` on the plain JSON the resumer sent — so the port is a
# dataclass with a default per optional field, and the fixture's structural
# type becomes a declared one. The declaration has to stay loose in the same
# places the TypeScript type is optional: the driver resumes with `{type, id}`
# on one payload and `{type, done}` on another, and a required field would
# raise on whichever call omitted it.
#
# Three translation traps, each of which cost a debugging round here:
#
# - **`using hook` is not `try/finally`.** A Python workflow body unwinds
#   through a `_SuspendException` on *every* suspension, so a `finally` around
#   an `await` runs once per turn rather than once at scope exit. Disposing a
#   hook there deletes the suspension before the orchestrator can flush its
#   `hook_created`, and the run stalls with no hook for the driver to resume.
#   Dispose on the normal path only.
# - **`void sleep('1d')`** is `asyncio.ensure_future(sleep("1d"))`. The wait is
#   created and never completes; the body returns first and the orphaned task
#   is cancelled with the loop.
# - **A step takes the payload as a dict**, not as the dataclass: keeping the
#   step signature `dict` avoids registering a serializer for a type that only
#   exists to satisfy `set_result`.
#
# `sleepWithSequentialStepsWorkflow` is the cluster's control and has no hook in
# it at all — a fire-and-forget sleep plus three sequential steps. It is ported
# here rather than with the other sleep fixtures because its whole purpose is to
# be read next to the two above: it passes, which is what makes their failures
# specific to hooks rather than to a pending wait.
#
# Two of the four are under `unsupported`, and both look like real orchestrator
# defects rather than missing API — see `../e2e-conformance.json` for what was
# observed.


@dataclasses.dataclass
class SleepHookPayload(BaseHook):
    type: str
    id: int | None = None
    done: bool | None = None


@app.step
async def processPayload(payload: dict) -> dict:
    return {"processed": True, "type": payload["type"], "id": payload.get("id")}


@app.workflow
async def hookWithSleepWorkflow(token: str) -> list:
    hook = SleepHookPayload.wait(token=token)

    # Concurrent sleep that won't complete during the test
    asyncio.ensure_future(sleep("1d"))

    results = []
    async for payload in hook:
        results.append(await processPayload(dataclasses.asdict(payload)))
        if payload.done:
            break

    hook.dispose()
    return results


@app.workflow
async def hookWithSleepFinalStepWorkflow(token: str) -> dict:
    hook = SleepHookPayload.wait(token=token)
    asyncio.ensure_future(sleep("1d"))

    seen = []
    finalResult = None
    async for payload in hook:
        if payload.id is not None:
            seen.append(payload.id)
        if payload.done:
            finalResult = await processPayload(dataclasses.asdict(payload))
            break

    hook.dispose()
    return {"seen": seen, "finalResult": finalResult}


@dataclasses.dataclass
class ReuseHookPayload(BaseHook):
    message: str


@app.workflow
async def hookTokenReuseLoopWorkflow(token: str, rounds: int) -> dict:
    received = []
    for round in range(rounds):
        hook = ReuseHookPayload.wait(token=token)

        # `hook.getConflict()` has no Python equivalent. A conflict is still
        # observable, just later and as an exception: `HookConflictEvent`
        # resolves the hook's future with a `RuntimeError`, so the same two
        # outcomes come out of one `await` instead of two.
        try:
            payload = await hook
        except RuntimeError as e:
            if "already in use" not in str(e):
                raise
            return {"received": received, "conflictRound": round}

        received.append(payload.message)
        hook.dispose()

    return {"received": received, "conflictRound": None}


@app.step
async def addNumbers(a: int, b: int) -> int:
    return a + b


@app.workflow
async def sleepWithSequentialStepsWorkflow() -> dict:
    shouldCancel = False

    async def _cancelAfterSleep() -> None:
        nonlocal shouldCancel
        await sleep("1d")
        shouldCancel = True

    asyncio.ensure_future(_cancelAfterSleep())

    a = await addNumbers(1, 2)
    b = await addNumbers(a, 3)
    c = await addNumbers(b, 4)
    return {"a": a, "b": b, "c": c, "shouldCancel": shouldCancel}
