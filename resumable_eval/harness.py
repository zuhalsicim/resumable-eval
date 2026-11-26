"""Run a benchmark on a local Temporal server with workers in their own processes, and crash them.

run_once starts a worker process, starts the workflow and waits for its result. On request it
kills the worker after some results are written, or lets the worker kill itself right after a
provider answered, and starts a fresh worker each time.
crash_test runs the same configuration twice, without and with a crash, and compares the two.
"""

import asyncio
import math
import subprocess
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

from resumable_eval import store
from resumable_eval.activities import planned_fault
from resumable_eval.shared import Result, RunConfig, Summary
from resumable_eval.workflow import EvalWorkflow


@dataclass
class Report:
    summary: Summary
    results: list[Result]
    calls: list[tuple[int, str, int]]  # the ledger: provider calls that returned an answer
    crashed_after: int | None  # results written when the worker died
    events: int  # length of the run's event history
    seconds: float


def new_run_id(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


def start_worker(address: str, queue: str, db: Path, exit_after_answer: int | None = None) -> subprocess.Popen:
    command = [sys.executable, "-m", "resumable_eval", "worker", "--address", address, "--queue", queue]
    command += ["--db", str(db)]
    if exit_after_answer is not None:
        command += ["--exit-after-answer", str(exit_after_answer)]
    with db.with_name(f"{queue}.log").open("a") as log:
        return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)


async def run_once(
    client: Client,
    cfg: RunConfig,
    db: Path,
    crash_after: int | None = None,
    exit_after_answer: int | None = None,
) -> Report:
    address = client.service_client.config.target_host
    queue = f"queue-{cfg.run_id}"
    started = time.monotonic()
    worker = start_worker(address, queue, db, exit_after_answer)
    handle = await client.start_workflow(
        EvalWorkflow.run, cfg, id=cfg.run_id, task_queue=queue, task_timeout=timedelta(seconds=2)
    )
    result = asyncio.ensure_future(handle.result())
    crashed_after = None
    try:
        if crash_after is not None or exit_after_answer is not None:
            # Wait until the worker has died by itself, or until it is time to kill it.
            while worker.poll() is None and not result.done():
                if crash_after is not None and store.count_results(db, cfg.run_id) >= crash_after:
                    worker.kill()  # SIGKILL on Linux, TerminateProcess on Windows: no clean shutdown
                    worker.wait()
                    break
                await asyncio.sleep(0.02)
            if not result.done():
                crashed_after = store.count_results(db, cfg.run_id)
                worker = start_worker(address, queue, db)
        summary = await result
    finally:
        worker.kill()
        worker.wait()
    seconds = time.monotonic() - started
    events = len((await handle.fetch_history()).events)
    return Report(summary, store.results(db, cfg.run_id), store.calls(db, cfg.run_id), crashed_after, events, seconds)


def planned_faults(cfg: RunConfig, questions: int) -> Counter:
    """What the seed plans for the primary on the first questions: dead and flaky questions, errors and hangs."""
    counts = Counter()
    for question_id in range(questions):
        attempts = range(1, cfg.max_attempts + 1)
        faults = [planned_fault(cfg.seed, cfg.primary.name, question_id, a, cfg.faults) for a in attempts]
        if all(faults):
            counts["dead"] += 1
        elif any(faults):
            counts["flaky"] += 1
        counts.update(f for f in faults if f)
    return counts


@dataclass
class CrashTest:
    questions: int
    crashed_after: int | None  # results written when the worker died
    status: str
    results: int  # result rows of the crashed run
    missing: int  # questions without a result row
    counted_twice: int  # answers the run counted beyond one per question
    spend_matches: bool  # the run's own spend equals the sum over its result rows
    calls: int  # provider calls that returned an answer, from the ledger
    repeated: int  # questions whose provider call returned an answer more than once
    identical: bool  # same result rows and summary as the uninterrupted run
    events: int
    seconds: float
    baseline_seconds: float

    @property
    def passed(self) -> bool:
        return (
            self.crashed_after is not None
            and self.status == "completed"
            and self.results == self.questions
            and self.missing == 0
            and self.counted_twice == 0
            and self.spend_matches
            and self.identical
        )

    def line(self) -> str:
        def yes(flag: bool) -> str:
            return "yes" if flag else "no"

        return (
            f"crash test | questions: {self.questions} | worker died after: {self.crashed_after} results "
            f"| status: {self.status} | results: {self.results} | missing: {self.missing} "
            f"| counted twice: {self.counted_twice} | spend matches results: {yes(self.spend_matches)} "
            f"| provider answers in ledger: {self.calls} | questions answered more than once: {self.repeated} "
            f"| identical to uninterrupted run: {yes(self.identical)} | history events: {self.events} "
            f"| seconds: {self.baseline_seconds:.0f} uninterrupted, {self.seconds:.0f} with crash"
        )


def comparable(report: Report) -> tuple:
    rows = [(r.question_id, r.provider, r.used_fallback, r.sql, r.correct, r.cost) for r in report.results]
    return rows, replace(report.summary, run_id="")


async def crash_test(
    client: Client, cfg: RunConfig, db: Path, crash_after: int | None = None, exit_after_answer: int | None = None
) -> CrashTest:
    baseline = await run_once(client, replace(cfg, run_id=new_run_id("uninterrupted")), db)
    crashed = await run_once(client, replace(cfg, run_id=new_run_id("crashed")), db, crash_after, exit_after_answer)
    summary = crashed.summary
    distinct = {r.question_id for r in crashed.results}
    asked = Counter(question_id for question_id, _, _ in crashed.calls)
    return CrashTest(
        questions=cfg.questions,
        crashed_after=crashed.crashed_after,
        status=summary.status,
        results=len(crashed.results),
        missing=len(set(range(cfg.questions)) - distinct),
        counted_twice=summary.answered - len(distinct),
        spend_matches=math.isclose(summary.spent, math.fsum(r.cost for r in crashed.results)),
        calls=len(crashed.calls),
        repeated=sum(1 for n in asked.values() if n > 1),
        identical=comparable(crashed) == comparable(baseline),
        events=crashed.events,
        seconds=crashed.seconds,
        baseline_seconds=baseline.seconds,
    )


UI_PORT = 8233


async def local_server(ui: bool = False) -> WorkflowEnvironment:
    """A Temporal dev server on this machine. The SDK downloads it the first time."""
    return await WorkflowEnvironment.start_local(dev_server_log_level="error", ui=ui, ui_port=UI_PORT if ui else None)
