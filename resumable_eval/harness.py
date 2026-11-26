"""Run a benchmark on a local Temporal server with workers in their own processes, and crash them.

run_once starts one or more worker processes, starts the workflow and waits for its result. On
request it kills a worker after some results are written, or lets workers kill themselves right
after a provider answered, and starts a fresh worker whenever none is left.
crash_test runs the same configuration twice, without and with a crash, and compares the two.
"""

import asyncio
import math
import subprocess
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client, WorkflowHistory
from temporalio.testing import WorkflowEnvironment

from resumable_eval import store
from resumable_eval.faults import planned_fault
from resumable_eval.shared import Result, RunConfig, Summary
from resumable_eval.workflow import EvalWorkflow


@dataclass
class Report:
    summary: Summary
    results: list[Result]
    calls: list[tuple[int, str, int]]  # the ledger: provider calls that returned an answer
    workers: set[int]  # process ids of the workers that made those calls
    crashed_after: int | None  # results written when the first worker died
    history: WorkflowHistory  # the run's event history
    seconds: float

    @property
    def events(self) -> int:
        return len(self.history.events)


def new_run_id(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


def start_worker(address: str, queue: str, db: Path, exit_after_answer: int | None = None) -> subprocess.Popen:
    command = [sys.executable, "-m", "resumable_eval", "worker", "--address", address, "--queue", queue]
    command += ["--db", str(db)]
    if exit_after_answer is not None:
        command += ["--exit-after-answer", str(exit_after_answer)]
    with db.with_name(f"{queue}.log").open("a") as log:
        return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)


async def start_run(client: Client, cfg: RunConfig, queue: str):
    return await client.start_workflow(
        EvalWorkflow.run, cfg, id=cfg.run_id, task_queue=queue, task_timeout=timedelta(seconds=2)
    )


async def run_once(
    client: Client,
    cfg: RunConfig,
    db: Path,
    crash_after: int | None = None,
    exit_after_answer: int | None = None,
    exits: int = 1,
    workers: int = 1,
) -> Report:
    """Run the benchmark with `workers` worker processes on one task queue.

    crash_after: kill the first worker once this many results are written.
    exit_after_answer: workers die by themselves right after a provider answered this question,
        `exits` times in a row: each new worker dies again, until the last one, which does not.
    When no worker is left, a new one is started.
    """
    address = client.service_client.config.target_host
    queue = f"queue-{cfg.run_id}"
    started = time.monotonic()
    hook = exit_after_answer
    pool = [start_worker(address, queue, db, hook) for _ in range(workers)]
    handle = await start_run(client, cfg, queue)
    result = asyncio.ensure_future(handle.result())
    planned_deaths = (crash_after is not None) + (exits if hook is not None else 0)
    deaths, crashed_after = 0, None
    try:
        while deaths < planned_deaths and not result.done():
            if crash_after is not None and deaths == 0 and store.count_results(db, cfg.run_id) >= crash_after:
                pool[0].kill()  # SIGKILL on Linux, TerminateProcess on Windows: no clean shutdown
                pool[0].wait()
            for dead in [w for w in pool if w.poll() is not None]:
                pool.remove(dead)
                deaths += 1
                if crashed_after is None:
                    crashed_after = store.count_results(db, cfg.run_id)
            if not pool:
                pool.append(start_worker(address, queue, db, hook if deaths < exits else None))
            await asyncio.sleep(0.02)
        summary = await result
    finally:
        for w in pool:
            w.kill()
            w.wait()
    return await report(handle, db, cfg.run_id, summary, crashed_after, time.monotonic() - started)


async def report(handle, db: Path, run_id: str, summary: Summary, crashed_after: int | None, seconds: float) -> Report:
    history = await handle.fetch_history()
    results, ledger, workers = store.results(db, run_id), store.calls(db, run_id), store.workers(db, run_id)
    return Report(summary, results, ledger, workers, crashed_after, history, seconds)


def planned_faults(cfg: RunConfig, questions: int) -> Counter:
    """What the seed plans for the primary on the first questions: dead and flaky questions, and each fault."""
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
    crashed_after: int | None  # results written when the worker (or the server) died
    status: str
    results: int  # result rows of the crashed run
    missing: int  # questions without a result row
    counted_twice: int  # questions the run counted beyond one per question
    spend_matches: bool  # the run's own spend equals the sum over its result rows
    calls: int  # provider calls that returned an answer, from the ledger
    repeated: int  # questions whose provider call returned an answer more than once
    identical: bool  # same result rows and summary as the uninterrupted run
    events: int
    seconds: float
    baseline_seconds: float
    report: Report = field(repr=False)  # everything about the crashed run, for tests that look closer

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
            f"crash test | questions: {self.questions} | died after: {self.crashed_after} results "
            f"| status: {self.status} | results: {self.results} | missing: {self.missing} "
            f"| counted twice: {self.counted_twice} | spend matches results: {yes(self.spend_matches)} "
            f"| provider answers in ledger: {self.calls} | questions answered more than once: {self.repeated} "
            f"| identical to uninterrupted run: {yes(self.identical)} | history events: {self.events} "
            f"| seconds: {self.baseline_seconds:.0f} uninterrupted, {self.seconds:.0f} with crash"
        )


def comparable(report: Report) -> tuple:
    rows = [(r.question_id, r.provider, r.used_fallback, r.sql, r.correct, r.cost, r.error) for r in report.results]
    return rows, replace(report.summary, run_id="")


def compare(cfg: RunConfig, baseline: Report, crashed: Report) -> CrashTest:
    summary = crashed.summary
    distinct = {r.question_id for r in crashed.results}
    asked = Counter(question_id for question_id, _, _ in crashed.calls)
    return CrashTest(
        questions=cfg.questions,
        crashed_after=crashed.crashed_after,
        status=summary.status,
        results=len(crashed.results),
        missing=len(set(range(cfg.questions)) - distinct),
        counted_twice=summary.answered + summary.unanswered - len(distinct),
        spend_matches=math.isclose(summary.spent, math.fsum(r.cost for r in crashed.results)),
        calls=len(crashed.calls),
        repeated=sum(1 for n in asked.values() if n > 1),
        identical=comparable(crashed) == comparable(baseline),
        events=crashed.events,
        seconds=crashed.seconds,
        baseline_seconds=baseline.seconds,
        report=crashed,
    )


async def crash_test(client: Client, cfg: RunConfig, db: Path, baseline: RunConfig | None = None, **crash) -> CrashTest:
    """Run `baseline` (default: the same settings) without a crash and `cfg` with one, and compare them."""
    uninterrupted = await run_once(client, replace(baseline or cfg, run_id=new_run_id("uninterrupted")), db)
    crashed = await run_once(client, replace(cfg, run_id=new_run_id("crashed")), db, **crash)
    return compare(cfg, uninterrupted, crashed)


UI_PORT = 8233


async def local_server(ui: bool = False) -> WorkflowEnvironment:
    """A Temporal dev server on this machine. The SDK downloads it the first time."""
    return await WorkflowEnvironment.start_local(dev_server_log_level="error", ui=ui, ui_port=UI_PORT if ui else None)
