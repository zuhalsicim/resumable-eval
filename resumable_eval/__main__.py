"""Command line.

python -m resumable_eval run --questions 200 --budget 1.00 --crash-after 80
python -m resumable_eval crash-test --questions 200 --crash-after 80
python -m resumable_eval server-crash-test --questions 200 --crash-after 80   (needs the Temporal CLI)
python -m resumable_eval worker --address 127.0.0.1:7233 --queue q --db runs/results.db   (started by the others)
"""

import argparse
import asyncio
import sys
from pathlib import Path

from resumable_eval import harness, server_crash, worker
from resumable_eval.activities import recorded_answers
from resumable_eval.shared import PRIMARY, Faults, RunConfig


def config(args: argparse.Namespace, run_id: str) -> RunConfig:
    faults = Faults(dead_rate=args.dead_rate, flaky_rate=args.flaky_rate)
    return RunConfig(
        run_id=run_id,
        questions=args.questions,
        budget=args.budget,
        seed=args.seed,
        faults=faults,
        in_flight=args.in_flight,
        batch_size=args.batch_size,
    )


async def run(args: argparse.Namespace) -> int:
    cfg = config(args, harness.new_run_id("run"))
    async with await harness.local_server(ui=args.ui) as env:
        report = await harness.run_once(env.client, cfg, args.db, crash_after=args.crash_after, workers=args.workers)
        show(cfg, report, args.db)
        if args.ui:
            print(f"web UI: http://localhost:{harness.UI_PORT}/namespaces/default/workflows/{cfg.run_id}")
            await asyncio.to_thread(input, "press Enter to stop the server ")
    return 0


def show(cfg: RunConfig, report: harness.Report, db: Path) -> None:
    s = report.summary
    faults = harness.planned_faults(cfg, s.answered + s.unanswered)
    print(f"{s.run_id}: {s.status}")
    print(f"answered {s.answered} of {cfg.questions}, {s.correct} correct, {s.used_fallback} by the fallback")
    if s.unanswered:
        print(f"unanswered (the fallback gave up too): {s.unanswered}")
    print(f"spent ${s.spent:.4f} of ${cfg.budget:.2f}")
    print(
        f"faults planned for {cfg.primary.name}: dead questions {faults['dead']}, flaky questions {faults['flaky']}, "
        f"errors {faults['error']}, hangs {faults['hang']}"
    )
    if report.crashed_after is not None:
        print(f"worker killed after {report.crashed_after} results, another worker took over")
    print(f"provider answers in ledger: {len(report.calls)}, result rows: {len(report.results)} (in {db})")
    print(f"history events: {report.events}, seconds: {report.seconds:.1f}")


async def crash_test(args: argparse.Namespace) -> int:
    cfg = config(args, "")
    if args.command == "server-crash-test":
        cli = server_crash.find_cli()
        if cli is None:
            print("server-crash-test needs the Temporal CLI: set TEMPORAL_CLI or put temporal on the PATH")
            return 2
        test = await server_crash.server_crash_test(cli, cfg, args.db.parent / "server-crash", args.crash_after)
    else:
        async with await harness.local_server() as env:
            crash = {"crash_after": args.crash_after, "workers": args.workers}
            test = await harness.crash_test(env.client, cfg, args.db, **crash)
    print(test.line())
    return 0 if test.passed else 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="resumable_eval")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["run", "crash-test", "server-crash-test"]:
        p = commands.add_parser(name)
        p.add_argument("--questions", type=int, default=200)
        p.add_argument("--budget", type=float, default=1.00, help="dollars, at illustrative prices")
        p.add_argument("--seed", type=int, default=7)
        p.add_argument("--dead-rate", type=float, default=Faults.dead_rate)
        p.add_argument("--flaky-rate", type=float, default=Faults.flaky_rate)
        p.add_argument("--crash-after", type=int, default=None if name == "run" else 80)
        p.add_argument("--in-flight", type=int, default=1, help="questions asked at the same time")
        p.add_argument("--batch-size", type=int, default=0, help="K questions per long activity with heartbeats")
        p.add_argument("--workers", type=int, default=1, help="worker processes on the same task queue")
        p.add_argument("--db", type=Path, default=Path("runs/results.db"))
        if name == "run":
            p.add_argument("--ui", action="store_true", help="start the Temporal web UI and keep the server open")
    w = commands.add_parser("worker")
    w.add_argument("--address", required=True)
    w.add_argument("--queue", required=True)
    w.add_argument("--db", required=True)
    w.add_argument("--exit-after-answer", type=int, default=None)
    args = parser.parse_args()

    if args.command == "worker":
        asyncio.run(worker.run_worker(args.address, args.queue, args.db, args.exit_after_answer))
        return 0
    available = len(recorded_answers(PRIMARY.name))
    if not 0 < args.questions <= available:
        parser.error(f"--questions must be between 1 and {available}")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    return asyncio.run(run(args) if args.command == "run" else crash_test(args))


if __name__ == "__main__":
    sys.exit(main())
