"""Kill the Temporal server itself in the middle of a run, and restart it from its file.

The server here is the Temporal CLI dev server started with --db-filename, so it keeps its
state (the event histories, timers and task queues) in a SQLite file instead of in memory.
The test kills the server and the worker hard, starts the server again on the same file,
starts a new worker, and checks that the run ends as if nothing had happened.
"""

import asyncio
import os
import shutil
import socket
import subprocess
import time
from dataclasses import replace
from pathlib import Path

from temporalio.client import Client

from resumable_eval import harness, store
from resumable_eval.shared import RunConfig
from resumable_eval.workflow import EvalWorkflow


def find_cli() -> str | None:
    """The Temporal CLI: $TEMPORAL_CLI, or `temporal` on the PATH."""
    return os.environ.get("TEMPORAL_CLI") or shutil.which("temporal")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class CliServer:
    def __init__(self, cli: str, state_file: Path) -> None:
        self.cli, self.state_file, self.port = cli, state_file, free_port()
        self.address = f"127.0.0.1:{self.port}"
        self.process: subprocess.Popen | None = None

    def start(self) -> None:
        command = [self.cli, "server", "start-dev", "--db-filename", str(self.state_file)]
        command += ["--ip", "127.0.0.1", "--port", str(self.port), "--headless", "--log-level", "error"]
        with self.state_file.with_suffix(".log").open("a") as log:
            self.process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

    def kill(self) -> None:
        self.process.kill()  # SIGKILL on Linux, TerminateProcess on Windows: no clean shutdown
        self.process.wait()

    async def connect(self) -> Client:
        deadline = time.monotonic() + 60
        while True:
            try:
                return await Client.connect(self.address)
            except RuntimeError:
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.2)


async def run_with_server_crash(server: CliServer, cfg: RunConfig, db: Path, crash_after: int) -> harness.Report:
    queue = f"queue-{cfg.run_id}"
    started = time.monotonic()
    client = await server.connect()
    worker = harness.start_worker(server.address, queue, db)
    await harness.start_run(client, cfg, queue)
    while store.count_results(db, cfg.run_id) < crash_after:
        await asyncio.sleep(0.02)
    server.kill()
    worker.kill()
    worker.wait()
    crashed_after = store.count_results(db, cfg.run_id)

    server.start()  # the same state file: the run is still there, with its history
    client = await server.connect()
    worker = harness.start_worker(server.address, queue, db)
    try:
        handle = client.get_workflow_handle_for(EvalWorkflow.run, cfg.run_id)
        summary = await handle.result()
    finally:
        worker.kill()
        worker.wait()
    return await harness.report(handle, db, cfg.run_id, summary, crashed_after, time.monotonic() - started)


async def server_crash_test(cli: str, cfg: RunConfig, folder: Path, crash_after: int) -> harness.CrashTest:
    """Run without a crash, then with the server killed after `crash_after` results, and compare."""
    folder.mkdir(parents=True, exist_ok=True)
    db = folder / "results.db"
    server = CliServer(cli, folder / f"temporal-{harness.new_run_id('state')}.db")
    server.start()
    try:
        client = await server.connect()
        uninterrupted = replace(cfg, run_id=harness.new_run_id("uninterrupted"))
        baseline = await harness.run_once(client, uninterrupted, db)
        crashed_cfg = replace(cfg, run_id=harness.new_run_id("crashed"))
        crashed = await run_with_server_crash(server, crashed_cfg, db, crash_after)
    finally:
        server.kill()
    return harness.compare(cfg, baseline, crashed)
