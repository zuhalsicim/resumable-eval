"""A SQLite file with two tables, written by the activities and read by the harness.

results: one row per run and question. The first write wins: writing the same key again changes nothing.
calls:   one row per provider call that returned an answer, keyed by run, question, provider and attempt,
         with the process id of the worker that made it.
         It is the ledger that shows whether a provider call finished more than once, and on which worker.
"""

import os
import sqlite3
from contextlib import closing
from pathlib import Path

from resumable_eval.shared import Result

SCHEMA = """
CREATE TABLE IF NOT EXISTS results (
    run_id TEXT, question_id INTEGER, provider TEXT, attempt INTEGER,
    used_fallback INTEGER, sql TEXT, correct INTEGER, cost REAL, error TEXT,
    PRIMARY KEY (run_id, question_id)
);
CREATE TABLE IF NOT EXISTS calls (
    run_id TEXT, question_id INTEGER, provider TEXT, attempt INTEGER, worker INTEGER,
    PRIMARY KEY (run_id, question_id, provider, attempt)
);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")  # the harness can read while a worker writes
    conn.executescript(SCHEMA)
    return conn


def save_result(path: str | Path, r: Result) -> None:
    with closing(connect(path)) as conn, conn:
        conn.execute(
            "INSERT INTO results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (r.run_id, r.question_id, r.provider, r.attempt, r.used_fallback, r.sql, r.correct, r.cost, r.error),
        )


def log_call(path: str | Path, run_id: str, question_id: int, provider: str, attempt: int) -> None:
    with closing(connect(path)) as conn, conn:
        conn.execute(
            "INSERT INTO calls VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (run_id, question_id, provider, attempt, os.getpid()),
        )


def results(path: str | Path, run_id: str) -> list[Result]:
    with closing(connect(path)) as conn:
        rows = conn.execute("SELECT * FROM results WHERE run_id = ? ORDER BY question_id", (run_id,)).fetchall()
    return [
        Result(run, qid, provider, attempt, bool(fallback), sql, bool(correct), cost, error)
        for run, qid, provider, attempt, fallback, sql, correct, cost, error in rows
    ]


def calls(path: str | Path, run_id: str) -> list[tuple[int, str, int]]:
    """(question_id, provider, attempt) of every provider call that returned an answer."""
    with closing(connect(path)) as conn:
        return conn.execute(
            "SELECT question_id, provider, attempt FROM calls WHERE run_id = ? ORDER BY question_id, attempt",
            (run_id,),
        ).fetchall()


def workers(path: str | Path, run_id: str) -> set[int]:
    """Process ids of the workers whose provider calls returned an answer."""
    with closing(connect(path)) as conn:
        return {pid for (pid,) in conn.execute("SELECT DISTINCT worker FROM calls WHERE run_id = ?", (run_id,))}


def count_results(path: str | Path, run_id: str) -> int:
    with closing(connect(path)) as conn:
        return conn.execute("SELECT COUNT(*) FROM results WHERE run_id = ?", (run_id,)).fetchone()[0]
