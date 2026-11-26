"""The activities: the steps of a run that touch the outside world.

ask_provider plays the model provider. It returns the answer recorded in the benchmark,
or fails on purpose, as planned by the seed. No model and no API is called.
save_result writes one result row. Running it twice for the same question changes nothing.
"""

import json
import os
import random
import time
from functools import cache
from pathlib import Path

from temporalio import activity
from temporalio.exceptions import ApplicationError

from resumable_eval import store
from resumable_eval.shared import Answer, Faults, Question, Result

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


@cache
def recorded_answers(provider: str) -> dict[int, dict]:
    rows = json.loads((DATA_DIR / f"{provider}.json").read_text(encoding="utf-8"))
    return {row["question_id"]: row for row in rows}


def planned_fault(seed: int, provider: str, question_id: int, attempt: int, faults: Faults) -> str | None:
    """The fault this attempt meets: "error", "hang" or None.

    The plan depends only on the seed, the provider and the question, so every run with
    the same seed meets the same faults. A question is either dead (every attempt fails)
    or flaky (the first one or two attempts fail) or fine.
    """
    rng = random.Random(f"{seed}:{provider}:{question_id}")
    draw = rng.random()
    if draw < faults.dead_rate:
        return "error"
    if draw < faults.dead_rate + faults.flaky_rate:
        failures = ["hang" if rng.random() < faults.hang_share else "error" for _ in range(rng.choice([1, 2]))]
        if attempt <= len(failures):
            return failures[attempt - 1]
    return None


class Activities:
    def __init__(self, db_path: str, exit_after_answer: int | None = None) -> None:
        self.db_path = db_path
        self.exit_after_answer = exit_after_answer  # used by a test to kill the worker at the worst moment

    @activity.defn
    def ask_provider(self, q: Question) -> Answer:
        attempt = activity.info().attempt
        fault = planned_fault(q.seed, q.provider, q.question_id, attempt, q.faults)
        if fault == "hang":
            # Sleep past the call timeout. Temporal gives up on this attempt and retries it.
            time.sleep(activity.info().start_to_close_timeout.total_seconds() + 0.5)
        if fault:
            raise ApplicationError(f"{q.provider} {fault} on question {q.question_id}, attempt {attempt}")

        row = recorded_answers(q.provider)[q.question_id]
        store.log_call(self.db_path, q.run_id, q.question_id, q.provider, attempt)
        if q.question_id == self.exit_after_answer:
            os._exit(1)  # the answer exists, but the worker dies before it can tell Temporal
        return Answer(
            question_id=q.question_id,
            provider=q.provider,
            attempt=attempt,
            sql=row["sql"],
            correct=row["correct"],
            prompt_tokens=row["prompt_tokens"],
            generated_tokens=row["generated_tokens"],
        )

    @activity.defn
    def save_result(self, result: Result) -> None:
        store.save_result(self.db_path, result)
