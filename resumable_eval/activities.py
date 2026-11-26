"""The activities: the steps of a run that touch the outside world.

ask_provider plays the model provider. It returns the answer recorded in the benchmark,
or fails on purpose, as planned by the seed. No model and no API is called.
ask_batch asks one provider several questions in one long call and reports its progress by heartbeat.
save_result writes one result row. Running it twice for the same question changes nothing.
"""

import json
import os
import time
from datetime import timedelta
from functools import cache
from pathlib import Path

from temporalio import activity
from temporalio.exceptions import ApplicationError

from resumable_eval import store
from resumable_eval.faults import planned_fault, planned_latency
from resumable_eval.shared import Answer, Batch, Question, Result

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


@cache
def recorded_answers(provider: str) -> dict[int, dict]:
    rows = json.loads((DATA_DIR / f"{provider}.json").read_text(encoding="utf-8"))
    return {row["question_id"]: row for row in rows}


class Activities:
    def __init__(self, db_path: str, exit_after_answer: int | None = None) -> None:
        self.db_path = db_path
        self.exit_after_answer = exit_after_answer  # used by tests to kill the worker at the worst moment

    @activity.defn
    def ask_provider(self, q: Question) -> Answer:
        attempt = activity.info().attempt
        time.sleep(planned_latency(q.seed, q.provider, q.question_id, attempt, q.faults))
        fault = planned_fault(q.seed, q.provider, q.question_id, attempt, q.faults)
        if fault == "hang":
            # Sleep past the call timeout. Temporal gives up on this attempt and retries it.
            time.sleep(activity.info().start_to_close_timeout.total_seconds() + 0.5)
        if fault == "rate limit":
            # Like an HTTP 429 with a Retry-After header. Temporal waits that long before the next attempt.
            raise ApplicationError(
                f"{q.provider} rate limit on question {q.question_id}, retry after {q.faults.retry_after_seconds} s",
                type="RateLimited",
                next_retry_delay=timedelta(seconds=q.faults.retry_after_seconds),
            )
        if fault:
            raise ApplicationError(
                f"{q.provider} {fault} on question {q.question_id}, attempt {attempt}", type="ProviderError"
            )
        return self.answer(q.run_id, q.question_id, q.provider, attempt)

    @activity.defn
    def ask_batch(self, batch: Batch) -> list[Answer | None]:
        """Ask one provider several questions in one long call. None for a question whose call failed.

        After each question it sends a heartbeat with the answers so far. If the worker dies, the
        server notices when the heartbeats stop (the heartbeat timeout), and the next attempt starts
        after the last answer that reached the server, not at the start of the batch.
        """
        info = activity.info()
        answers = [Answer(**a) if a else None for a in info.heartbeat_details[0]] if info.heartbeat_details else []
        for question_id in batch.question_ids[len(answers) :]:
            time.sleep(batch.seconds_per_question)  # a slow call
            if planned_fault(batch.seed, batch.provider, question_id, 1, batch.faults):
                # One try per question in a batch: a call that fails or would hang is left to the workflow,
                # which asks that question again on its own, with retries and the fallback.
                answers.append(None)
            else:
                answers.append(self.answer(batch.run_id, question_id, batch.provider, info.attempt))
            activity.heartbeat(answers)
        return answers

    def answer(self, run_id: str, question_id: int, provider: str, attempt: int) -> Answer:
        """The recorded answer, after writing the call to the ledger."""
        row = recorded_answers(provider)[question_id]
        store.log_call(self.db_path, run_id, question_id, provider, attempt)
        if question_id == self.exit_after_answer:
            os._exit(1)  # the answer exists, but the worker dies before it can tell Temporal
        return Answer(
            question_id=question_id,
            provider=provider,
            attempt=attempt,
            sql=row["sql"],
            correct=row["correct"],
            prompt_tokens=row["prompt_tokens"],
            generated_tokens=row["generated_tokens"],
        )

    @activity.defn
    def save_result(self, result: Result) -> None:
        store.save_result(self.db_path, result)
