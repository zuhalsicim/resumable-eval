"""The workflow: the plan of one benchmark run.

Temporal stores the result of every step in the run's event history. When a worker dies,
a new worker runs this code again from the top against that history: steps that already
finished return their stored results at once, and the run goes on where it stopped.
That only works if this code makes the same decisions every time, so it reads no clock,
draws no random numbers and does no I/O. All of that happens in the activities.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from resumable_eval.activities import Activities
    from resumable_eval.budget import Budget, worst_case
    from resumable_eval.shared import Answer, Batch, Faults, Provider, Question, Result, RunConfig, Summary, cost

RESERVE_PATCH = "reserve-worst-case"  # the change from version 1.0 to 1.1, see reserves_budget()


@workflow.defn
class EvalWorkflow:
    def __init__(self) -> None:
        self.summary = Summary(run_id="", status="running", answered=0, correct=0, used_fallback=0, spent=0.0)

    @workflow.run
    async def run(self, cfg: RunConfig) -> Summary:
        self.summary.run_id = cfg.run_id
        budget = Budget(cfg.budget, worst_case(cfg), reserve=self.reserves_budget())
        status, done = "completed", 0
        while done < cfg.questions:
            taken = budget.take(min(cfg.batch_size or cfg.in_flight, cfg.questions - done))
            if taken == 0:
                status = f"budget exhausted after question {done}"
                break
            ids = list(range(done, done + taken))
            results = await (self.batch(cfg, ids) if cfg.batch_size else self.window(cfg, ids))
            for result in results:  # in question order, so the sums do not depend on which call finished first
                budget.settle(result.cost)
                self.count(result, budget.spent)
            done += taken
        self.summary.status = status
        if cfg.approval_timeout_seconds is not None:
            await self.wait_for_approval(cfg.approval_timeout_seconds)
        return self.summary

    def reserves_budget(self) -> bool:
        """True for new runs: reserve the worst case before each question (version 1.1).

        A run that started on version 1.0 has no marker for this patch in its history, so this is
        False while it replays and the run keeps the old rule: its replay asks for the same steps.
        """
        return workflow.patched(RESERVE_PATCH)

    async def window(self, cfg: RunConfig, ids: list[int]) -> list[Result]:
        """Ask these questions at the same time. gather returns the results in question order."""
        return list(await asyncio.gather(*(self.question(cfg, q) for q in ids)))

    async def batch(self, cfg: RunConfig, ids: list[int]) -> list[Result]:
        """Ask the primary these questions in one long activity that reports its progress by heartbeat."""
        try:
            answers = await workflow.execute_activity_method(
                Activities.ask_batch,
                Batch(cfg.run_id, cfg.seed, ids, cfg.primary.name, cfg.faults, cfg.seconds_per_batch_question),
                start_to_close_timeout=timedelta(minutes=10),  # long: a dead worker is noticed by its heartbeats
                heartbeat_timeout=timedelta(seconds=cfg.heartbeat_timeout_seconds),
                retry_policy=retry_policy(cfg),
            )
        except ActivityError:
            answers = [None] * len(ids)
        # A question the batch could not answer is asked on its own, with retries and the fallback.
        return list(await asyncio.gather(*(self.question(cfg, q, a) for q, a in zip(ids, answers, strict=True))))

    async def question(self, cfg: RunConfig, question_id: int, from_batch: Answer | None = None) -> Result:
        """Answer one question and save its result. The run counts it only after the save."""
        result = await self.answer(cfg, question_id, from_batch)
        await workflow.execute_activity_method(
            Activities.save_result, result, start_to_close_timeout=timedelta(seconds=10)
        )
        return result

    async def answer(self, cfg: RunConfig, question_id: int, from_batch: Answer | None) -> Result:
        """The primary's answer; if it gives up, the fallback's; if that gives up too, an unanswered result."""
        if from_batch:
            return to_result(cfg.run_id, from_batch, cfg.primary, used_fallback=False)
        try:
            return to_result(cfg.run_id, await self.ask(cfg, cfg.primary, cfg.faults, question_id), cfg.primary, False)
        except ActivityError:
            workflow.logger.warning(f"question {question_id}: {cfg.primary.name} gave up, asking {cfg.fallback.name}")
        try:
            answer = await self.ask(cfg, cfg.fallback, cfg.fallback_faults, question_id)
            return to_result(cfg.run_id, answer, cfg.fallback, used_fallback=True)
        except ActivityError as err:
            workflow.logger.warning(f"question {question_id}: {cfg.fallback.name} gave up too, it stays unanswered")
            return Result(cfg.run_id, question_id, "", 0, True, "", False, 0.0, error=error_kind(err))

    async def ask(self, cfg: RunConfig, provider: Provider, faults: Faults, question_id: int) -> Answer:
        return await workflow.execute_activity_method(
            Activities.ask_provider,
            Question(cfg.run_id, cfg.seed, question_id, provider.name, faults),
            start_to_close_timeout=timedelta(seconds=cfg.call_timeout_seconds),
            retry_policy=retry_policy(cfg),
        )

    def count(self, result: Result, spent: float) -> None:
        s = self.summary
        if result.error:
            s.unanswered += 1
        else:
            s.answered += 1
            s.correct += result.correct
            s.used_fallback += result.used_fallback
        s.spent = spent

    async def wait_for_approval(self, timeout_seconds: float) -> None:
        """Wait for a person to approve or reject the summary. No decision in time ends as not approved."""
        self.summary.approval = "waiting"
        try:
            await workflow.wait_condition(lambda: self.summary.approval != "waiting", timeout=timeout_seconds)
        except TimeoutError:
            self.summary.approval = "not approved"

    @workflow.signal
    def approve(self) -> None:
        if self.summary.approval == "waiting":  # a decision counts only once the summary is ready
            self.summary.approval = "approved"

    @workflow.signal
    def reject(self) -> None:
        if self.summary.approval == "waiting":
            self.summary.approval = "rejected"

    @workflow.query
    def current_summary(self) -> Summary:
        return self.summary


def retry_policy(cfg: RunConfig) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=cfg.first_retry_seconds),
        backoff_coefficient=2.0,
        maximum_attempts=cfg.max_attempts,
    )


def to_result(run_id: str, answer: Answer, provider: Provider, used_fallback: bool) -> Result:
    price = cost(answer.prompt_tokens, answer.generated_tokens, provider)
    a = answer
    return Result(run_id, a.question_id, provider.name, a.attempt, used_fallback, a.sql, a.correct, price)


def error_kind(err: ActivityError) -> str:
    """The kind of the last failure: the type the provider gave its error, or TimeoutError for a hang."""
    cause = err.cause
    return cause.type if isinstance(cause, ApplicationError) and cause.type else type(cause).__name__
