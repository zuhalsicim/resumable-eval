"""The budget of a run: reserve the most a question can cost before asking it, settle to its real cost after.

The workflow imports this module, so everything here must be deterministic.

Why it never overspends: a question is only started if the spend plus the worst case of every
open question, this one included, fits in the limit. An answer never costs more than the worst
case, so replacing a reservation by the real cost can only lower the total.
"""

from dataclasses import dataclass

from resumable_eval.shared import RunConfig, cost


def worst_case(cfg: RunConfig) -> float:
    """The most one question can cost: the longest prompt and max_tokens of output, at the dearer provider.

    The dearer one, because the fallback may have to answer.
    """
    return max(cost(cfg.max_prompt_tokens, cfg.max_tokens, p) for p in (cfg.primary, cfg.fallback))


@dataclass
class Budget:
    limit: float  # dollars
    worst_case: float  # the most one question can cost
    reserve: bool = True  # False only for runs that started on version 1.0 (see workflow.py)
    spent: float = 0.0
    open: int = 0  # questions reserved and not settled yet

    def take(self, wanted: int) -> int:
        """Reserve up to `wanted` more questions. Returns how many fit."""
        taken = 0
        while taken < wanted and self.fits():
            self.open += 1
            taken += 1
        return taken

    def fits(self) -> bool:
        if not self.reserve:
            # The rule of version 1.0: one question at a time, started while the spend is below the limit.
            return self.open == 0 and self.spent < self.limit
        return self.spent + (self.open + 1) * self.worst_case <= self.limit

    def settle(self, cost: float) -> None:
        """A reserved question has finished: replace its reservation by what it really cost."""
        self.open -= 1
        self.spent += cost
