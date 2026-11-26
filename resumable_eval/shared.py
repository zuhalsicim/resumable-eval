"""The messages passed between the workflow, the activities and the harness, and the cost rule.

The workflow imports this module, so everything here must be deterministic.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Provider:
    name: str  # a precision of the benchmark, which is also the name of its file in data/
    input_price: float  # dollars per million prompt tokens (illustrative)
    output_price: float  # dollars per million generated tokens (illustrative)


# Illustrative prices: the small primary is cheap, the fallback costs five times as much.
PRIMARY = Provider("q4_k_m", input_price=1.00, output_price=4.00)
FALLBACK = Provider("f16", input_price=5.00, output_price=20.00)


@dataclass(frozen=True)
class Faults:
    """Faults injected into a provider, as shares of all questions."""

    dead_rate: float = 0.02  # the provider fails every attempt on these questions
    flaky_rate: float = 0.08  # the provider fails the first one or two attempts, then answers
    hang_share: float = 0.25  # share of those failures that hang past the timeout instead of erroring


NO_FAULTS = Faults(dead_rate=0.0, flaky_rate=0.0, hang_share=0.0)


@dataclass
class RunConfig:
    run_id: str
    questions: int  # the run asks question ids 0 .. questions - 1
    budget: float  # dollars
    seed: int = 7  # fixes which questions meet which faults
    primary: Provider = PRIMARY
    fallback: Provider = FALLBACK
    faults: Faults = field(default_factory=Faults)  # injected into the primary only
    max_attempts: int = 4  # per provider and question
    first_retry_seconds: float = 0.1  # then 0.2, 0.4, ...
    call_timeout_seconds: float = 1.0


@dataclass
class Question:
    run_id: str
    seed: int
    question_id: int
    provider: str
    faults: Faults


@dataclass
class Answer:
    question_id: int
    provider: str
    attempt: int
    sql: str
    correct: bool
    prompt_tokens: int
    generated_tokens: int


@dataclass
class Result:
    run_id: str
    question_id: int
    provider: str
    attempt: int
    used_fallback: bool
    sql: str
    correct: bool
    cost: float


@dataclass
class Summary:
    run_id: str
    status: str  # "completed" or "budget exhausted after question N"
    answered: int
    correct: int
    used_fallback: int
    spent: float


def cost(prompt_tokens: int, generated_tokens: int, provider: Provider) -> float:
    """Dollars for one answer, from its recorded token counts."""
    return (prompt_tokens * provider.input_price + generated_tokens * provider.output_price) / 1_000_000
