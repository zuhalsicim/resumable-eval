"""The messages passed between the workflow, the activities and the harness, the prices and the cost rule.

The workflow imports this module, so everything here must be deterministic. The prices are read from
prices.toml once, when this module is imported. They travel inside each run's settings, so a run keeps
the prices it started with even if the file changes.
"""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

PRICES = tomllib.loads((Path(__file__).resolve().parent.parent / "prices.toml").read_text(encoding="utf-8"))


@dataclass(frozen=True)
class Provider:
    name: str  # a precision of the benchmark, which is also the name of its file in data/
    input_price: float  # dollars per million prompt tokens (illustrative)
    output_price: float  # dollars per million generated tokens (illustrative)


def provider(name: str) -> Provider:
    return Provider(name, input_price=PRICES[name]["input"], output_price=PRICES[name]["output"])


PRIMARY = provider("q4_k_m")
FALLBACK = provider("f16")


@dataclass(frozen=True)
class Faults:
    """Faults injected into a provider, as shares of all questions."""

    dead_rate: float = 0.02  # the provider fails every attempt on these questions
    flaky_rate: float = 0.08  # the provider fails the first one or two attempts, then answers
    hang_share: float = 0.25  # share of those failures that hang past the timeout instead of erroring
    rate_limit_rate: float = 0.0  # the first attempt is refused like an HTTP 429 (off by default)
    retry_after_seconds: float = 1.0  # how long the provider asks the client to wait after a 429
    median_latency_seconds: float = 0.0  # each call takes a time drawn around this median (off by default)


NO_FAULTS = Faults(dead_rate=0.0, flaky_rate=0.0, hang_share=0.0)


@dataclass
class RunConfig:
    run_id: str
    questions: int  # the run asks question ids 0 .. questions - 1
    budget: float  # dollars
    seed: int = 7  # fixes which questions meet which faults
    primary: Provider = PRIMARY
    fallback: Provider = FALLBACK
    faults: Faults = field(default_factory=Faults)  # injected into the primary
    fallback_faults: Faults = NO_FAULTS  # injected into the fallback: none by default
    max_attempts: int = 5  # per provider and question
    first_retry_seconds: float = 0.1  # then 0.2, 0.4, 0.8
    call_timeout_seconds: float = 1.0
    max_prompt_tokens: int = 1024  # an upper bound on every prompt (the longest in data/ has 905 tokens)
    max_tokens: int = 256  # the cap on generated tokens of one call, as in the benchmark
    in_flight: int = 1  # questions asked at the same time
    batch_size: int = 0  # 0: one activity per question. K: one long activity per K questions, with heartbeats
    seconds_per_batch_question: float = 0.2  # how long a batch call takes for each question
    heartbeat_timeout_seconds: float = 2.0
    approval_timeout_seconds: float | None = None  # if set, wait this long for approve or reject at the end


@dataclass
class Question:
    run_id: str
    seed: int
    question_id: int
    provider: str
    faults: Faults


@dataclass
class Batch:
    run_id: str
    seed: int
    question_ids: list[int]
    provider: str
    faults: Faults
    seconds_per_question: float


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
    provider: str  # "" if no provider answered
    attempt: int
    used_fallback: bool
    sql: str
    correct: bool
    cost: float
    error: str = ""  # for an unanswered question: the kind of the fallback's last error


@dataclass
class Summary:
    run_id: str
    status: str  # "running", "completed" or "budget exhausted after question N"
    answered: int
    correct: int
    used_fallback: int  # questions the fallback answered
    spent: float
    unanswered: int = 0  # questions that both providers gave up on
    approval: str = ""  # "" (no approval step), "waiting", "approved", "rejected" or "not approved"


def cost(prompt_tokens: int, generated_tokens: int, provider: Provider) -> float:
    """Dollars for one answer, from its recorded token counts."""
    return (prompt_tokens * provider.input_price + generated_tokens * provider.output_price) / 1_000_000
