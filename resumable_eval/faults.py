"""The fault plan: which attempt of which call fails, how, and how long each call takes.

Everything here depends only on the seed, the provider, the question and the attempt number.
So every run with the same seed meets the same faults, and a crash cannot change the plan:
an attempt lost in a crash that would have failed changes nothing, and one that would have
answered moves the same answer to the next attempt.
"""

import math
import random

from resumable_eval.shared import Faults

LATENCY_CAP_SECONDS = 0.5  # below the 1-second call timeout, so a slow call alone never fails


def planned_fault(seed: int, provider: str, question_id: int, attempt: int, faults: Faults) -> str | None:
    """The fault this attempt meets: "error", "hang", "rate limit" or None.

    A question is either dead (every attempt fails) or flaky (the first one or two attempts
    fail) or fine. On top of that, the first attempt of some questions is refused with a rate limit.
    Each of those decisions uses its own random generator, so adding one did not move the others.
    """
    if attempt == 1 and random.Random(f"{seed}:{provider}:{question_id}:rate").random() < faults.rate_limit_rate:
        return "rate limit"
    rng = random.Random(f"{seed}:{provider}:{question_id}")
    draw = rng.random()
    if draw < faults.dead_rate:
        return "error"
    if draw < faults.dead_rate + faults.flaky_rate:
        failures = ["hang" if rng.random() < faults.hang_share else "error" for _ in range(rng.choice([1, 2]))]
        if attempt <= len(failures):
            return failures[attempt - 1]
    return None


def planned_latency(seed: int, provider: str, question_id: int, attempt: int, faults: Faults) -> float:
    """Seconds this call takes: drawn from a log-normal distribution around the median, at most 0.5 s."""
    if faults.median_latency_seconds == 0:
        return 0.0
    rng = random.Random(f"{seed}:{provider}:{question_id}:{attempt}:latency")
    return min(rng.lognormvariate(math.log(faults.median_latency_seconds), 0.5), LATENCY_CAP_SECONDS)
