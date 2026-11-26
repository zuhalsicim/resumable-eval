# resumable-eval

[![ci](https://github.com/zuhalsicim/resumable-eval/actions/workflows/ci.yml/badge.svg)](https://github.com/zuhalsicim/resumable-eval/actions/workflows/ci.yml)

An LLM benchmark run as a [Temporal](https://temporal.io) workflow, with retries, a fallback model and a cost budget that is never overspent. Kill the worker halfway, or the Temporal server itself, and the run still ends with exactly the results it would have had without the crash.

## The question

A long LLM job fails in the middle: the provider times out, returns errors, or the machine running the job dies.
How do you make a run of 1,034 model calls retry what failed, switch to a fallback model when the main one keeps failing, stop before the money runs out, and resume after a crash without losing an answer or counting one twice?
This repo answers that with Temporal, at the smallest size that still has every one of those parts.

## How it works

1. One run is one Temporal workflow ([`workflow.py`](resumable_eval/workflow.py)): it goes through the questions in order and asks the primary provider each one in an activity with a 1-second timeout and a retry policy of 5 attempts (waits of 0.1, 0.2, 0.4 and 0.8 s).
2. When all 5 attempts fail, the workflow asks the fallback provider the same question and records that it did. When the fallback gives up too, the question is recorded as unanswered, with the kind of the last error, and the run goes on.
3. Before a question starts, the workflow reserves the most it can cost: a prompt of 1,024 tokens (the longest in the data has 905) and the 256-token output cap of the benchmark, at the fallback's price, $0.01024. A question only starts if the spend plus every open reservation fits in the budget; after the answer, its reservation is replaced by the real cost. So the spend never passes the budget, and the run stops with `budget exhausted after question N` when the next reservation does not fit ([`budget.py`](resumable_eval/budget.py)).
4. A second activity writes each result to SQLite under the key (run, question); the first write wins, so writing it again changes nothing. The provider activity adds every call that returned an answer to a ledger.
5. Temporal stores the result of every finished step in the run's event history, so a new worker that takes over after a crash replays the workflow against that history and goes on at the first step that had not finished.

Options, all off by default: ask several questions at once (`in_flight`), ask the primary a batch of questions in one long activity that reports its progress by heartbeat (`batch_size`), and wait at the end for a person to approve or reject the summary (`approval_timeout_seconds`).

**No model is called.** The two providers return the answers that Qwen2.5-Coder-1.5B-Instruct really gave on the 1,034 questions of the Spider development set in my benchmark [sql-under-compression](https://github.com/zuhalsicim/sql-under-compression): `q4_k_m` is the primary, `f16` the fallback, each answer with its token counts and whether it was correct ([data/PROVENANCE.md](data/PROVENANCE.md)).
Faults are injected on purpose, planned by a seed so that every run with that seed meets the same ones: 2% of questions fail on every attempt, 8% fail on their first one or two attempts, and a quarter of those failures hang past the timeout instead of raising an error. Optionally, the first attempt of some questions is refused like an HTTP 429 with a retry-after, and every call takes a time drawn from a seeded distribution ([`faults.py`](resumable_eval/faults.py)).
The prices in [`prices.toml`](prices.toml) are illustrative: made up for this project, not anyone's price list.
The point is the orchestration (retries, fallback, budget, resume), not model quality.

## The crash tests

Each crash test starts a Temporal server and runs the same benchmark twice: once without interruption, and once with a crash, then compares the two.
It checks that the crashed run completes, that every question has exactly one result, that nothing was counted twice (the run's own count and spend equal the result rows, and the ledger shows how many provider calls really returned an answer), and that the results equal those of the uninterrupted run.

The worker crash kills the worker, in its own process, hard (SIGKILL) after K results are written and starts a fresh worker.
The server crash kills the Temporal server itself hard after K results, together with the worker. The server is the Temporal CLI dev server started with `--db-filename`, so its state is in a SQLite file; it is started again on the same file, a new worker is started, and the run goes on ([`server_crash.py`](resumable_eval/server_crash.py)).

Numbers from CI run [45837678497](https://github.com/zuhalsicim/resumable-eval/actions/runs/45837678497) (GitHub Actions, Ubuntu, seed 7). The 200-question worker crash is part of `pytest`, the other two are separate CI steps:

| | worker killed, 200 questions | worker killed, all 1,034 questions | server killed, 200 questions |
|---|---|---|---|
| Killed after | 80 results | 400 results | 80 results |
| Status of the crashed run | completed | completed | completed |
| Result rows | 200, none missing | 1,034, none missing | 200, none missing |
| Answers counted twice | 0 | 0 | 0 |
| Spend equals the sum of the rows | yes | yes | yes |
| Provider calls that returned an answer (ledger) | 200 | 1,034 | 200 |
| Same results and summary as the uninterrupted run | yes | yes | yes |
| Events in the run's history | 2,422 | 12,535 | 2,422 |
| Seconds, uninterrupted / with the crash | 36 / 38 | 269 / 281 | 36 / 38 |

Version 1.0 had 2,420 and 12,536 events and took 34 / 36 and 250 / 252 seconds ([CI run 45824885832](https://github.com/zuhalsicim/resumable-eval/actions/runs/45824885832)). The seconds grew because a dead question now fails 5 attempts instead of 4; the event count moves by one or two between runs, because the server sometimes handles two finished activities in one workflow task.

On all 1,034 questions the seed plans 20 dead and 87 flaky questions for the primary, with 207 errors and 28 hangs over the 5 attempts.
The run answers 20 questions with the fallback, gets 616 of 1,034 right (the primary alone: 614) and spends $0.5492 (`python -m resumable_eval run --questions 1034 --crash-after 400`).

Where the kill lands is chance.
In an earlier CI run ([45826694170](https://github.com/zuhalsicim/resumable-eval/actions/runs/45826694170), version 1.0) it landed on all 1,034 questions after a provider had answered and before the worker had reported the answer: the ledger had 1,035 answers, one question was answered twice, and the run still counted every question once and ended identical to the uninterrupted run.

A second test makes the worker die at that worst moment on purpose: right after the provider answered question 4, before the worker could report the answer.
Temporal never received it, so after the 1-second timeout it ran the call again on the new worker.
The ledger shows 11 provider answers for 10 questions, while the results and the spend count question 4 once.
Temporal runs an activity at least once, not exactly once, and that is why the writes are idempotent.

### Two crashes during one question

A crash loses the attempt that was running, and Temporal counts it as a timed-out attempt.
The fault plan depends only on the seed, the provider, the question and the attempt number. So a lost attempt that would have failed changes nothing, and one that would have answered moves the same answer to the next attempt: a crash can only use up an attempt.
An answerable question fails at most twice, so with 5 attempts it survives two crashes: 2 failures + 2 lost attempts + 1 answer.
The test picks a question that fails twice. The worker dies right after its third attempt answered, the next worker right after its fourth; the fifth attempt answers, and the results are identical. The ledger shows attempts 3, 4 and 5 for that question.
With the 4 attempts of version 1.0, the same two crashes send the question to the fallback and the results differ: that is why version 1.1 has 5.

### Heartbeats: a long batch resumes where it stopped

With `batch_size=K` the primary gets K questions in one long activity, which sends a heartbeat with the answers so far after each question. Its start-to-close timeout is 10 minutes, its heartbeat timeout 2 seconds.
In the test, 10 questions take 0.3 s each and the worker dies right after question 6 answered. The first attempt answered questions 0 to 6. The server noticed the missing heartbeats, and the second attempt started at question 6, not 0: the last heartbeat that reached the server carried the answers to questions 0 to 5. (The SDK sends heartbeats at most every 1.6 s, 80% of the heartbeat timeout, so the server's copy can be a little behind; that part is asked again.) The history records the second attempt with a heartbeat timeout as its last failure, and the results equal the uninterrupted run (CI run 45837678497).
A question that fails inside a batch is asked again on its own, with retries and the fallback, so batch mode ends with the same results as one activity per question.

### Two workers, four questions in flight

With `in_flight=4` the workflow asks four questions at once with `asyncio.gather`, which returns the results in question order; the workflow adds them up in that order, so the sums do not depend on which call finished first.
The test runs two worker processes on the same task queue and kills the first one after 40 of 100 results. The other worker finishes the run. The ledger shows that both workers answered questions, and the results and the summary equal an uninterrupted run that asks one question at a time (CI run 45837678497: 100 provider answers, 1,024 history events, 18 s one at a time, 15 s four at a time with the crash).

### Versioning: a run that started on the old code

The budget rule of version 1.1 changes decisions: a run that started on 1.0 may already have asked a question where the new rule would stop. So the new rule sits behind `workflow.patched("reserve-worst-case")`. A new run takes it and gets a marker in its history; a run without the marker keeps the old rule.
[`tests/old_workflow.py`](tests/old_workflow.py) is the workflow of version 1.0, unchanged apart from its name. The test starts a run with it, stops that worker once 6 of the 12 results are saved, and lets a worker with the new code finish the run: all 12 questions by the old rule, with no failed workflow task. A new run with the same budget stops after question 4, within the budget. The SDK's `Replayer` then replays the old run's history: with the patch it passes; without it ([`tests/workflow_without_patch.py`](tests/workflow_without_patch.py)) it fails with a non-determinism error.

### Approval

With `approval_timeout_seconds` set, the workflow waits after the last question for an `approve` or `reject` signal, and the query `current_summary` shows the summary meanwhile. Nobody deciding in time ends as `not approved`. No worker has to stay busy while the run waits: the wait is a timer on the Temporal server.

## What each test proves

| Test | What it proves |
|---|---|
| `test_crash.py::test_a_killed_worker_is_replaced_and_the_run_ends_as_if_nothing_happened` | A worker killed hard after 80 of 200 results is replaced, and the run ends identical to an uninterrupted one. The CI step does the same on all 1,034 questions, killed after 400. |
| `test_crash.py::test_an_answer_lost_before_it_reached_temporal_is_asked_again_but_counted_once` | Temporal runs an activity at least once: an answer lost before the worker reported it is asked again (11 ledger answers for 10 questions) and counted once. |
| `test_crash.py::test_two_crashes_during_one_questions_retries_do_not_change_its_answer` | Two workers die in the retry window of the same question, each right after an attempt answered; the fifth attempt answers and the results are identical. With four attempts the same crashes send the question to the fallback. |
| `test_server_crash.py` and the CI step `server-crash-test` | The Temporal server killed hard and restarted on its state file loses nothing: the run ends identical to an uninterrupted one. |
| `test_crash.py::test_a_batch_resumes_from_its_last_heartbeat_after_the_worker_dies` | A long batch activity whose worker dies is noticed by the heartbeat timeout, not the 10-minute start-to-close timeout, and its next attempt resumes from the heartbeat details, not from the start of the batch. |
| `test_crash.py::test_two_workers_share_the_run_and_one_of_them_is_killed` | Two workers on one task queue, four questions in flight, one worker killed: the survivor finishes, and the results equal a run that asks one question at a time. |
| `test_versioning.py` | A run started on the code of version 1.0 finishes on the new code by the old budget rule, with no failed workflow task; a new run takes the new rule; replaying the old history without `workflow.patched` fails with a non-determinism error. |
| `test_approval.py` | The run waits for `approve` or `reject`, shows its summary to a query meanwhile, and ends as `not approved` when nobody decides in time. |
| `test_workflow.py::test_failed_calls_are_retried_until_they_answer` | Retries happen, and the answer comes from the first attempt the plan lets through. |
| `test_workflow.py::test_the_fallback_answers_when_the_primary_gives_up` | After five failed attempts the fallback answers, at the fallback's price. |
| `test_workflow.py::test_a_question_both_providers_give_up_on_is_recorded_as_unanswered_and_the_run_goes_on` | When the fallback fails too, the question is saved as unanswered with its error kind, counted, and the run completes. |
| `test_workflow.py::test_the_budget_reserves_the_worst_case_and_the_spend_never_passes_it` | The run stops when the next worst case does not fit, and the spend stays within the budget. |
| `test_workflow.py::test_questions_in_flight_at_once_give_the_same_results_as_one_at_a_time` | Asking four questions at once changes no result and no sum. |
| `test_workflow.py::test_batch_mode_gives_the_same_results_as_one_activity_per_question` | A question that fails inside a batch is asked again on its own; batch mode ends with the same results. |
| `test_workflow.py::test_a_rate_limited_call_is_retried_after_the_wait_the_provider_asked_for` | A 429-like error with a retry-after makes Temporal wait that long (1.5 s), not the policy's 0.1 s. |
| `test_faults_and_store.py` | The fault plan repeats with the seed; two lost attempts cannot send an answerable question to the fallback; rate limits move no other fault; latency is seeded and below the timeout; the first write of a result wins; prices come from `prices.toml`; no answer costs more than the worst case; reserving never lets the spend pass the limit, while the old rule could. |

## Run it

Python 3.12. The first run downloads the Temporal dev server; the Temporal SDK does that by itself.
The server crash test needs the Temporal CLI as a program: put `temporal` on the PATH or set `TEMPORAL_CLI` to it (the file the SDK downloaded works, for example `%TEMP%\temporal-sdk-python-1.37.0.exe` on Windows). Without it, `pytest` skips that test.

```
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

python -m resumable_eval run --questions 200 --crash-after 80     # kill the worker after 80 results
python -m resumable_eval run --questions 200 --budget 0.05        # stop before 5 cents could be passed
python -m resumable_eval run --questions 200 --in-flight 4 --workers 2 --crash-after 80
python -m resumable_eval run --questions 20 --ui                  # keep the server open and look at the run in the web UI
python -m resumable_eval crash-test --questions 1034 --crash-after 400
python -m resumable_eval server-crash-test --questions 200 --crash-after 80
pytest -v -s
```

What a run prints (on Windows 11; the seconds differ by machine):

```
$ python -m resumable_eval run --questions 200 --crash-after 80
run-e2ee9620: completed
answered 200 of 200, 102 correct, 2 by the fallback
spent $0.0855 of $1.00
faults planned for q4_k_m: dead questions 2, flaky questions 12, errors 25, hangs 3
worker killed after 80 results, another worker took over
provider answers in ledger: 200, result rows: 200 (in runs\results.db)
history events: 2422, seconds: 38.6

$ python -m resumable_eval run --questions 200 --budget 0.05
run-b3a7eae0: budget exhausted after question 98
answered 98 of 200, 57 correct, 1 by the fallback
spent $0.0399 of $0.05
faults planned for q4_k_m: dead questions 1, flaky questions 6, errors 14, hangs 1
provider answers in ledger: 98, result rows: 98 (in runs\results.db)
history events: 1189, seconds: 19.1
```

The budget run stops after question 98 with $0.0399 spent: the next question could cost up to $0.01024, and $0.0399 + $0.01024 would pass $0.05.

## Files

| File | What it does |
|---|---|
| [`resumable_eval/workflow.py`](resumable_eval/workflow.py) | The workflow: the loop over the questions, the fallback, unanswered questions, questions in flight, batch mode, the approval step and the one `workflow.patched`. No clock, no random numbers, no I/O, so it can be replayed. |
| [`resumable_eval/budget.py`](resumable_eval/budget.py) | The budget: reserve the worst case of a question before it starts, settle to the real cost after. |
| [`resumable_eval/activities.py`](resumable_eval/activities.py) | The provider activity (recorded answers and planned faults), the batch activity with heartbeats, and the activity that saves a result. |
| [`resumable_eval/faults.py`](resumable_eval/faults.py) | The fault plan: errors, hangs, rate limits and latency, all from the seed. |
| [`resumable_eval/store.py`](resumable_eval/store.py) | SQLite: the results table keyed by run and question (the first write wins), and the ledger of provider calls. |
| [`resumable_eval/worker.py`](resumable_eval/worker.py) | The worker that runs the workflow and activity code. |
| [`resumable_eval/harness.py`](resumable_eval/harness.py) | Starts workers as separate processes, kills them, and compares a crashed run with an uninterrupted one. |
| [`resumable_eval/server_crash.py`](resumable_eval/server_crash.py) | Starts the Temporal CLI dev server on a state file, kills it, starts it again, and compares. |
| [`resumable_eval/shared.py`](resumable_eval/shared.py) | The messages passed between the parts, the providers with their prices, and the cost rule. |
| [`prices.toml`](prices.toml) | The illustrative prices. |
| [`tests/`](tests) | The tests in the table above, and the two workflow versions the versioning test needs. |

## Limits

- The model calls are replayed. No answer changes on a retry; that is what lets the tests compare a crashed run with an uninterrupted one exactly. With a real provider you would compare the stored answers instead. Latency and rate limits are drawn from the seed, not measured.
- The reservation is cautious: it assumes the longest prompt and the dearer provider for every question, $0.01024, about 19 times the average cost of a question in the 1,034-question run ($0.5492 / 1,034). So a run stops while about a cent of the budget is still free. A real system would count each prompt before the call.
- A question survives two crashes during its retries. A third one in the same question could still send it to the fallback.
- The server crash test kills the server process, not the machine, so data the operating system had not yet written to disk is not put to the test. It uses the dev server with SQLite, not a cluster.
- Questions in flight go in windows: a window waits for its slowest question before the next one starts. There is no client-side rate limit per provider.
- Batch mode asks the primary only and makes one try per question inside the batch; a question that fails there goes back to the normal path.
- Retries add no events to the history, but each question adds about 12, so a run of 1,034 questions has 12,535 events. Temporal allows 51,200 events per run, which this design reaches at about 4,200 questions; a longer run would start a fresh history every few thousand questions (continue-as-new) or give each batch its own child workflow. Not done.
- Versioning is shown with `workflow.patched` only, not with worker versioning (build IDs).
- The approval signals carry no identity: whoever can send a signal can approve.
- The prices are made up, not anyone's price list.

## Licence

MIT
