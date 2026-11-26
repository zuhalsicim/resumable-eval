"""Copy the recorded answers of two precisions from a clone of sql-under-compression into data/.

python scripts/copy_data.py ../sql-under-compression
"""

import json
import sys
from pathlib import Path

PRECISIONS = ["q4_k_m", "f16"]
FIELDS = ["db_id", "sql", "correct", "prompt_tokens", "generated_tokens"]


def main(benchmark_repo: str) -> None:
    source = Path(benchmark_repo) / "results-full-dev"
    target = Path(__file__).resolve().parent.parent / "data"
    for precision in PRECISIONS:
        run = json.loads((source / f"{precision}.json").read_text(encoding="utf-8"))
        rows = [{"question_id": q["index"], **{f: q[f] for f in FIELDS}} for q in run["per_question"]]
        lines = ",\n".join(json.dumps(row) for row in rows)
        (target / f"{precision}.json").write_text(f"[\n{lines}\n]\n", encoding="utf-8")
        print(f"{precision}: {len(rows)} answers, {sum(r['correct'] for r in rows)} correct")


if __name__ == "__main__":
    main(sys.argv[1])
