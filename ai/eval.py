"""Measure how often the agent gets the right number.

Each case is a question plus a hand-written SQL statement that returns the
true answer as a single value. The agent is asked the question, and its answer
passes if the true value appears in it (numbers within 0.5%, text ignoring
case). Results are written to ai/eval_results.json.

    python -m ai.eval
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from ai.agent import MODEL_ID, ask
from ai.athena import run_query

CASES = [
    {
        "question": "How many flood insurance claims are in the data in total?",
        "truth_sql": "select count(*) from nfip_claims",
    },
    {
        "question": "How many claims had a date of loss in 2017?",
        "truth_sql": "select count(*) from nfip_claims where year_of_loss = 2017",
    },
    {
        "question": "Which state had the highest total amount paid across all years? Give the two-letter code.",
        "truth_sql": "select state from nfip_claims group by 1 order by sum(total_paid) desc limit 1",
    },
    {
        "question": "What was the total amount paid on claims in Texas in 2017, in dollars?",
        "truth_sql": "select round(sum(total_paid)) from nfip_claims where state = 'TX' and year_of_loss = 2017",
    },
    {
        "question": "Which named flood event has the most claims? Give the event name.",
        "truth_sql": (
            "select flood_event from nfip_claims where flood_event is not null "
            "group by 1 order by count(*) desc limit 1"
        ),
    },
    {
        "question": "What is the average total paid per claim in Florida, in dollars, for claims where something was paid?",
        "truth_sql": "select round(avg(total_paid)) from nfip_claims where state = 'FL' and total_paid > 0",
    },
    {
        "question": "In which year of loss was the total amount paid the highest?",
        "truth_sql": "select year_of_loss from nfip_claims group by 1 order by sum(total_paid) desc limit 1",
    },
    {
        "question": "How many claims in New Jersey were for a primary residence?",
        "truth_sql": "select count(*) from nfip_claims where state = 'NJ' and is_primary_residence",
    },
    {
        "question": "What percentage of all claims had nothing paid (total paid of zero)? Round to one decimal.",
        "truth_sql": "select round(100.0 * count_if(total_paid = 0) / count(*), 1) from nfip_claims",
    },
    {
        "question": "Which calendar month, as a number from 1 to 12, has the most claims by date of loss?",
        "truth_sql": "select month(date_of_loss) from nfip_claims group by 1 order by count(*) desc limit 1",
    },
]

NUMBER = re.compile(r"(-?\d[\d,]*\.?\d*)\s*(billion|million|thousand)?", re.IGNORECASE)
SCALE = {"": 1, "thousand": 1e3, "million": 1e6, "billion": 1e9}


def numbers_in(text: str) -> list[float]:
    """Every number in the text, reading '9.08 billion' as 9,080,000,000."""
    values = []
    for digits, word in NUMBER.findall(text):
        cleaned = digits.replace(",", "").rstrip(".")
        if cleaned and cleaned != "-":
            values.append(float(cleaned) * SCALE[word.lower()])
    return values


def answer_matches(truth: str, answer: str) -> bool:
    try:
        expected = float(truth)
    except ValueError:
        return truth.strip().lower() in answer.lower()
    tolerance = max(abs(expected) * 0.005, 0.051)
    return any(abs(found - expected) <= tolerance for found in numbers_in(answer))


def main() -> int:
    results = []
    for case in CASES:
        truth = run_query(case["truth_sql"]).rows[0][0]
        answer = ask(case["question"])
        passed = answer_matches(truth, answer.text)
        results.append(
            {
                "question": case["question"],
                "truth": truth,
                "passed": passed,
                "answer": answer.text,
                "queries": answer.queries,
                "input_tokens": answer.input_tokens,
                "output_tokens": answer.output_tokens,
                "bytes_scanned": answer.bytes_scanned,
            }
        )
        print(f"{'PASS' if passed else 'FAIL'}  {case['question']}  (truth: {truth})")

    passed_count = sum(result["passed"] for result in results)
    summary = {
        "model": MODEL_ID,
        "passed": passed_count,
        "total": len(results),
        "input_tokens": sum(result["input_tokens"] for result in results),
        "output_tokens": sum(result["output_tokens"] for result in results),
        "bytes_scanned": sum(result["bytes_scanned"] for result in results),
        "results": results,
    }
    Path(__file__).with_name("eval_results.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\n{passed_count} of {len(results)} passed")
    return 0 if passed_count == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
