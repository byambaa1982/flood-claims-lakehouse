"""Ask a question from the command line.

    python -m ai.ask "Which five states had the most claims paid in 2017?"
"""

from __future__ import annotations

import sys

from ai.agent import ask


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    answer = ask(" ".join(sys.argv[1:]))
    print(answer.text)
    print()
    for number, sql in enumerate(answer.queries, start=1):
        print(f"--- query {number} ---")
        print(sql.strip())
    print()
    print(
        f"{len(answer.queries)} queries, {answer.bytes_scanned / 1e6:.1f} MB scanned, "
        f"{answer.input_tokens} input and {answer.output_tokens} output tokens"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
