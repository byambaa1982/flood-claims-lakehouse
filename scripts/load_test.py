"""Measure requests per second and latency for one API route.

    python scripts/load_test.py --url "http://127.0.0.1:8000/claims/by-state?year=2017" \
        --key change-me --requests 20000 --processes 4 --concurrency 10

One Python process can only send several hundred requests a second, which is
less than the API can serve. Use --processes to run several generators at once;
their results are combined.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from multiprocessing import Pool

import httpx


async def generate(url: str, key: str, total: int, concurrency: int) -> tuple[list[float], int, float]:
    latencies: list[float] = []
    errors = 0
    remaining = total
    limits = httpx.Limits(max_connections=concurrency)

    async with httpx.AsyncClient(headers={"X-API-Key": key}, limits=limits, timeout=30) as client:
        # One request first, so a cold cache does not count against the test.
        (await client.get(url)).raise_for_status()

        async def worker() -> None:
            nonlocal remaining, errors
            while remaining > 0:
                remaining -= 1
                started = time.perf_counter()
                response = await client.get(url)
                latencies.append(time.perf_counter() - started)
                if response.status_code != 200:
                    errors += 1

        started = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        elapsed = time.perf_counter() - started

    return latencies, errors, elapsed


def one_process(args: tuple[str, str, int, int]) -> tuple[list[float], int, float]:
    return asyncio.run(generate(*args))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--url", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--requests", type=int, default=20000, help="total across all processes")
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=10, help="connections per process")
    args = parser.parse_args()

    per_process = args.requests // args.processes
    job = (args.url, args.key, per_process, args.concurrency)
    with Pool(args.processes) as pool:
        results = pool.map(one_process, [job] * args.processes)

    latencies = sorted(value for result in results for value in result[0])
    errors = sum(result[1] for result in results)
    # The generators run side by side, so their rates add up.
    rate = sum(len(result[0]) / result[2] for result in results)

    def percentile(share: float) -> float:
        return latencies[min(len(latencies) - 1, int(len(latencies) * share))] * 1000

    print(f"requests      {len(latencies)}")
    print(f"generators    {args.processes} x {args.concurrency} connections")
    print(f"errors        {errors}")
    print(f"requests/sec  {rate:.0f}")
    print(f"median ms     {statistics.median(latencies) * 1000:.1f}")
    print(f"p95 ms        {percentile(0.95):.1f}")
    print(f"p99 ms        {percentile(0.99):.1f}")


if __name__ == "__main__":
    main()
