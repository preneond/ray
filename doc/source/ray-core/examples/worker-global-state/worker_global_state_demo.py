"""
Demonstrates that Ray reuses worker processes across tasks within the same job,
so module-level global state (counters, manual caches) persists between task
invocations on the same worker.

Key findings from the Ray source (worker_pool.cc, worker.cc):
- An idle worker is reused if its job_id matches the incoming task's job_id.
- Workers are NEVER shared across different jobs; cross-job contamination is impossible.
- Within a single job, tasks run sequentially on the same worker and share all
  module-level state (counters, dicts, etc. — including functools.lru_cache).

NOTE on functools.lru_cache: lru_cache-wrapped functions cannot be pickled by
reference when they are defined in __main__, so the example below uses a plain
dict to demonstrate the same persistence semantics. In real worker code that
imports lru_cache'd helpers from a proper module (not __main__), the cache
would persist across tasks in exactly the same way.

Run:
    uv run python worker_global_state_demo.py
"""

import os

import ray

# ── Module-level state that lives for the lifetime of the worker process ─────
# cloudpickle captures these by value when first serialising the task, then the
# worker's __main__ owns them and they mutate across task calls.

_invocation_count: int = 0
_result_cache: dict = {}  # manual stand-in for functools.lru_cache


# ── Ray tasks ─────────────────────────────────────────────────────────────────


@ray.remote
def task_with_shared_worker(n: int) -> dict:
    """Default Ray behaviour: worker process is reused across task calls.

    The module-level counter and result cache accumulate across invocations
    because the same Python process handles multiple tasks sequentially.
    """
    global _invocation_count, _result_cache
    _invocation_count += 1

    cache_hit = n in _result_cache
    if not cache_hit:
        _result_cache[n] = n * n  # simulated expensive computation

    return {
        "pid": os.getpid(),
        "worker_invocation_count": _invocation_count,
        "cache_size": len(_result_cache),
        "cache_hit": cache_hit,
        "n": n,
        "result": _result_cache[n],
    }


@ray.remote(max_calls=1)
def task_with_fresh_worker(n: int) -> dict:
    """max_calls=1 forces Ray to exit the worker after every task and spawn a
    fresh process for the next one.  Each task sees a clean module state:
    counter starts at 0, cache is empty.
    """
    global _invocation_count, _result_cache
    _invocation_count += 1

    cache_hit = n in _result_cache
    if not cache_hit:
        _result_cache[n] = n * n

    return {
        "pid": os.getpid(),
        "worker_invocation_count": _invocation_count,
        "cache_size": len(_result_cache),
        "cache_hit": cache_hit,
        "n": n,
        "result": _result_cache[n],
    }


# ── Helpers ───────────────────────────────────────────────────────────────────


def _print_result(call_num: int, n: int, r: dict) -> None:
    hit = "HIT " if r["cache_hit"] else "MISS"
    print(
        f"  call {call_num:>2}: n={n:<3}  pid={r['pid']}  "
        f"global_counter={r['worker_invocation_count']}  "
        f"cache=[{hit}] size={r['cache_size']}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    # num_cpus=1: only one worker slot available, so all tasks queue on the
    # same worker, making the shared-state effect deterministic to observe.
    ray.init(num_cpus=1, ignore_reinit_error=True)

    # Repeated inputs: n=42 appears three times to surface cache hits.
    inputs = [42, 42, 7, 42, 7]

    # ── Part 1: default shared-worker behaviour ───────────────────────────────
    print()
    print("=" * 65)
    print("PART 1: shared worker (default Ray behaviour within a job)")
    print("  Tasks run sequentially on the same worker process.")
    print("  Global counter keeps climbing; cache hits accumulate.")
    print("=" * 65)

    for i, n in enumerate(inputs, start=1):
        r = ray.get(task_with_shared_worker.remote(n))
        _print_result(i, n, r)

    print()
    print("  => same pid every row  → same worker process reused")
    print("  => global_counter grows 1→5")
    print("  => n=42 and n=7 cache-HIT after their first MISS")

    # ── Part 2: fresh worker per task ────────────────────────────────────────
    print()
    print("=" * 65)
    print("PART 2: fresh worker per task (max_calls=1)")
    print("  Ray exits the worker after every task, spawning a new process.")
    print("  Global counter always starts at 1; cache is always empty.")
    print("=" * 65)

    for i, n in enumerate(inputs, start=1):
        r = ray.get(task_with_fresh_worker.remote(n))
        _print_result(i, n, r)

    print()
    print("  => different pid each row  → new worker process each time")
    print("  => global_counter is always 1; cache is always MISS, size=1")

    ray.shutdown()


if __name__ == "__main__":
    main()
