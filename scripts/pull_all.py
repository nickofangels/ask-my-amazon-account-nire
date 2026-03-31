"""
pull_all.py — Orchestrator: pull data, transform, and build keyword intelligence.

Usage:
    python -m scripts.pull_all
    python -m scripts.pull_all --skip-listings --skip-backfill

Steps:
    1. pull_listings     — ASIN/SKU catalog snapshot
    2. pull_sales_traffic — daily brand-level sales & traffic
    3. pull_search_terms  — Brand Analytics search term data
    4. backfill           — 5 raw report types (monthly, into Supabase)
    5. transform          — raw tables → derived dashboard tables
    6. build_asin_keywords — ASIN-keyword scoring matrix
    7. build_keywords      — keyword targets aggregation
"""

import argparse
import time
import importlib


STEPS = [
    ("listings",        "scripts.pull_listings",       "pull_listings"),
    ("sales-traffic",   "scripts.pull_sales_traffic",  "pull_sales_traffic"),
    ("search-terms",    "scripts.pull_search_terms",   "pull_search_terms"),
    ("backfill",        None,                          None),  # special: runs backfill.py
    ("transform",       "db.transform",                "run_all"),
    ("asin-keywords",   "db.build_asin_keywords",      "main"),
    ("keywords",        "db.build_keywords",            "main"),
]


def run_step(name, module_path, func_name):
    """Run one pipeline step. Returns (success, elapsed_seconds)."""
    t0 = time.time()
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"{'='*60}")

    try:
        if name == "backfill":
            # backfill.py has its own CLI; import and call main()
            import backfill
            backfill.main()
        else:
            mod = importlib.import_module(module_path)
            fn = getattr(mod, func_name)
            fn()
        elapsed = time.time() - t0
        print(f"  {name} completed in {elapsed:.1f}s")
        return True, elapsed
    except Exception as exc:
        elapsed = time.time() - t0
        print(f"  {name} FAILED after {elapsed:.1f}s: {exc}")
        return False, elapsed


def main():
    parser = argparse.ArgumentParser(description="Run the full Nire Beauty data pipeline")
    for step_name, _, _ in STEPS:
        parser.add_argument(
            f"--skip-{step_name}",
            action="store_true",
            help=f"Skip the {step_name} step",
        )
    args = parser.parse_args()

    t_start = time.time()
    results = []

    for step_name, module_path, func_name in STEPS:
        flag = f"skip_{step_name.replace('-', '_')}"
        if getattr(args, flag, False):
            print(f"\n  Skipping {step_name}")
            results.append((step_name, "SKIPPED", 0))
            continue

        ok, elapsed = run_step(step_name, module_path, func_name)
        results.append((step_name, "OK" if ok else "FAILED", elapsed))

    # Summary
    total = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE — {total:.0f}s total")
    print(f"{'='*60}")
    for name, status, elapsed in results:
        print(f"  {status:<8} {name:<20} {elapsed:.1f}s")


if __name__ == "__main__":
    main()
