"""
rebuild_all.py — Full pipeline: schema -> load -> build scored tables.

Usage:
    python scripts/rebuild_all.py              # full rebuild
    python scripts/rebuild_all.py --skip-load  # rebuild scored tables only (data already loaded)
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Full rebuild pipeline")
    parser.add_argument("--skip-load", action="store_true", help="Skip loading, rebuild scored tables only")
    args = parser.parse_args()

    t0 = time.time()

    # Step 1: Schema + indexes
    print("=" * 60)
    print("Step 1: Initializing schema + indexes")
    print("=" * 60)
    from schema import init_db
    init_db()

    # Step 2: Load raw files -> derived tables
    if not args.skip_load:
        print("\n" + "=" * 60)
        print("Step 2: Loading raw files into Supabase")
        print("=" * 60)
        from schema import get_conn
        from db.load import load_all
        conn = get_conn()
        try:
            load_all(conn)
        finally:
            conn.close()
    else:
        print("\n[Skipping load — --skip-load]")

    # Step 3: Build ASIN-keyword scoring matrix
    print("\n" + "=" * 60)
    print("Step 3: Building ASIN-keyword scores")
    print("=" * 60)
    from db.build_asin_keywords import main as build_asin_keywords
    build_asin_keywords()

    # Step 4: Build keyword-level aggregation
    print("\n" + "=" * 60)
    print("Step 4: Building keyword targets")
    print("=" * 60)
    from db.build_keywords import main as build_keywords
    build_keywords()

    elapsed = time.time() - t0
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    print(f"\nFull rebuild complete in {mins}m {secs}s")


if __name__ == "__main__":
    main()
