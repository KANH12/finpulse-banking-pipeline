"""
simulate_stream.py — Local continuous streaming simulation for FinPulse.

Usage:
    python simulate_stream.py
    python simulate_stream.py --interval-seconds 10 --batch-size 20 --max-batches 5

Stop: Ctrl+C
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.producer.handler  import handler as lambda1
from src.pipeline.handler  import handler as lambda2
from src.analytics.handler import handler as lambda3
from src.config.logger     import get_logger

log = get_logger("finpulse.simulate")

# ── Console formatting ────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
CYAN   = "\033[36m"
WHITE  = "\033[97m"

def _line(char: str = "─", width: int = 68) -> str:
    return char * width

def _header(batch: int, ts: str) -> None:
    print(f"\n{BOLD}{_line()}{RESET}")
    print(f"{BOLD}  BATCH #{batch:04d}   {DIM}{ts}{RESET}")
    print(f"{BOLD}{_line()}{RESET}")

def _step(icon: str, label: str, detail: str = "") -> None:
    detail_str = f"  {DIM}{detail}{RESET}" if detail else ""
    print(f"  {icon}  {label}{detail_str}")

def _ok(msg: str)   -> None: print(f"  {GREEN}✓{RESET}  {msg}")
def _warn(msg: str) -> None: print(f"  {YELLOW}!{RESET}  {msg}")
def _err(msg: str)  -> None: print(f"  {RED}✗{RESET}  {msg}")

def _summary(b1: dict, b2: dict, b3: dict, elapsed: float) -> None:
    t1p = b2.get("t1_passed", 0)
    t1f = b2.get("t1_failed", 0)
    t2p = b2.get("t2_passed", 0)
    t2f = b2.get("t2_failed", 0)
    gold = b3.get("gold_rows", 0)
    total_proc = b1.get("total_processed", 0)

    print(f"\n  {DIM}{_line('·', 60)}{RESET}")
    print(f"  {'Ingested':28s} {b1.get('rows_ingested', 0):>6,} rows")
    print(f"  {'Tier 1 — passed':28s} {t1p:>6,}  │  failed: {t1f:,}")
    print(f"  {'Tier 2 — passed':28s} {t2p:>6,}  │  failed: {t2f:,}")
    print(f"  {'Gold rows written':28s} {gold:>6,}")
    print(f"  {'Total processed (lifetime)':28s} {total_proc:>6,}")
    print(f"  {'Batch time':28s} {elapsed:>5.1f}s")
    print(f"  {DIM}{_line('·', 60)}{RESET}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(interval: int, batch_size: int, max_batches: int | None, run_gold_every: int) -> None:
    print(f"\n{BOLD}FinPulse — Streaming Simulation{RESET}")
    print(f"{DIM}interval={interval}s  batch_size={batch_size}  "
          f"gold_every={run_gold_every} batches{RESET}")
    print(f"{DIM}Press Ctrl+C to stop gracefully.{RESET}\n")

    batch_num   = 0
    gold_ticker = 0

    while True:
        if max_batches and batch_num >= max_batches:
            print(f"\n{GREEN}Reached max-batches ({max_batches}). Stopping.{RESET}\n")
            break

        batch_num   += 1
        gold_ticker += 1
        t0 = time.time()

        _header(batch_num, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

        # ── Lambda 1: Ingestion ───────────────────────────────────────────
        _step("→", "Lambda 1  Ingestion + Detection")
        try:
            r1 = lambda1({"batch_size_override": batch_size}, context=None)
        except Exception as exc:
            _err(f"Lambda 1 failed: {exc}")
            break

        if r1["status"] == "EXHAUSTED":
            print(f"\n{GREEN}{BOLD}Dataset fully processed. Pipeline stopped gracefully.{RESET}\n")
            break

        _ok(f"Ingested {r1['rows_ingested']:,} rows  →  bronze/{r1['bronze_key'].split('/')[-1]}")

        # ── Lambda 2: DQ Pipeline ─────────────────────────────────────────
        _step("→", "Lambda 2  Tier 1 + Tier 2 DQ Gates")
        try:
            r2 = lambda2({"bronze_key": r1["bronze_key"]}, context=None)
        except Exception as exc:
            _err(f"Lambda 2 failed: {exc}")
            break

        t1f = r2.get("t1_failed", 0)
        t2f = r2.get("t2_failed", 0)
        t2p = r2.get("t2_passed", 0)

        if t1f:
            _warn(f"Tier 1: {t1f} records → quarantine/tier1/")
        if t2f:
            _warn(f"Tier 2: {t2f} records → quarantine/tier2/")
        if t2p:
            _ok(f"{t2p} records → silver/")

        # ── Lambda 3: Gold Aggregation (every N batches) ──────────────────
        if gold_ticker >= run_gold_every:
            gold_ticker = 0
            _step("→", "Lambda 3  Gold Aggregation")
            try:
                r3 = lambda3({}, context=None)
                _ok(f"{r3.get('gold_rows', 0):,} rows written → gold/ (partitioned by month)")
            except Exception as exc:
                _err(f"Lambda 3 failed: {exc}")
                r3 = {"gold_rows": 0}
        else:
            r3 = {"gold_rows": 0}
            _step("·", f"Lambda 3  skipped (runs every {run_gold_every} batches)")

        elapsed = time.time() - t0
        _summary(r1, r2, r3, elapsed)

        if batch_num == 1 or (batch_num % 10 == 0):
            log.info(
                "Simulation checkpoint",
                extra={
                    "batch": batch_num,
                    "total_processed": r1.get("total_processed"),
                    "t2_passed": r2.get("t2_passed"),
                },
            )

        print(f"\n  {DIM}Next batch in {interval}s — Ctrl+C to stop{RESET}")
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print(f"\n\n{YELLOW}Interrupted by user. Stopping cleanly.{RESET}\n")
            sys.exit(0)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="FinPulse local streaming simulation",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--interval-seconds", type=int,   default=5,  metavar="N",
                   help="Seconds to wait between batches")
    p.add_argument("--batch-size",       type=int,   default=20, metavar="N",
                   help="Rows per micro-batch")
    p.add_argument("--max-batches",      type=int,   default=None, metavar="N",
                   help="Stop after N batches (omit for infinite)")
    p.add_argument("--gold-every",       type=int,   default=3,  metavar="N",
                   help="Run Lambda 3 Gold aggregation every N batches")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        run(
            interval     = args.interval_seconds,
            batch_size   = args.batch_size,
            max_batches  = args.max_batches,
            run_gold_every = args.gold_every,
        )
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Stopped.{RESET}\n")
        sys.exit(0)
