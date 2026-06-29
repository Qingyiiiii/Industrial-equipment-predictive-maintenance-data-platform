# -*- coding: utf-8 -*-
"""Build a compact MetroPT CSV sample for local demos and contract tests."""
import argparse
import csv
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "datas" / "MetroPT3_AirCompressor.csv"
DEFAULT_OUTPUT_ROOT = ROOT / "data" / "metropt_quality" / "samples"


def build_head_sample(source: Path, output: Path, rows: int) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    with source.open("r", encoding="utf-8", newline="") as src, output.open("w", encoding="utf-8", newline="") as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)
        try:
            header = next(reader)
        except StopIteration:
            raise RuntimeError(f"source CSV is empty: {source}")
        writer.writerow(header)
        written = 0
        for row in reader:
            if written >= rows:
                break
            writer.writerow(row)
            written += 1
    return written


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    if args.rows <= 0:
        parser.error("--rows must be > 0")
    source = Path(args.source)
    if not source.exists():
        raise FileNotFoundError(f"source CSV missing: {source}")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = Path(args.output) if args.output else DEFAULT_OUTPUT_ROOT / f"metropt_sample_head_{args.rows}_{run_id}.csv"
    written = build_head_sample(source, output, args.rows)
    print("MetroPT sample built")
    print("source:", source)
    print("output:", output)
    print("rows:", written)


if __name__ == "__main__":
    main()

