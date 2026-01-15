import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.build import build_dataset


def parse_args():
    parser = argparse.ArgumentParser(description="Build profitability datasets")
    parser.add_argument("--input", required=True, help="Path to Excel input file")
    parser.add_argument("--fy", default=None, help="Filter by financial year label (e.g., FY26)")
    parser.add_argument("--output", default="data/processed", help="Output directory")
    return parser.parse_args()


def main():
    args = parse_args()
    build_dataset(args.input, output_dir=args.output, fy=args.fy)


if __name__ == "__main__":
    main()
