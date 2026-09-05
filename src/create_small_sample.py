from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.settings import HUGE_INPUT_FILE, SAMPLE_ROWS, SMALL_SAMPLE_FILE


def create_sample(
    input_file: Path,
    output_file: Path,
    rows: int = SAMPLE_ROWS,
) -> None:
    """
    Reads the original dirty CSV line-by-line using streaming and writes a reproducible
    small sample dataset without loading the full CSV into memory.
    Preserves original CSV header.
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found:\n{input_file}")

    if rows <= 0:
        raise ValueError("rows count must be greater than 0")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"Creating small sample from {input_file.name}...")
    print(f"Target row count: {rows:,}")

    with input_file.open("r", encoding="utf-8-sig", newline="") as infile:
        with output_file.open("w", encoding="utf-8", newline="") as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            try:
                header = next(reader)
            except StopIteration:
                raise ValueError("CSV file is empty")

            writer.writerow(header)
            count = 0

            for row in reader:
                writer.writerow(row)
                count += 1
                if count >= rows:
                    break

    size_mb = output_file.stat().st_size / (1024 * 1024)
    print("=" * 60)
    print("SMALL SAMPLE CREATION COMPLETED")
    print("=" * 60)
    print(f"Input file : {input_file}")
    print(f"Output file: {output_file}")
    print(f"Rows copied: {count:,}")
    print(f"File size  : {size_mb:.2f} MB")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Create a small sample CSV from a huge dataset programmatically."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(HUGE_INPUT_FILE),
        help="Path to input CSV file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(SMALL_SAMPLE_FILE),
        help="Path to output sample CSV file",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=SAMPLE_ROWS,
        help="Number of rows to extract",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    create_sample(
        input_file=input_path,
        output_file=output_path,
        rows=args.rows,
    )


if __name__ == "__main__":
    main()