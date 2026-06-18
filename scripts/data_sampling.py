#!/usr/bin/env python3

"""Create representative Criteo sample datasets for development and validation."""

from __future__ import annotations

import csv
import hashlib
import heapq
from pathlib import Path
from typing import Dict, List, Tuple


SAMPLE_SIZES = {
    "criteo_100k.csv": 100_000,
    "criteo_1m.csv": 1_000_000,
    "criteo_5m.csv": 5_000_000,
}

SEED = "ctr-analytics-warehouse-v1"

COLUMN_NAMES = [
    "label",
    *[f"I{i}" for i in range(1, 14)],
    *[f"C{i}" for i in range(1, 27)],
]


def stable_hash(*parts: object) -> int:
    digest = hashlib.blake2b(
        "|".join(str(part) for part in parts).encode("utf-8"),
        digest_size=8,
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


def count_labels(raw_file: Path) -> Dict[int, int]:
    counts = {0: 0, 1: 0}
    with raw_file.open("r", newline="") as source:
        reader = csv.reader(source, delimiter="\t")
        for row in reader:
            label = int(row[0])
            counts[label] += 1
    return counts


def target_counts(label_counts: Dict[int, int], sample_size: int) -> Dict[int, int]:
    total_rows = sum(label_counts.values())
    target_one = round(sample_size * label_counts[1] / total_rows)
    target_one = min(target_one, label_counts[1], sample_size)
    target_zero = sample_size - target_one
    target_zero = min(target_zero, label_counts[0])
    target_one = sample_size - target_zero
    return {0: target_zero, 1: target_one}


def build_label_reservoirs(
    raw_file: Path,
    sample_targets: Dict[str, Dict[int, int]],
) -> Dict[int, List[Tuple[int, int]]]:
    max_targets = {
        label: max(targets[label] for targets in sample_targets.values())
        for label in (0, 1)
    }
    reservoirs: Dict[int, List[Tuple[int, int]]] = {0: [], 1: []}

    with raw_file.open("r", newline="") as source:
        reader = csv.reader(source, delimiter="\t")
        for row_number, row in enumerate(reader, start=1):
            label = int(row[0])
            key = stable_hash(SEED, "reservoir", row_number)
            reservoir = reservoirs[label]
            target_size = max_targets[label]

            if len(reservoir) < target_size:
                heapq.heappush(reservoir, (-key, row_number))
                continue

            if key < -reservoir[0][0]:
                heapq.heapreplace(reservoir, (-key, row_number))

    return reservoirs


def choose_nested_samples(
    reservoirs: Dict[int, List[Tuple[int, int]]],
    sample_targets: Dict[str, Dict[int, int]],
) -> Dict[str, List[int]]:
    label_row_numbers: Dict[int, List[int]] = {}
    for label, heap in reservoirs.items():
        selected = [row_number for _, row_number in heap]
        selected.sort(key=lambda row_number: stable_hash(SEED, "nested", label, row_number))
        label_row_numbers[label] = selected

    chosen_rows: Dict[str, List[int]] = {}
    for file_name, targets in sample_targets.items():
        selected_rows: List[int] = []
        for label in (0, 1):
            target_size = targets[label]
            selected_rows.extend(label_row_numbers[label][:target_size])
        selected_rows.sort()
        chosen_rows[file_name] = selected_rows

    return chosen_rows


def write_sample_files(
    raw_file: Path,
    sample_dir: Path,
    selected_rows: Dict[str, List[int]],
) -> None:
    files_in_order = sorted(selected_rows.items(), key=lambda item: len(item[1]))
    output_handles = {}
    output_writers = {}
    positions = {}

    try:
        for file_name, row_numbers in files_in_order:
            output_path = sample_dir / file_name
            handle = output_path.open("w", newline="")
            writer = csv.writer(handle)
            writer.writerow(COLUMN_NAMES)
            output_handles[file_name] = handle
            output_writers[file_name] = writer
            positions[file_name] = 0

        with raw_file.open("r", newline="") as source:
            reader = csv.reader(source, delimiter="\t")
            for row_number, row in enumerate(reader, start=1):
                for file_name, row_numbers in files_in_order:
                    position = positions[file_name]
                    if position >= len(row_numbers):
                        continue

                    target_row = row_numbers[position]
                    if row_number == target_row:
                        output_writers[file_name].writerow(row)
                        positions[file_name] += 1

    finally:
        for handle in output_handles.values():
            handle.close()


def print_summary(
    label_counts: Dict[int, int],
    sample_targets: Dict[str, Dict[int, int]],
) -> None:
    total_rows = sum(label_counts.values())
    click_rate = label_counts[1] / total_rows
    print(f"Source rows: {total_rows:,}")
    print(f"Clicks: {label_counts[1]:,}")
    print(f"Non-clicks: {label_counts[0]:,}")
    print(f"Source CTR: {click_rate:.6f}")
    print()

    for file_name, targets in sample_targets.items():
        sample_size = sum(targets.values())
        sample_ctr = targets[1] / sample_size
        print(f"{file_name}:")
        print(f"  rows={sample_size:,}")
        print(f"  label_1={targets[1]:,}")
        print(f"  label_0={targets[0]:,}")
        print(f"  target_ctr={sample_ctr:.6f}")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    raw_file = project_root / "data" / "raw" / "dac" / "train.txt"
    sample_dir = project_root / "data" / "sample"
    sample_dir.mkdir(parents=True, exist_ok=True)

    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file}")

    label_counts = count_labels(raw_file)
    sample_targets = {
        file_name: target_counts(label_counts, sample_size)
        for file_name, sample_size in SAMPLE_SIZES.items()
    }

    print_summary(label_counts, sample_targets)
    print("\nBuilding stratified sample reservoirs...")
    reservoirs = build_label_reservoirs(raw_file, sample_targets)

    print("Selecting nested sample row sets...")
    selected_rows = choose_nested_samples(reservoirs, sample_targets)

    print("Writing sampled CSV files...")
    write_sample_files(raw_file, sample_dir, selected_rows)

    print("\nCompleted sample files:")
    for file_name in SAMPLE_SIZES:
        print(sample_dir / file_name)


if __name__ == "__main__":
    main()
