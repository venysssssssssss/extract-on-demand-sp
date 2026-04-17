from __future__ import annotations

import argparse
import json
from pathlib import Path

from .medidor import compact_medidor_raw_exports


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compact MEDIDOR EL31/IQ09 raw TXT exports into one deduplicated CSV.",
    )
    parser.add_argument(
        "--raw-dir",
        required=True,
        help="Directory containing el31_medidor_*.txt and iq09_medidor_*.txt files.",
    )
    parser.add_argument(
        "--group-map-path",
        default="gruporegsap.xlsx",
        help="XLSX with Grp.registrad. -> Tipo mapping. Defaults to gruporegsap.xlsx.",
    )
    parser.add_argument(
        "--output-csv-path",
        default="",
        help="Destination CSV path. Defaults to <raw-dir>/../normalized/medidor_raw_compactado.csv.",
    )
    parser.add_argument(
        "--manifest-path",
        default="",
        help="Destination manifest JSON path. Defaults to <output-csv-path>.manifest.json.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = compact_medidor_raw_exports(
        raw_dir=Path(args.raw_dir),
        group_map_path=Path(args.group_map_path),
        output_csv_path=Path(args.output_csv_path) if str(args.output_csv_path).strip() else None,
        manifest_path=Path(args.manifest_path) if str(args.manifest_path).strip() else None,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
