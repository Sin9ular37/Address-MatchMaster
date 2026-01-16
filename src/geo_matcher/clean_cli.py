from __future__ import annotations

import argparse
from pathlib import Path

from loguru import logger

from .address_cleaner import AddressCleaner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="geo-clean",
        description="批量对Excel中的地址列进行拆分清洗",
    )
    parser.add_argument("-i", "--input", type=Path, required=True, help="输入Excel路径")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="输出Excel路径，默认写入 <输入文件名>_clean.xlsx",
    )
    parser.add_argument(
        "-c",
        "--column",
        default="地址",
        help="需要清洗的列名，默认“地址”",
    )
    parser.add_argument(
        "--no-lac",
        action="store_true",
        help="禁用 LAC 分词/NER（默认开启，可提升拆分准确率）",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    input_path: Path = args.input
    if not input_path.exists():
        raise FileNotFoundError(f"找不到输入文件 {input_path}")
    output_path: Path
    if args.output:
        output_path = args.output
    else:
        output_path = input_path.with_name(f"{input_path.stem}_clean.xlsx")
    enable_lac = not args.no_lac
    cleaner = AddressCleaner(enable_lac=enable_lac)
    logger.info("读取 {}", input_path)
    cleaner.process_file(input_path, output_path, args.column)
    logger.info("清洗完成，结果已写入 {}", output_path)


if __name__ == "__main__":
    main()
