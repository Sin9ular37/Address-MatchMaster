from __future__ import annotations

import argparse
from pathlib import Path

from loguru import logger

from .validator import run_validation, ValidationError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="geo-validate",
        description="检查 geo-matcher 配置文件与向量模型（可选）",
    )
    parser.add_argument("config", type=Path, help="配置文件路径")
    parser.add_argument(
        "--no-vector",
        action="store_true",
        help="跳过向量模型检查（默认会检查）",
    )
    parser.add_argument(
        "--sample",
        type=str,
        help="验证向量模型时的示例文本，默认使用固定地址样例",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        run_validation(args.config, check_vector=not args.no_vector, sample_text=args.sample)
    except ValidationError as exc:
        logger.error(str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
