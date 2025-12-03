from __future__ import annotations

import argparse
from pathlib import Path

from loguru import logger

from .config import load_config
from .pipeline import GeoMatchingPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="geo-matcher",
        description="高德POI + 快递地址批量匹配工具",
    )
    parser.add_argument(
        "config",
        type=Path,
        help="配置文件路径",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config_path: Path = args.config
    pipe_config = load_config(config_path)
    pipeline = GeoMatchingPipeline(pipe_config)
    logger.info("开始加载POI与快递地址数据")
    pipeline.load()
    logger.info("开始匹配")
    results = pipeline.match_all()
    pipeline.export(results)
    logger.info("全部完成，匹配结果数 {}", len(results))


if __name__ == "__main__":
    main()
