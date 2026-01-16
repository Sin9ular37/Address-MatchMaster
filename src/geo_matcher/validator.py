from __future__ import annotations

from pathlib import Path

from loguru import logger

from .config import PipelineConfig, load_config


class ValidationError(Exception):
    ...


def validate_paths(config: PipelineConfig) -> None:
    if not config.poi_file.exists():
        raise ValidationError(f"POI 文件不存在：{config.poi_file}")
    if not config.address_file.exists():
        raise ValidationError(f"地址文件不存在：{config.address_file}")
    logger.info("路径检查通过：POI={poi}, 地址={addr}", poi=config.poi_file, addr=config.address_file)


def validate_vector_model(config: PipelineConfig, sample_text: str | None = None) -> None:
    if not config.retriever.enable_vector:
        logger.warning("配置中未启用向量召回（retriever.enable_vector=false），跳过向量模型检查")
        return
    try:
        from sentence_transformers import SentenceTransformer
    except Exception as exc:  # pragma: no cover - 依赖缺失时抛出
        raise ValidationError("未安装 sentence-transformers，请执行 `pip install .[vector]` 再试") from exc
    model_name = config.retriever.vector_model
    logger.info("开始加载向量模型 {model}", model=model_name)
    model = SentenceTransformer(model_name)
    sample = sample_text or "北京市海淀区中关村大街1号"
    _ = model.encode([sample], show_progress_bar=False)
    logger.info("向量模型加载并编码成功，示例输入：{text}", text=sample)


def run_validation(config_path: Path, check_vector: bool = True, sample_text: str | None = None) -> None:
    cfg = load_config(config_path)
    validate_paths(cfg)
    if check_vector:
        validate_vector_model(cfg, sample_text=sample_text)
    logger.info("配置与模型校验完成，一切正常")
