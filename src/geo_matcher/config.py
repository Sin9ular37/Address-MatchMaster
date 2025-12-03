from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, validator


class ColumnMapping(BaseModel):
    poi: dict[str, str]
    address: dict[str, str]


class RetrieverConfig(BaseModel):
    max_candidates: int = 10
    enable_vector: bool = False
    vector_model: str = "shibing624/text2vec-base-chinese"


class ScoringConfig(BaseModel):
    coverage_weight: float = 0.5
    edit_distance_weight: float = 0.3
    doorplate_weight: float = 0.2
    min_score: float = 0.75


class GaodeConfig(BaseModel):
    enable: bool = False
    key: str = ""
    rate_limit_per_sec: int = 5


class RuntimeConfig(BaseModel):
    workers: int = 4
    chunk_size: int = 2000
    log_level: str = "INFO"


class PipelineConfig(BaseModel):
    poi_file: Path
    poi_sheet: Optional[str] = None
    address_file: Path
    address_sheet: Optional[str] = None
    output_file: Path = Path("output/matched_addresses.xlsx")

    columns: ColumnMapping
    retriever: RetrieverConfig = Field(default_factory=RetrieverConfig)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    gaode: GaodeConfig = Field(default_factory=GaodeConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)

    @validator("output_file", pre=True)
    def ensure_output_parent(cls, value: Path | str) -> Path:
        path = Path(value)
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        return path


def load_config(path: str | Path) -> PipelineConfig:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return PipelineConfig(**data)
