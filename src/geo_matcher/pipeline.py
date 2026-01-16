from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from typing import Mapping, Optional, Sequence

import pandas as pd
from loguru import logger

from .config import PipelineConfig
from .indexers import InvertedIndex
from .models import AddressRecord, MatchResult, POIRecord
from .normalizer import AddressNormalizer
from .retrievers import CandidateRetriever, InvertedRetriever, VectorRetriever
from .scorer import CandidateScorer, ScoringWeights


class GeoMatchingPipeline:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.normalizer = AddressNormalizer()
        self.scorer = CandidateScorer(
            ScoringWeights(
                coverage_weight=config.scoring.coverage_weight,
                edit_distance_weight=config.scoring.edit_distance_weight,
                doorplate_weight=config.scoring.doorplate_weight,
            )
        )
        self.pois: list[POIRecord] = []
        self.addresses: list[AddressRecord] = []
        self.retrievers: list[CandidateRetriever] = []

    def load(self) -> None:
        self.pois = self._load_pois()
        self.addresses = self._load_addresses()
        logger.info("加载POI {count} 条，地址 {addr_count} 条", count=len(self.pois), addr_count=len(self.addresses))
        index = InvertedIndex()
        index.build(self.pois)
        self.retrievers = [InvertedRetriever(index)]
        if self.config.retriever.enable_vector:
            self.retrievers.append(VectorRetriever(self.pois, self.config.retriever.vector_model))

    def match_all(self) -> list[MatchResult]:
        if not self.retrievers:
            raise RuntimeError("请先调用 load() 构建索引")
        worker = partial(self._match_single)
        results: list[MatchResult] = []
        if self.config.runtime.workers <= 1:
            for address in self.addresses:
                results.append(worker(address))
            return results
        with ProcessPoolExecutor(max_workers=self.config.runtime.workers) as executor:
            future_map = {executor.submit(worker, address): address.order_id for address in self.addresses}
            for future in as_completed(future_map):
                results.append(future.result())
        return results

    def export(self, results: Sequence[MatchResult]) -> None:
        merged_rows: list[dict] = []
        for result in results:
            payload = result.model_dump()
            payload.pop("order_id", None)  # 输入无运单号时，不在导出结果中强加行号
            original = payload.pop("original_row", {})
            combined = dict(payload)
            for key, value in original.items():
                if key in combined:
                    combined[f"original_{key}"] = value
                else:
                    combined[key] = value
            merged_rows.append(combined)
        df = pd.DataFrame(merged_rows)
        df.to_excel(self.config.output_file, index=False)
        logger.info("结果写入 {path}", path=self.config.output_file)

    def _load_pois(self) -> list[POIRecord]:
        raw_df = self._read_table(self.config.poi_file, sheet_name=self.config.poi_sheet)
        mapper = self.config.columns.poi
        df = self._select_columns(raw_df, mapper)
        pois: list[POIRecord] = []
        for row in df.to_dict(orient="records"):
            poi = POIRecord(
                poi_id=str(row.get("poi_id") or ""),
                name=str(row.get("name") or ""),
                province=row.get("province", "") or "",
                city=row.get("city", "") or "",
                district=row.get("district", "") or "",
                street=row.get("street", "") or "",
                house_number=str(row.get("house_number") or ""),
                latitude=float(row.get("latitude")),
                longitude=float(row.get("longitude")),
                poi_type=row.get("poi_type"),
                address_raw=str(row.get("street") or ""),
            )
            normalized = self.normalizer.normalize(
                "".join([poi.province, poi.city, poi.district, poi.street, poi.name, poi.house_number])
            )
            poi.normalized = " ".join(normalized.tokens)
            pois.append(poi)
        return pois

    def _load_addresses(self) -> list[AddressRecord]:
        raw_df = self._read_table(self.config.address_file, sheet_name=self.config.address_sheet)
        mapper = self.config.columns.address
        df = self._select_columns(raw_df, mapper)
        addresses: list[AddressRecord] = []
        raw_records = raw_df.to_dict(orient="records")
        std_records = df.to_dict(orient="records")
        for idx, (raw_row, row) in enumerate(zip(raw_records, std_records), start=1):
            addr = AddressRecord(
                order_id=self._ensure_order_id(row.get("order_id"), idx),
                raw_address=self._coerce_str(row.get("raw_address")),
                province=self._coerce_str(row.get("province")),
                city=self._coerce_str(row.get("city")),
                district=self._coerce_str(row.get("district")),
                street=self._coerce_str(row.get("street")),
                house_number=self._coerce_str(row.get("house_number")),
                original_row=raw_row,
            )
            addresses.append(addr)
        return addresses

    def _match_single(self, address: AddressRecord) -> MatchResult:
        normalized = self.normalizer.normalize(
            "".join([address.province, address.city, address.district, address.street, address.raw_address])
        )
        address.normalized = normalized.text

        candidate_lists: list[POIRecord] = []
        for retriever in self.retrievers:
            candidate_lists.extend(
                retriever.query(normalized.text, normalized.tokens, limit=self.config.retriever.max_candidates)
            )
        unique_candidates = {poi.poi_id: poi for poi in candidate_lists}.values()
        return self.scorer.score_candidates(
            address=address,
            normalized=normalized,
            candidates=unique_candidates,
            min_score=self.config.scoring.min_score,
        )

    def _read_table(self, path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
        ext = path.suffix.lower()
        if ext in {".csv", ".txt"}:
            return pd.read_csv(path, encoding="utf-8-sig")
        kwargs = {}
        if sheet_name is not None:
            kwargs["sheet_name"] = sheet_name
        return pd.read_excel(path, **kwargs)

    def _select_columns(self, df: pd.DataFrame, mapping: Mapping[str, str]) -> pd.DataFrame:
        data: dict[str, pd.Series] = {}
        for logical, source in mapping.items():
            if source in df.columns:
                data[logical] = df[source]
        if not data:
            return pd.DataFrame(index=df.index).copy()
        return pd.DataFrame(data)

    def _ensure_order_id(self, raw_value: object, index: int) -> str:
        if isinstance(raw_value, str):
            candidate = raw_value.strip()
            if candidate:
                return candidate
        elif raw_value is not None and not pd.isna(raw_value):
            return str(raw_value)
        return f"ROW_{index}"

    def _coerce_str(self, value: object, default: str = "") -> str:
        if isinstance(value, str):
            return value
        if value is None:
            return default
        try:
            import pandas as pd
            if pd.isna(value):
                return default
        except Exception:
            pass
        return str(value)
