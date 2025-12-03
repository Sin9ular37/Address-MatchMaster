from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from typing import Iterable, List, Sequence

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
        logger.info("加载POI {count} 条, 地址 {addr_count} 条", count=len(self.pois), addr_count=len(self.addresses))
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
        df = pd.DataFrame([result.model_dump() for result in results])
        df.to_excel(self.config.output_file, index=False)
        logger.info("结果写入 {path}", path=self.config.output_file)

    def _load_pois(self) -> list[POIRecord]:
        df = pd.read_excel(self.config.poi_file, sheet_name=self.config.poi_sheet)
        mapper = self.config.columns.poi
        df = df.rename(columns={v: k for k, v in mapper.items() if v in df.columns})
        pois = []
        for row in df.to_dict(orient="records"):
            poi = POIRecord(
                poi_id=str(row.get("poi_id")),
                name=str(row.get("name")),
                province=row.get("province", "") or "",
                city=row.get("city", "") or "",
                district=row.get("district", "") or "",
                street=row.get("street", "") or "",
                house_number=str(row.get("house_number") or ""),
                latitude=float(row.get("latitude")),
                longitude=float(row.get("longitude")),
                poi_type=row.get("poi_type"),
            )
            normalized = self.normalizer.normalize("".join([poi.province, poi.city, poi.district, poi.street, poi.name, poi.house_number]))
            poi.normalized = " ".join(normalized.tokens)
            pois.append(poi)
        return pois

    def _load_addresses(self) -> list[AddressRecord]:
        df = pd.read_excel(self.config.address_file, sheet_name=self.config.address_sheet)
        mapper = self.config.columns.address
        df = df.rename(columns={v: k for k, v in mapper.items() if v in df.columns})
        addresses = []
        for row in df.to_dict(orient="records"):
            addr = AddressRecord(
                order_id=str(row.get("order_id")),
                raw_address=str(row.get("raw_address")),
                province=row.get("province", "") or "",
                city=row.get("city", "") or "",
                district=row.get("district", "") or "",
                street=row.get("street", "") or "",
                house_number=str(row.get("house_number") or ""),
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
