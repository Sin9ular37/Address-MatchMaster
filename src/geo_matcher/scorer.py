from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from rapidfuzz.distance import Levenshtein

from .models import AddressRecord, MatchResult, POIRecord
from .normalizer import NormalizedAddress


@dataclass
class ScoringWeights:
    coverage_weight: float
    edit_distance_weight: float
    doorplate_weight: float


class CandidateScorer:
    def __init__(self, weights: ScoringWeights) -> None:
        self.weights = weights

    def score_candidates(
        self,
        address: AddressRecord,
        normalized: NormalizedAddress,
        candidates: Iterable[POIRecord],
        min_score: float,
    ) -> MatchResult:
        best_score = 0.0
        best_poi: POIRecord | None = None
        candidate_cache: List[dict] = []
        for poi in candidates:
            coverage = self._token_coverage(normalized.tokens, (poi.normalized or poi.name).split())
            edit_distance = 1 - self._edit_ratio(normalized.text, poi.normalized or poi.name)
            doorplate_score = self._doorplate_score(normalized.house_number, poi.house_number)

            total = (
                coverage * self.weights.coverage_weight
                + edit_distance * self.weights.edit_distance_weight
                + doorplate_score * self.weights.doorplate_weight
            )
            candidate_cache.append(
                {
                    "poi_id": poi.poi_id,
                    "poi_name": poi.name,
                    "score": total,
                    "coverage": coverage,
                    "edit_distance": edit_distance,
                    "doorplate": doorplate_score,
                }
            )
            if total > best_score:
                best_score = total
                best_poi = poi

        if best_score >= min_score and best_poi:
            return MatchResult(
                order_id=address.order_id,
                raw_address=address.raw_address,
                matched_poi_id=best_poi.poi_id,
                matched_poi_name=best_poi.name,
                latitude=best_poi.latitude,
                longitude=best_poi.longitude,
                score=best_score,
                source="RULE",
                candidates=candidate_cache,
            )
        return MatchResult(order_id=address.order_id, raw_address=address.raw_address, score=best_score, candidates=candidate_cache)

    def _token_coverage(self, addr_tokens: List[str], poi_tokens: List[str]) -> float:
        if not addr_tokens:
            return 0.0
        hits = sum(1 for token in addr_tokens if token in poi_tokens)
        return hits / len(addr_tokens)

    def _edit_ratio(self, a: str, b: str) -> float:
        if not a or not b:
            return 1.0
        max_len = max(len(a), len(b))
        if max_len == 0:
            return 1.0
        distance = Levenshtein.distance(a, b)
        return distance / max_len

    def _doorplate_score(self, addr_house: str | None, poi_house: str | None) -> float:
        if not addr_house or not poi_house:
            return 0.0
        return 1.0 if addr_house == poi_house else 0.2
