from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Sequence

from .models import POIRecord


class InvertedIndex:
    def __init__(self) -> None:
        self.term_to_ids: Dict[str, set[str]] = defaultdict(set)
        self.id_to_poi: Dict[str, POIRecord] = {}

    def build(self, pois: Sequence[POIRecord]) -> None:
        for poi in pois:
            self.id_to_poi[poi.poi_id] = poi
            tokens = (poi.normalized or poi.name).split()
            for token in tokens:
                if token:
                    self.term_to_ids[token].add(poi.poi_id)

    def query(self, tokens: Iterable[str], limit: int = 20) -> List[POIRecord]:
        score_map: Dict[str, int] = defaultdict(int)
        for token in tokens:
            ids = self.term_to_ids.get(token)
            if not ids:
                continue
            for poi_id in ids:
                score_map[poi_id] += 1
        ranked = sorted(score_map.items(), key=lambda x: x[1], reverse=True)[:limit]
        return [self.id_to_poi[poi_id] for poi_id, _ in ranked]
