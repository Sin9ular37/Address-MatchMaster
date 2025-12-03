from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, List, Sequence

try:
    from sentence_transformers import SentenceTransformer
except ImportError:  # pragma: no cover - optional依赖
    SentenceTransformer = None  # type: ignore

from .indexers import InvertedIndex
from .models import POIRecord


class CandidateRetriever(ABC):
    @abstractmethod
    def query(self, normalized_text: str, tokens: Iterable[str], limit: int) -> List[POIRecord]:
        ...


class InvertedRetriever(CandidateRetriever):
    def __init__(self, index: InvertedIndex):
        self.index = index

    def query(self, normalized_text: str, tokens: Iterable[str], limit: int) -> List[POIRecord]:
        return self.index.query(tokens, limit=limit)


class VectorRetriever(CandidateRetriever):
    def __init__(self, pois: Sequence[POIRecord], model_name: str):
        if SentenceTransformer is None:
            raise RuntimeError("未安装 sentence-transformers，请使用 `pip install .[vector]`")
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)
        self.poi_list = list(pois)
        self.poi_embeddings = self.model.encode([poi.normalized or poi.name for poi in self.poi_list], show_progress_bar=False)

    def query(self, normalized_text: str, tokens: Iterable[str], limit: int) -> List[POIRecord]:
        query_emb = self.model.encode([normalized_text], show_progress_bar=False)
        scores = (query_emb @ self.poi_embeddings.T)[0]
        ranked_idx = scores.argsort()[::-1][:limit]
        return [self.poi_list[i] for i in ranked_idx]
