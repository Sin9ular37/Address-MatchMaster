from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import jieba
from pypinyin import Style, lazy_pinyin

COMMON_REPLACEMENTS = {
    " ": "",
    "\t": "",
    "－": "-",
    "—": "-",
    "～": "-",
    "～": "-",
    "号楼": "号",
    "栋": "号",
    "弄": "弄",
    "幢": "号",
    "－": "-",
}

ROAD_SUFFIX = ["路", "街", "大道", "巷", "胡同", "横", "纵", "弄"]
HOUSE_PATTERN = re.compile(r"(\d+)(号|弄|栋|幢|室|单元)?")


@dataclass
class NormalizedAddress:
    text: str
    tokens: list[str]
    house_number: str | None = None
    phonetic: str | None = None


class AddressNormalizer:
    def __init__(self) -> None:
        jieba.initialize()

    def normalize(self, text: str) -> NormalizedAddress:
        cleaned = self._basic_clean(text)
        tokens = list(jieba.cut(cleaned))
        house = self._extract_house(tokens)
        phonetic = "".join(lazy_pinyin(cleaned, style=Style.NORMAL))
        return NormalizedAddress(text=cleaned, tokens=tokens, house_number=house, phonetic=phonetic)

    def _basic_clean(self, text: str) -> str:
        result = text.strip()
        for old, new in COMMON_REPLACEMENTS.items():
            result = result.replace(old, new)
        result = re.sub(r"[()（）【】]", "", result)
        result = re.sub(r"\s+", "", result)
        return result

    def _extract_house(self, tokens: Iterable[str]) -> str | None:
        joined = "".join(tokens)
        match = HOUSE_PATTERN.search(joined)
        if match:
            return match.group(1)
        return None
