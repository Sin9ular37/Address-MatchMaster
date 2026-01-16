from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

PROVINCES = [
    "北京市",
    "天津市",
    "上海市",
    "重庆市",
    "河北省",
    "山西省",
    "辽宁省",
    "吉林省",
    "黑龙江省",
    "江苏省",
    "浙江省",
    "安徽省",
    "福建省",
    "江西省",
    "山东省",
    "河南省",
    "湖北省",
    "湖南省",
    "广东省",
    "海南省",
    "四川省",
    "贵州省",
    "云南省",
    "陕西省",
    "甘肃省",
    "青海省",
    "台湾省",
    "内蒙古自治区",
    "广西壮族自治区",
    "西藏自治区",
    "宁夏回族自治区",
    "新疆维吾尔自治区",
    "香港特别行政区",
    "澳门特别行政区",
]

MUNICIPALITIES = {"北京市", "天津市", "上海市", "重庆市"}
CITY_SUFFIXES = ("市", "盟", "自治州", "地区", "林区")
DISTRICT_SUFFIXES = ("区", "县", "旗", "自治县", "新区", "林区")
TOWN_SUFFIXES = ("街道", "镇", "乡", "苏木", "办事处", "开发区", "农场", "社区")
REMARK_BRACKETS = re.compile(r"[（(【\[]([^）)】\]]+)[）)】\]]")
CONTACT_PATTERN = re.compile(r"(备注|电话|手机|联系方式|联系|请拨打)[:：]?\s*([\u4e00-\u9fa5A-Za-z0-9-]+.*)$")
ROAD_PATTERN = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9]{2,30}(?:路|街|道|巷|胡同|弄|大道|支路|环路|线))")
HOUSE_PATTERN = re.compile(r"(?P<value>[0-9一二三四五六七八九十百千]+)\s*(?P<suffix>号|号院|弄|支弄|支路)")
BUILDING_PATTERN = re.compile(r"(?P<value>[0-9一二三四五六七八九十百千]+)\s*(?P<suffix>栋|幢|号楼|楼|座|期)")
UNIT_PATTERN = re.compile(r"(?P<value>[0-9一二三四五六七八九十百千]+)\s*(?P<suffix>单元|门|梯)")
ROOM_PATTERN = re.compile(r"(?P<value>[0-9一二三四五六七八九十百千]+)\s*(?P<suffix>室|户|房|号|铺|店)")
NEIGHBORHOOD_PATTERN = re.compile(
    r"([\u4e00-\u9fa5A-Za-z0-9]{2,40}(?:小区|花园|家园|城|大厦|公寓|苑|府|庄|屯|村|雅苑|雅居|公馆|广场|天地|中心|府邸|别墅)(?:[一二三四五六七八九十0-9]+期)?)"
)
TRAILING_ROOM_PATTERN = re.compile(r"(?P<value>[0-9]{1,8})$")


@dataclass
class ParsedAddress:
    province: str = ""
    city: str = ""
    district: str = ""
    town: str = ""
    road: str = ""
    house_number: str = ""
    neighborhood: str = ""
    building: str = ""
    unit: str = ""
    room: str = ""
    remark: str = ""
    cleaned_text: str = ""

    def to_row(self) -> dict[str, str]:
        return {
            "省份": self.province,
            "地市": self.city,
            "区县": self.district,
            "街道/乡镇": self.town,
            "道路名称": self.road,
            "门牌号": self.house_number,
            "小区/屯名称": self.neighborhood,
            "楼栋": self.building,
            "单元": self.unit,
            "户室": self.room,
            "备注": self.remark,
        }


class LACAnalyzer:
    """轻量封装 LAC，避免核心逻辑散落。"""

    def __init__(self) -> None:
        try:
            from LAC import LAC as LACClient  # type: ignore
        except Exception as exc:  # pragma: no cover - 依赖缺失时抛出
            raise ImportError("请先安装 LAC：pip install LAC") from exc
        self.client = LACClient(mode="lac")

    def tag(self, text: str) -> list[tuple[str, str]]:
        result = self.client.run(text)
        tokens: list[str] = []
        tags: list[str] = []
        if (
            isinstance(result, (list, tuple))
            and len(result) == 2
            and isinstance(result[0], (list, tuple))
            and isinstance(result[1], (list, tuple))
        ):
            tokens, tags = list(result[0]), list(result[1])
        elif isinstance(result, list) and all(isinstance(item, str) for item in result):
            tokens = list(result)
            tags = [""] * len(tokens)
        return [(tok, tags[idx] if idx < len(tags) else "") for idx, tok in enumerate(tokens)]


class AddressParser:
    def __init__(self, enable_lac: bool = True, lac_analyzer: LACAnalyzer | None = None) -> None:
        self.lac = lac_analyzer if enable_lac else None
        if self.lac is None and enable_lac:
            try:
                self.lac = LACAnalyzer()
            except ImportError:
                # 若未安装 LAC，保留回退的纯规则模式
                self.lac = None

    def parse(self, text: str) -> ParsedAddress:
        if text is None:
            text = ""
        raw = str(text).strip()
        if not raw:
            return ParsedAddress()
        normalized = self._normalize(raw)
        normalized, remark_extra = self._extract_bracket_remark(normalized)
        normalized, contact_remark = self._extract_contact(normalized)
        remarks = [seg for seg in [remark_extra, contact_remark] if seg]
        lac_tags = self._run_lac(normalized)
        province, remaining = self._extract_province(normalized)
        city, remaining = self._extract_city(remaining, province)
        district, remaining = self._extract_by_suffix(remaining, DISTRICT_SUFFIXES)
        town, remaining = self._extract_by_suffix(remaining, TOWN_SUFFIXES)
        detail = remaining.strip()
        neighborhood, detail = self._extract_first(NEIGHBORHOOD_PATTERN, detail)
        road, detail = self._extract_first(ROAD_PATTERN, detail)
        house_number, detail = self._extract_named(HOUSE_PATTERN, detail)
        building, detail = self._extract_named(BUILDING_PATTERN, detail, keep_suffix=True)
        unit, detail = self._extract_named(UNIT_PATTERN, detail, keep_suffix=True)
        room, detail = self._extract_named(ROOM_PATTERN, detail)
        if not room:
            room, detail = self._extract_trailing_room(detail)
        detail = detail.strip(" ，,;；")
        if detail:
            remarks.append(detail)
        remark_text = "；".join(filter(None, remarks))
        cleaned = "".join(filter(None, [province, city, district, town, neighborhood, road, house_number, building, unit, room]))
        parsed = ParsedAddress(
            province=province,
            city=city,
            district=district,
            town=town,
            road=road,
            house_number=house_number,
            neighborhood=neighborhood,
            building=building,
            unit=unit,
            room=room,
            remark=remark_text,
            cleaned_text=cleaned or normalized,
        )
        self._apply_lac_assist(parsed, lac_tags)
        return parsed

    def _normalize(self, text: str) -> str:
        replaced = (
            text.replace("（", "(")
            .replace("）", ")")
            .replace("【", "[")
            .replace("】", "]")
            .replace("　", "")
            .replace("\n", "")
            .replace("\t", "")
        )
        replaced = re.sub(r"\s+", "", replaced)
        return replaced

    def _extract_bracket_remark(self, text: str) -> tuple[str, str]:
        remarks: list[str] = []

        def repl(match: re.Match[str]) -> str:
            remarks.append(match.group(1).strip())
            return ""

        cleaned = REMARK_BRACKETS.sub(repl, text)
        return cleaned, "；".join(remarks)

    def _extract_contact(self, text: str) -> tuple[str, str]:
        match = CONTACT_PATTERN.search(text)
        if not match:
            return text, ""
        remark = match.group(0)
        return text[: match.start()], match.group(2).strip() or remark

    def _extract_province(self, text: str) -> tuple[str, str]:
        for name in sorted(PROVINCES, key=len, reverse=True):
            if text.startswith(name):
                return name, text[len(name) :]
        match = re.match(r"^([\u4e00-\u9fa5]{2,10}?省)", text)
        if match:
            province = match.group(1)
            return province, text[len(province) :]
        match = re.match(r"^([\u4e00-\u9fa5]{2,10}?自治区)", text)
        if match:
            province = match.group(1)
            return province, text[len(province) :]
        return "", text

    def _extract_city(self, text: str, province: str) -> tuple[str, str]:
        remaining = text
        if province in MUNICIPALITIES and province:
            return province, text
        for suffix in CITY_SUFFIXES:
            pattern = re.compile(rf"^([\u4e00-\u9fa5]{{2,12}}{suffix})")
            match = pattern.match(remaining)
            if match:
                city = match.group(1)
                return city, remaining[len(city) :]
        return "", remaining

    def _extract_by_suffix(self, text: str, suffixes: Iterable[str]) -> tuple[str, str]:
        for suffix in suffixes:
            pattern = re.compile(rf"^([\u4e00-\u9fa5]{{2,12}}{suffix})")
            match = pattern.match(text)
            if match:
                value = match.group(1)
                return value, text[len(value) :]
        return "", text

    def _extract_first(self, pattern: re.Pattern[str], text: str) -> tuple[str, str]:
        match = pattern.search(text)
        if not match:
            return "", text
        value = match.group(1).strip()
        new_text = text[: match.start()] + text[match.end() :]
        return value, new_text

    def _run_lac(self, text: str) -> list[tuple[str, str]]:
        if not self.lac:
            return []
        try:
            return self.lac.tag(text)
        except Exception:
            return []

    def _apply_lac_assist(self, parsed: ParsedAddress, lac_tags: list[tuple[str, str]]) -> None:
        if not lac_tags:
            return
        used: set[str] = set(filter(None, [parsed.province, parsed.city, parsed.district, parsed.town, parsed.road, parsed.neighborhood]))
        if not parsed.town:
            town_candidate = self._pick_with_suffix(lac_tags, TOWN_SUFFIXES, used)
            if town_candidate:
                parsed.town = town_candidate
                used.add(town_candidate)
        if not parsed.road:
            road_candidate = self._pick_with_pattern(lac_tags, ROAD_PATTERN, used)
            if road_candidate:
                parsed.road = road_candidate
                used.add(road_candidate)
        if not parsed.neighborhood:
            neighborhood_candidate = self._pick_with_pattern(lac_tags, NEIGHBORHOOD_PATTERN, used, prefer_longest=True)
            if neighborhood_candidate:
                parsed.neighborhood = neighborhood_candidate
                used.add(neighborhood_candidate)

    def _pick_with_suffix(
        self,
        lac_tags: Sequence[tuple[str, str]],
        suffixes: Iterable[str],
        used: set[str],
    ) -> str:
        for token, tag in lac_tags:
            if token in used or not token:
                continue
            if any(token.endswith(suffix) for suffix in suffixes):
                return token
        return ""

    def _pick_with_pattern(
        self,
        lac_tags: Sequence[tuple[str, str]],
        pattern: re.Pattern[str],
        used: set[str],
        prefer_longest: bool = False,
    ) -> str:
        candidates: list[str] = []
        for token, tag in lac_tags:
            if token in used or not token:
                continue
            if pattern.search(token):
                candidates.append(token)
        if not candidates:
            return ""
        if prefer_longest:
            candidates.sort(key=lambda x: len(x), reverse=True)
        return candidates[0]

    def _extract_named(
        self,
        pattern: re.Pattern[str],
        text: str,
        keep_suffix: bool = False,
    ) -> tuple[str, str]:
        match = pattern.search(text)
        if not match:
            return "", text
        value = match.group("value").strip()
        suffix = match.groupdict().get("suffix", "")
        final = value + suffix if keep_suffix and suffix else value
        new_text = text[: match.start()] + text[match.end() :]
        return final, new_text

    def _extract_trailing_room(self, text: str) -> tuple[str, str]:
        match = TRAILING_ROOM_PATTERN.search(text)
        if not match:
            return "", text
        value = match.group("value")
        new_text = text[: match.start()]
        return value, new_text


class AddressCleaner:
    def __init__(self, parser: AddressParser | None = None, enable_lac: bool = True) -> None:
        self.parser = parser or AddressParser(enable_lac=enable_lac)

    def clean_dataframe(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        if column not in df.columns:
            raise ValueError(f"地址列 {column} 不存在")
        parsed_rows: list[dict[str, str]] = []
        for value in df[column].fillna(""):
            parsed = self.parser.parse(str(value))
            parsed_rows.append(parsed.to_row())
        parsed_df = pd.DataFrame(parsed_rows, index=df.index)
        merged = df.copy()
        for col in parsed_df.columns:
            merged[col] = parsed_df[col]
        return merged

    def process_file(self, input_path: Path, output_path: Path, column: str) -> pd.DataFrame:
        df = pd.read_excel(input_path)
        cleaned = self.clean_dataframe(df, column)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned.to_excel(output_path, index=False)
        return cleaned
