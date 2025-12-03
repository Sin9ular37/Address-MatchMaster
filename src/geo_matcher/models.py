from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class POIRecord(BaseModel):
    poi_id: str
    name: str
    province: str = ""
    city: str = ""
    district: str = ""
    street: str = ""
    house_number: str = ""
    latitude: float
    longitude: float
    poi_type: Optional[str] = None
    normalized: Optional[str] = None


class AddressRecord(BaseModel):
    order_id: str
    raw_address: str
    province: str = ""
    city: str = ""
    district: str = ""
    street: str = ""
    house_number: str = ""
    normalized: Optional[str] = None


class MatchResult(BaseModel):
    order_id: str
    raw_address: str
    matched_poi_id: Optional[str] = None
    matched_poi_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    score: float = 0.0
    source: str = "RULE"
    candidates: list[dict] = Field(default_factory=list)
