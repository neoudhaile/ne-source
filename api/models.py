from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class RunRecord(BaseModel):
    id: int
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str
    inserted: int
    skipped_geo: int
    skipped_dupe: int
    total_leads: Optional[int] = None
    error_message: Optional[str] = None
    triggered_by: Optional[str] = None
    cost: Optional[float] = None

    class Config:
        from_attributes = True


class IndustryCount(BaseModel):
    industry: str
    count: int


class OwnershipCount(BaseModel):
    ownership_type: str
    count: int


class StatsResponse(BaseModel):
    total_leads: int
    by_industry: list[IndustryCount]
    by_ownership_type: list[OwnershipCount]


class StatusResponse(BaseModel):
    is_running: bool
    active_run_id: Optional[int] = None
    next_run_at: Optional[str] = None


class ConfigPayload(BaseModel):
    industries: Optional[list[str]] = None
    cities: Optional[list[str]] = None
    min_reviews: Optional[int] = None
    min_rating: Optional[float] = None
    geo_radius_miles: Optional[int] = None
    max_leads_per_run: Optional[int] = None


class TriggerResponse(BaseModel):
    run_id: int
