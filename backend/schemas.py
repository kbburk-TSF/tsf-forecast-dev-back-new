from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

class AirQualityRecord(BaseModel):
    date_local: date
    parameter_name: str
    arithmetic_mean: float
    local_site_name: Optional[str] = None
    state_name: str
    county_name: Optional[str] = None
    city_name: Optional[str] = None
    cbsa_name: Optional[str] = None

class SeriesPoint(BaseModel):
    date: date
    value: float

class ForecastRequest(BaseModel):
    state: str
    parameter: str
    h: int = Field(default=30, ge=1, le=365)
    agg: str = Field(default="mean", pattern="^(mean|sum)$")
    method: str = Field(default="seasonal_naive_dow", pattern="^(seasonal_naive_dow|ewma)$")

class ForecastResponse(BaseModel):
    history: List[SeriesPoint]
    forecast: List[SeriesPoint]
    method: str
