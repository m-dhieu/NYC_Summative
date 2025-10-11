"""
Business logic and validation layer for trip-related domain rules
to separate business concerns from routing and DB access
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator

class TripValidator(BaseModel):
    vendor_id: int
    pickup_datetime: str
    dropoff_datetime: str
    passenger_count: int
    pickup_longitude: float
    pickup_latitude: float
    dropoff_longitude: float
    dropoff_latitude: float
    store_and_fwd_flag: Optional[str] = None
    trip_duration_sec: int

    @validator('pickup_datetime', 'dropoff_datetime')
    def validate_datetime_format(cls, v):
        # verify datetime strings are in ISO 8601 format
        try:
            datetime.fromisoformat(v)
        except ValueError:
            raise ValueError("Datetime fields must be ISO 8601 formatted strings")
        return v

    @validator('passenger_count')
    def validate_passenger_count(cls, v):
        if v <= 0:
            raise ValueError("Passenger count must be positive")
        return v

    @validator('trip_duration_sec')
    def validate_duration(cls, v):
        if v < 0:
            raise ValueError("Trip duration cannot be negative")
        return v

def validate_trip_data(trip_data: dict):
    """
    Validates raw trip data dictionary using Pydantic model.
    Raises ValidationError if data is invalid.
    """
    trip = TripValidator(**trip_data)
    return trip
