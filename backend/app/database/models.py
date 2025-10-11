from dataclasses import dataclass
from typing import Optional

@dataclass
class Vendor:
    """Represents a vendor with a unique identifier & name (optional)"""
    vendor_id: int
    vendor_name: Optional[str] = None

@dataclass
class Trip:
    """
    Represents a trip record with:
        unique identification
        vendor operating 
        pickup date & time
        dropoff date & time
        number of passengers
        pickup longitude coordinate 
        pickup latitude coordinate
        dropoff longitude coordinate
        dropoff latitude coordinate
        store & fwd flag (Optional: 'Y' or 'N' flag indicating data forwarding status)
        duration in seconds
    """
    trip_id: int
    vendor_id: int
    pickup_datetime: str
    dropoff_datetime: str
    passenger_count: int
    pickup_longitude: float
    pickup_latitude: float
    dropoff_longitude: float
    dropoff_latitude: float
    store_and_fwd_flag: Optional[str] = None
    trip_duration_sec: int = 0
