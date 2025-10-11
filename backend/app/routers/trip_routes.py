from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from database.manager import TripManager
from auth import get_current_active_user
from pydantic import BaseModel

router = APIRouter()
trip_manager = TripManager()

class TripIn(BaseModel):
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

class TripOut(TripIn):
    trip_id: int

@router.get("/", response_model=List[TripOut])
def list_trips(limit: int = Query(100, le=500), vendor_id: Optional[int] = None, current_user=Depends(get_current_active_user)):
    """Retrieves trips (optionally filtered by vendor ID with authentication required)"""
    if vendor_id:
        return trip_manager.find_trips_by_vendor(vendor_id, limit)
    return trip_manager.get_trips(limit)

@router.get("/{trip_id}", response_model=TripOut)
def get_trip(trip_id: int, current_user=Depends(get_current_active_user)):
    """Retrieves a trip by ID (authentication required)"""
    trip = trip_manager.get_trip_by_id(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip

@router.post("/", response_model=TripOut, status_code=201)
def create_trip(trip_in: TripIn, current_user=Depends(get_current_active_user)):
    """Creates a new trip (authentication required)"""
    trip_id = trip_manager.add_trip(trip_in)
    return trip_manager.get_trip_by_id(trip_id)

@router.put("/{trip_id}", response_model=TripOut)
def update_trip(trip_id: int, trip_in: TripIn, current_user=Depends(get_current_active_user)):
    """Updates an existing trip (authentication required)"""
    existing_trip = trip_manager.get_trip_by_id(trip_id)
    if not existing_trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    with trip_manager.get_connection() as conn:
        conn.execute('''
            UPDATE Trips SET
                vendor_id = ?,
                pickup_datetime = ?,
                dropoff_datetime = ?,
                passenger_count = ?,
                pickup_longitude = ?,
                pickup_latitude = ?,
                dropoff_longitude = ?,
                dropoff_latitude = ?,
                store_and_fwd_flag = ?,
                trip_duration_sec = ?
            WHERE trip_id = ?
        ''', (
            trip_in.vendor_id,
            trip_in.pickup_datetime,
            trip_in.dropoff_datetime,
            trip_in.passenger_count,
            trip_in.pickup_longitude,
            trip_in.pickup_latitude,
            trip_in.dropoff_longitude,
            trip_in.dropoff_latitude,
            trip_in.store_and_fwd_flag,
            trip_in.trip_duration_sec,
            trip_id
        ))
        conn.commit()
    return trip_manager.get_trip_by_id(trip_id)

@router.delete("/{trip_id}", status_code=204)
def delete_trip(trip_id: int, current_user=Depends(get_current_active_user)):
    """Deletes a trip by ID (authentication required)"""
    success = trip_manager.delete_trip(trip_id)
    if not success:
        raise HTTPException(status_code=404, detail="Trip not found")
