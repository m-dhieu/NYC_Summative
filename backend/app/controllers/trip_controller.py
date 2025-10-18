#-------------------------------------------------
# File Name:   trip_controller.py
# Description: Handles trip-related business logic
# Author:      Monica Dhieu
# Date:        2025-10-14
#-------------------------------------------------

"""
Controller layer handling trip-related business logic.
It sits between API routes and DB layer, applying validation and domain rules.
"""

from typing import List, Optional
from database.manager import TripManager
from services.trip_service import validate_trip_data, TripValidator
from pydantic import ValidationError

trip_manager = TripManager()

def get_all_trips(limit: int = 100) -> List[dict]:
    """Retrieves all trips up to a certain limit"""
    return trip_manager.get_trips(limit)

def get_trips_by_vendor(vendor_id: int, limit: int = 100) -> List[dict]:
    """Retrieves trips filtered by vendor ID"""
    return trip_manager.find_trips_by_vendor(vendor_id, limit)

def get_trip(trip_id: int) -> Optional[dict]:
    """Retrieves a trip by ID"""
    return trip_manager.get_trip_by_id(trip_id)

def create_trip(trip_data: dict) -> int:
    """
    Validates trip data, creates a new trip, & returns created trip ID
    """
    try:
        trip = validate_trip_data(trip_data)
    except ValidationError as e:
        raise ValueError(f"Invalid trip data: {e}")

    trip_id = trip_manager.add_trip(trip.dict())
    return trip_id

def update_trip(trip_id: int, trip_data: dict) -> dict:
    """
    Validates updated trip data, updates existing trip by ID, & returns 
    updated trip dictionary
    """
    if not trip_manager.get_trip_by_id(trip_id):
        raise ValueError("Trip not found")

    try:
        trip = validate_trip_data(trip_data)
    except ValidationError as e:
        raise ValueError(f"Invalid trip data: {e}")

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
            trip.vendor_id,
            trip.pickup_datetime,
            trip.dropoff_datetime,
            trip.passenger_count,
            trip.pickup_longitude,
            trip.pickup_latitude,
            trip.dropoff_longitude,
            trip.dropoff_latitude,
            trip.store_and_fwd_flag,
            trip.trip_duration_sec,
            trip_id
        ))
        conn.commit()

    return trip_manager.get_trip_by_id(trip_id)

def delete_trip(trip_id: int) -> bool:
    """Deletes trip by ID & returns True if successful"""
    return trip_manager.delete_trip(trip_id)
