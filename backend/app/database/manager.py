#----------------------------------------------------
# Script Name: manager.py
# Description: Manages the NYC Train Mobility database
# Author:      Santhiana Ange Kaze
# Date:        2025-10-14
#-----------------------------------------------------

from typing import List, Optional, Dict, Any
from .connection import get_connection
from .models import Trip, Vendor

class VendorManager:
    # Manage CRUD operations for Vendors table

    def get_vendor_by_id(self, vendor_id: int) -> Optional[Vendor]:
        # Fetch vendor by ID
        with get_connection() as conn:
            cursor = conn.execute("SELECT vendor_id, vendor_name FROM Vendors WHERE vendor_id = ?", (vendor_id,))
            row = cursor.fetchone()
            return Vendor(**row) if row else None

    def get_all_vendors(self) -> List[Vendor]:
        # Retrieve all vendors
        with get_connection() as conn:
            cursor = conn.execute("SELECT vendor_id, vendor_name FROM Vendors")
            return [Vendor(**row) for row in cursor.fetchall()]

    def add_vendor(self, vendor_name: str) -> int:
        # Add new vendor and return its ID
        with get_connection() as conn:
            cursor = conn.execute("INSERT INTO Vendors (vendor_name) VALUES (?)", (vendor_name,))
            conn.commit()
            return cursor.lastrowid

    def delete_vendor(self, vendor_id: int) -> bool:
        # Delete vendor by ID
        with get_connection() as conn:
            cursor = conn.execute("DELETE FROM Vendors WHERE vendor_id = ?", (vendor_id,))
            conn.commit()
            return cursor.rowcount > 0

class TripManager:
    # Manage CRUD operations and queries for Trips table

    def get_trip_by_id(self, trip_id: int) -> Optional[Trip]:
        # Get specific trip by ID
        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM Trips WHERE trip_id = ?", (trip_id,))
            row = cursor.fetchone()
            return Trip(**row) if row else None

    def get_trips(self, limit: int = 100) -> List[Trip]:
        # Retrieve recent trips with a default limit (sort by pickup_datetime, descending)
        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM Trips ORDER BY pickup_datetime DESC LIMIT ?", (limit,))
            return [Trip(**row) for row in cursor.fetchall()]

    def add_trip(self, trip: Trip) -> int:
        # Insert new trip record and return the inserted trip ID
        with get_connection() as conn:
            cursor = conn.execute('''
                INSERT INTO Trips 
                (vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
                 pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
                 store_and_fwd_flag, trip_duration_sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                trip.trip_duration_sec
            ))
            conn.commit()
            return cursor.lastrowid

    def update_trip_duration(self, trip_id: int, new_duration_sec: int) -> bool:
        # Update duration for specific trip
        with get_connection() as conn:
            cursor = conn.execute("UPDATE Trips SET trip_duration_sec = ? WHERE trip_id = ?", (new_duration_sec, trip_id))
            conn.commit()
            return cursor.rowcount > 0

    def delete_trip(self, trip_id: int) -> bool:
        # Delete trip entry by ID
        with get_connection() as conn:
            cursor = conn.execute("DELETE FROM Trips WHERE trip_id = ?", (trip_id,))
            conn.commit()
            return cursor.rowcount > 0
        
    def find_trips_by_vendor(self, vendor_id: int, limit: int = 100) -> List[Trip]:
        # Find trips by a specific vendor, limited to recent ones
        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM Trips WHERE vendor_id = ? ORDER BY pickup_datetime DESC LIMIT ?", (vendor_id, limit))
            return [Trip(**row) for row in cursor.fetchall()]

    def find_trips_by_time_range(self, start_datetime: str, end_datetime: str, limit: int = 100) -> List[Trip]:
        # Retrieve trips occurring within a specified datetime range
        with get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM Trips 
                WHERE pickup_datetime >= ? AND dropoff_datetime <= ?
                ORDER BY pickup_datetime DESC LIMIT ?
            """, (start_datetime, end_datetime, limit))
            return [Trip(**row) for row in cursor.fetchall()]

    def find_trips_by_passenger_count(self, passenger_count: int, limit: int = 100) -> List[Trip]:
        # Retrieve trips filtered by passenger count
        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM Trips WHERE passenger_count = ? ORDER BY pickup_datetime DESC LIMIT ?", (passenger_count, limit))
            return [Trip(**row) for row in cursor.fetchall()]

    def find_trips_by_duration_range(self, min_duration: int, max_duration: int, limit: int = 100) -> List[Trip]:
        # Find trips within specific duration range
        with get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM Trips 
                WHERE trip_duration_sec BETWEEN ? AND ?
                ORDER BY pickup_datetime DESC LIMIT ?
            """, (min_duration, max_duration, limit))
            return [Trip(**row) for row in cursor.fetchall()]

    def filter_trips(self, filters: Dict[str, Any], limit: int = 100) -> List[Trip]:
        """
        Generic filter method for trips supporting multiple keys.
        Supported keys: vendor_id, passenger_count, min_duration, max_duration, start_datetime, end_datetime
        """
        query = "SELECT * FROM Trips WHERE 1=1"
        params = []

        if 'vendor_id' in filters:
            query += " AND vendor_id = ?"
            params.append(filters['vendor_id'])
        if 'passenger_count' in filters:
            query += " AND passenger_count = ?"
            params.append(filters['passenger_count'])
        if 'min_duration' in filters:
            query += " AND trip_duration_sec >= ?"
            params.append(filters['min_duration'])
        if 'max_duration' in filters:
            query += " AND trip_duration_sec <= ?"
            params.append(filters['max_duration'])
        if 'start_datetime' in filters:
            query += " AND pickup_datetime >= ?"
            params.append(filters['start_datetime'])
        if 'end_datetime' in filters:
            query += " AND dropoff_datetime <= ?"
            params.append(filters['end_datetime'])

        query += " ORDER BY pickup_datetime DESC LIMIT ?"
        params.append(limit)

        with get_connection() as conn:
            cursor = conn.execute(query, params)
            return [Trip(**row) for row in cursor.fetchall()]
          
