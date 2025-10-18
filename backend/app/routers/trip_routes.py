#---------------------------------------------------------------------
# File Name:   trip.py
# Description: FastAPI router that manages trips with CRUD operations
#              (list, get, create, update, delete)
#              Uses Pydantic models for request/response validation
#              Secured by user authentication
#              Interacts with the database through a TripManager class
# Author:      Monica Dhieu
# Date:        2025-10-14
#---------------------------------------------------------------------

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any, Tuple
from app.database.manager import TripManager
from app.database.connection import get_connection
from app.auth import get_current_active_user
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
    with get_connection() as conn:
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


# ---------- Analytics endpoints used by frontend dashboard ----------

class SummaryOut(BaseModel):
    total_trips: int
    avg_duration_sec: float
    busiest_hour: Optional[int]


def _apply_basic_filters(params: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Build a WHERE clause and parameter list for basic filters supported by the frontend.
    Currently supported: date (YYYY-MM-DD), hour (0-23)
    Note: distance, zone, fare are ignored as they are not present in the schema.
    """
    where = " WHERE 1=1"
    args: List[Any] = []
    date = params.get("date")
    hour = params.get("hour")
    if date:
        # pickup_datetime stored as text; compare on date prefix
        where += " AND substr(pickup_datetime, 1, 10) = ?"
        args.append(str(date))
    if hour is not None and str(hour) != "":
        # Compare hour extracted via strftime('%H')
        where += " AND cast(strftime('%H', pickup_datetime) as integer) = ?"
        args.append(int(hour))
    return where, args


@router.get("/summary", response_model=SummaryOut)
def trips_summary(
    date: Optional[str] = None,
    hour: Optional[int] = Query(None, ge=0, le=23),
    current_user=Depends(get_current_active_user),
):
    """Returns total trips, average duration, and busiest hour (0-23)."""
    where, args = _apply_basic_filters({"date": date, "hour": hour})
    with get_connection() as conn:
        # total and average duration
        row = conn.execute(
            f"SELECT COUNT(*) AS total, AVG(trip_duration_sec) AS avg_duration FROM Trips{where}",
            args,
        ).fetchone()
        total = int(row["total"]) if row and row["total"] is not None else 0
        avg_duration = float(row["avg_duration"]) if row and row["avg_duration"] is not None else 0.0

        # busiest hour by pickup count
        hr = conn.execute(
            f"""
            SELECT cast(strftime('%H', pickup_datetime) as integer) AS h, COUNT(*) AS c
            FROM Trips{where}
            GROUP BY h
            ORDER BY c DESC
            LIMIT 1
            """,
            args,
        ).fetchone()
        busiest = int(hr["h"]) if hr else None

    return SummaryOut(total_trips=total, avg_duration_sec=avg_duration, busiest_hour=busiest)


class TimeDistributionOut(BaseModel):
    hours: List[int]
    counts: List[int]


@router.get("/time-distribution", response_model=TimeDistributionOut)
def trips_time_distribution(
    date: Optional[str] = None,
    current_user=Depends(get_current_active_user),
):
    """Returns counts of trips grouped by pickup hour for a given date (or all)."""
    where, args = _apply_basic_filters({"date": date})
    counts_by_hour = {h: 0 for h in range(24)}
    with get_connection() as conn:
        for r in conn.execute(
            f"""
            SELECT cast(strftime('%H', pickup_datetime) as integer) AS h, COUNT(*) AS c
            FROM Trips{where}
            GROUP BY h
            ORDER BY h
            """,
            args,
        ).fetchall():
            counts_by_hour[int(r["h"])] = int(r["c"]) if r["c"] is not None else 0
    hours = list(range(24))
    counts = [counts_by_hour[h] for h in hours]
    return TimeDistributionOut(hours=hours, counts=counts)


class DurationHistogramOut(BaseModel):
    bins: List[str]
    counts: List[int]


@router.get("/duration-histogram", response_model=DurationHistogramOut)
def trips_duration_histogram(
    date: Optional[str] = None,
    current_user=Depends(get_current_active_user),
):
    """Returns a simple histogram of trip durations using fixed bins (in seconds)."""
    # Define duration bins in seconds: [0-300), [300-600), [600-900), [900-1200), [1200-1800), [1800+)
    bin_edges = [0, 300, 600, 900, 1200, 1800]
    bin_labels = ["0-5m", "5-10m", "10-15m", "15-20m", "20-30m", "30m+"]
    counts = [0] * len(bin_labels)
    where, args = _apply_basic_filters({"date": date})
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT trip_duration_sec FROM Trips{where}", args
        ).fetchall()
        for row in rows:
            d = int(row["trip_duration_sec"]) if row["trip_duration_sec"] is not None else 0
            idx = None
            for i in range(len(bin_edges)):
                if i < len(bin_edges) - 1:
                    if bin_edges[i] <= d < bin_edges[i + 1]:
                        idx = i
                        break
                else:
                    # last bin: >= last edge
                    if d >= bin_edges[i]:
                        idx = i
                        break
            if idx is not None:
                counts[idx] += 1
    return DurationHistogramOut(bins=bin_labels, counts=counts)


class HeatmapPoint(BaseModel):
    x: float
    y: float


class PickupHeatmapOut(BaseModel):
    locations: List[HeatmapPoint]


@router.get("/pickup-heatmap", response_model=PickupHeatmapOut)
def trips_pickup_heatmap(
    date: Optional[str] = None,
    limit: int = Query(500, ge=10, le=5000),
    current_user=Depends(get_current_active_user),
):
    """Returns a sample of pickup locations as points for scatter plotting."""
    where, args = _apply_basic_filters({"date": date})
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT pickup_longitude, pickup_latitude
            FROM Trips{where}
            ORDER BY pickup_datetime DESC
            LIMIT ?
            """,
            [*args, limit],
        ).fetchall()
    pts = [HeatmapPoint(x=float(r["pickup_longitude"]), y=float(r["pickup_latitude"])) for r in rows]
    return PickupHeatmapOut(locations=pts)
