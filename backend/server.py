from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel
from database.manager import TripManager, VendorManager

app = FastAPI(title="NYC Train Mobility API")

trip_manager = TripManager()
vendor_manager = VendorManager()

# pydantic models for API validation and serialization

class VendorIn(BaseModel):
    vendor_name: str

class VendorOut(BaseModel):
    vendor_id: int
    vendor_name: Optional[str]

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

# Vendors Routes

@app.get("/vendors/", response_model=List[VendorOut])
def list_vendors():
    """Retrieves all vendors"""
    return vendor_manager.get_all_vendors()

@app.get("/vendors/{vendor_id}", response_model=VendorOut)
def get_vendor(vendor_id: int):
    """Retrieves a vendor by ID"""
    vendor = vendor_manager.get_vendor_by_id(vendor_id)
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    return vendor

@app.post("/vendors/", response_model=VendorOut, status_code=201)
def create_vendor(vendor_in: VendorIn):
    """Creates a new vendor"""
    vendor_id = vendor_manager.add_vendor(vendor_in.vendor_name)
    return vendor_manager.get_vendor_by_id(vendor_id)

@app.put("/vendors/{vendor_id}", response_model=VendorOut)
def update_vendor(vendor_id: int, vendor_in: VendorIn):
    """Updates vendor name"""
    vendor = vendor_manager.get_vendor_by_id(vendor_id)
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")

    # directly update vendor name
    with vendor_manager.get_connection() as conn:
        cursor = conn.execute("UPDATE Vendors SET vendor_name = ? WHERE vendor_id = ?", (vendor_in.vendor_name, vendor_id))
        conn.commit()

    updated_vendor = vendor_manager.get_vendor_by_id(vendor_id)
    return updated_vendor

@app.delete("/vendors/{vendor_id}", status_code=204)
def delete_vendor(vendor_id: int):
    """Deletes a vendor by ID"""
    success = vendor_manager.delete_vendor(vendor_id)
    if not success:
        raise HTTPException(status_code=404, detail="Vendor not found")
    return

# Trips Routes

@app.get("/trips/", response_model=List[TripOut])
def list_trips(
    limit: int = Query(100, le=500),
    vendor_id: Optional[int] = None
):
    """Lists trips (optionally filtered by vendor_id)"""
    if vendor_id:
        return trip_manager.find_trips_by_vendor(vendor_id, limit)
    return trip_manager.get_trips(limit)

@app.get("/trips/{trip_id}", response_model=TripOut)
def get_trip(trip_id: int):
    """Gets trip by ID"""
    trip = trip_manager.get_trip_by_id(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip

@app.post("/trips/", response_model=TripOut, status_code=201)
def create_trip(trip_in: TripIn):
    """Adds a new trip record"""
    trip_id = trip_manager.add_trip(trip_in)
    return trip_manager.get_trip_by_id(trip_id)

@app.put("/trips/{trip_id}", response_model=TripOut)
def update_trip(trip_id: int, trip_in: TripIn):
    """Updates trip record by ID"""
    existing_trip = trip_manager.get_trip_by_id(trip_id)
    if not existing_trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    # directly update trip record 
    with trip_manager.get_connection() as conn:
        cursor = conn.execute('''
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

    updated_trip = trip_manager.get_trip_by_id(trip_id)
    return updated_trip

@app.delete("/trips/{trip_id}", status_code=204)
def delete_trip(trip_id: int):
    """Deletes trip by ID"""
    success = trip_manager.delete_trip(trip_id)
    if not success:
        raise HTTPException(status_code=404, detail="Trip not found")
    return
