-- Define DB entities & attributes 

-- Vendors Table
-- Stores train vendor info;
CREATE TABLE IF NOT EXISTS Vendors (
    vendor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_name TEXT UNIQUE NOT NULL
) 

-- Trips Table
-- Stores details of individual trips: timing, location, passengers, duration, and vendor;
CREATE TABLE IF NOT EXISTS Trips (
    trip_id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id INTEGER NOT NULL,
    pickup_datetime TEXT NOT NULL,
    dropoff_datetime TEXT NOT NULL,
    passenger_count INTEGER NOT NULL CHECK(passenger_count > 0),
    pickup_longitude REAL NOT NULL,
    pickup_latitude REAL NOT NULL,
    dropoff_longitude REAL NOT NULL,
    dropoff_latitude REAL NOT NULL,
    store_and_fwd_flag TEXT CHECK(store_and_fwd_flag IN ('Y','N')),
    trip_duration_sec INTEGER NOT NULL CHECK(trip_duration_sec >= 0),
    FOREIGN KEY (vendor_id) REFERENCES Vendors(vendor_id)
)

-- Index pickup datetime for query performance
CREATE INDEX IF NOT EXISTS idx_trips_pickup_datetime ON Trips(pickup_datetime);

-- Index dropoff datetime for query performance
CREATE INDEX IF NOT EXISTS idx_trips_dropoff_datetime ON Trips(dropoff_datetime);

-- Index vendor for quick vendor trip lookup
CREATE INDEX IF NOT EXISTS idx_trips_vendor_id ON Trips(vendor_id);

-- Index pickup coordinates (supporting spatial queries)
CREATE INDEX IF NOT EXISTS idx_trips_pickup_coords ON Trips(pickup_longitude, pickup_latitude);

-- Index dropoff coordinates
CREATE INDEX IF NOT EXISTS idx_trips_dropoff_coords ON Trips(dropoff_longitude, dropoff_latitude);
