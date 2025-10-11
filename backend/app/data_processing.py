"""
Integrated data processing and feature engineering module for NYC train trip data.

This script:
      Loads raw CSV data from a zipped archive
      Cleans and normalizes datetime and numeric fields
      Derives useful features, including trip speed, efficiency, idle time, and fare per km
      Implements a manual linked list to detect and log speed outliers
      Saves the cleaned dataset for use in backend services
      Inserts cleaned data directly into the SQLite backend DB
"""

import pandas as pd
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Any
from services.utils import calculate_trip_distance

# define type alias for clarity
TripRecord = Dict[str, Any]

# filepath constants
RAW_DATA_PATH = "data/raw/train.zip"          
CLEANED_DATA_PATH = "data/processed/clean_train.csv"  
SPEED_OUTLIER_THRESHOLD = 120.0  # km/h
DB_PATH = "backend/nyc_train.db"

# clean data
def clean_datetime_series(dt_series: pd.Series) -> pd.Series:
    """
    Converts a pandas Series of datetime strings to datetime objects 
    Converts invalid/missing values to NaT
    """
    return pd.to_datetime(dt_series, errors="coerce", utc=True)

def clean_passenger_count_series(passenger_series: pd.Series) -> pd.Series:
    """Cleans passenger counts by filling missing values with 1 (integers >= 1)"""
    cleaned = passenger_series.fillna(1).astype(int)
    return cleaned.clip(lower=1)

# derive features
def calculate_trip_duration_secs(df: pd.DataFrame) -> pd.Series:
    """
    Calculates trip durations as seconds between dropoff and pickup timestamps
    Sets negative durations to NaN
    """
    delta = df['dropoff_datetime'] - df['pickup_datetime']
    duration_sec = delta.dt.total_seconds()
    return duration_sec.where(duration_sec >= 0)

def calculate_trip_distance_km(df: pd.DataFrame) -> pd.Series:
    """
    Calculates trip distances in kilometers from geographic coordinates 
    using the haversine formula
    """
    def dist(row):
        try:
            return calculate_trip_distance(
                row['pickup_latitude'], row['pickup_longitude'],
                row['dropoff_latitude'], row['dropoff_longitude'])
        except Exception:
            return None
    return df.apply(dist, axis=1)

def derive_trip_speed_kmh(df: pd.DataFrame) -> pd.Series:
    """
    Calculates trip speed in kilometers per hour 
    Sets speed to NaN if trip duration is zero/invalid
    """
    speed = df['trip_distance_km'] / (df['trip_duration_sec'] / 3600)
    return speed.where(df['trip_duration_sec'] > 0)

def derive_trip_efficiency(df: pd.DataFrame, max_speed: float = SPEED_OUTLIER_THRESHOLD) -> pd.Series:
    """
    Finds trip efficiency
    Trip efficiency is the ratio of trip speed to max expected speed, capped at 1.0
    """
    efficiency = df['trip_speed_kmh'] / max_speed
    return efficiency.clip(upper=1.0)

def derive_fare_per_km(df: pd.DataFrame) -> pd.Series:
    """
    Calculates fare per kilometer for each trip
    Handles division by zero and infinite values gracefully
    """
    fare_per_km = df['fare_amount'] / df['trip_distance_km']
    return fare_per_km.replace([float('inf'), -float('inf')], pd.NA)

def calculate_idle_time_sec(df: pd.DataFrame) -> pd.Series:
    """
    Calculates idle time in seconds per vendor
    Idle time is time difference between current pickup and previous dropoff
    """
    idle_times = []
    last_dropoff = {}
    for _, row in df.iterrows():
        vendor = row['vendor_id']
        pickup = row['pickup_datetime']

        idle = None
        if vendor in last_dropoff:
            diff = (pickup - last_dropoff[vendor]).total_seconds()
            idle = diff if diff >= 0 else None
        
        idle_times.append(idle)
        last_dropoff[vendor] = row['dropoff_datetime']
    
    return pd.Series(idle_times)

# Linked List implementation for Outlier detection
class Node:
    """Node class for linked list storing trip records"""
    def __init__(self, trip: TripRecord):
        self.trip = trip
        self.next = None

class LinkedList:
    """Simple linked list to store outlier trips"""
    def __init__(self):
        self.head = None
        self.tail = None

    def add(self, trip: TripRecord):
        new_node = Node(trip)
        if not self.head:
            self.head = new_node
            self.tail = new_node
        else:
            self.tail.next = new_node
            self.tail = new_node

    def to_list(self) -> List[TripRecord]:
        result = []
        current = self.head
        while current:
            result.append(current.trip)
            current = current.next
        return result

def detect_speed_outliers(df: pd.DataFrame, max_speed: float = SPEED_OUTLIER_THRESHOLD) -> LinkedList:
    """
    Detects trips where speed exceeds max_speed threshold 
    Stores outliers in linked list
    """
    outliers = LinkedList()
    for _, trip in df.iterrows():
        speed = trip.get("trip_speed_kmh")
        if pd.isna(speed):
            speed = 0
        if speed > max_speed:
            outliers.add(trip.to_dict())
    return outliers

# Database insertion function
def insert_cleaned_data_to_db(df: pd.DataFrame, db_path: str = DB_PATH):
    """
    Inserts cleaned vendor and trip data into SQLite database
    Assumes tables 'vendors' and 'trips' already created
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # insert unique vendors
    vendor_ids = df['vendor_id'].unique()
    for vid in vendor_ids:
        cursor.execute("""
            INSERT OR IGNORE INTO vendors (vendor_id, vendor_name) VALUES (?, ?)
        """, (vid, f"Vendor {vid}"))

    # columns in trips table for insertion
    trips_columns = [
        'vendor_id', 'pickup_datetime', 'dropoff_datetime',
        'passenger_count', 'pickup_latitude', 'pickup_longitude',
        'dropoff_latitude', 'dropoff_longitude', 'trip_duration_sec',
        'trip_distance_km', 'trip_speed_kmh', 'trip_efficiency',
        'idle_time_sec', 'fare_amount', 'fare_per_km'
    ]

    insert_query = f"""
    INSERT INTO trips ({', '.join(trips_columns)})
    VALUES ({', '.join(['?']*len(trips_columns))})
    """

    # format datetime columns in ISO string
    for col in ['pickup_datetime', 'dropoff_datetime']:
        df[col] = df[col].apply(lambda x: x.isoformat() if not pd.isna(x) else None)

    # prepare list of records to insert
    trip_records = df[trips_columns].replace({pd.NA: None, pd.NaT: None}).values.tolist()

    # bulk insert trips    
    cursor.executemany(insert_query, trip_records)

    conn.commit()
    conn.close()
    print(f"[INFO] Inserted {len(trip_records)} trip records into database at {db_path}")

# main pipeline
def process_pipeline():
    """
    Executes the full data processing pipeline:
        Loads data
        Cleans and normalizes fields
        Derives features
        Detects and logs speed outliers
        Saves cleaned CSV dataset
        Inserts cleaned data into backend SQLite DB
    """
    print("[INFO] Loading raw data from zip file...")
    df = pd.read_csv(RAW_DATA_PATH, compression='zip')

    print("[INFO] Cleaning datetime and passenger count fields...")
    df['pickup_datetime'] = clean_datetime_series(df['pickup_datetime'])
    df['dropoff_datetime'] = clean_datetime_series(df['dropoff_datetime'])
    df['passenger_count'] = clean_passenger_count_series(df['passenger_count'])

    print("[INFO] Deriving trip duration and distance...")
    df['trip_duration_sec'] = calculate_trip_duration_secs(df)
    df['trip_distance_km'] = calculate_trip_distance_km(df)

    print("[INFO] Calculating speed, efficiency, fare per km, and idle times...")
    df['trip_speed_kmh'] = derive_trip_speed_kmh(df)
    df['trip_efficiency'] = derive_trip_efficiency(df)
    df['fare_per_km'] = derive_fare_per_km(df)

    df = df.sort_values(['vendor_id', 'pickup_datetime'])
    df['idle_time_sec'] = calculate_idle_time_sec(df)

    print("[INFO] Dropping rows with critical missing data...")
    df_clean = df.dropna(subset=['pickup_datetime', 'dropoff_datetime', 'trip_duration_sec', 'trip_distance_km'])

    print("[INFO] Detecting speed outliers...")
    outliers = detect_speed_outliers(df_clean)

    print(f"[INFO] Number of speed outliers detected: {len(outliers.to_list())}")
    for i, outlier in enumerate(outliers.to_list(), start=1):
        print(f"Outlier {i}: {outlier}")

    print(f"[INFO] Saving cleaned and enriched data to {CLEANED_DATA_PATH} ...")
    df_clean.to_csv(CLEANED_DATA_PATH, index=False)

    print("[INFO] Inserting cleaned data into database...")
    insert_cleaned_data_to_db(df_clean)

    print("[INFO] Data processing pipeline complete.")

# run pipeline 
if __name__ == "__main__":
    process_pipeline()
