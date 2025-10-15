#---------------------------------------------------
# File Name:   data_processing.py
# Description: Loads raw CSV data from a zipped archive
#              Cleans and normalizes datetime and numeric fields
#              Derives useful features, including trip speed, efficiency, idle time, and fare per km
#              Implements a manual linked list to detect and log speed outliers
#              Saves the cleaned dataset for use in backend services
#              Inserts cleaned data directly into the SQLite backend DB
#  Author:      Janviere Munezero
# Date:        2025-10-11
#--------------------------------------------------- 

import pandas as pd
import numpy as np
import sqlite3
import os
import zipfile
from datetime import datetime
from typing import List, Dict, Optional, Any
import math

# Define type alias for clarity
TripRecord = Dict[str, Any]

# Filepath constants
SPEED_OUTLIER_THRESHOLD = 120.0  # km/h


class TrainDataProcessor:
    """
    Main class for processing NYC train trip data.
    Handles loading, cleaning, feature engineering, and database insertion.
    """
    
    def __init__(self, zip_filepath=None, output_dir=None):
        # Set default paths relative to project root
        if zip_filepath is None:
            # Get the project root directory (two levels up from this file)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            zip_filepath = os.path.join(project_root, "data", "raw", "train.zip")
        
        if output_dir is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            output_dir = os.path.join(project_root, "data", "processed")
        
        self.zip_filepath = zip_filepath
        self.output_dir = output_dir
        self.df = None
        self.original_shape = None
        self.cleaning_log = []
        self.outlier_records = None
        self.removed_records = None
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def log_step(self, message):
        """Log processing steps with timestamp"""
        self.cleaning_log.append(f"{datetime.now().strftime('%H:%M:%S')} - {message}")
        print(f"[INFO] {message}")
    
    def load_data(self):
        """
        Load the raw NYC dataset (CSV) from zip file
        """
        self.log_step("Loading raw data from zip file...")
        
        try:
            # Extract and read CSV from zip
            with zipfile.ZipFile(self.zip_filepath, 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise ValueError("No CSV file found in zip archive")
                
                # Read the first CSV file found
                with zip_ref.open(csv_files[0]) as csv_file:
                    self.df = pd.read_csv(csv_file)
            
            self.original_shape = self.df.shape
            self.log_step(f"Loaded dataset: {self.original_shape[0]} rows, {self.original_shape[1]} columns")
            return self
            
        except Exception as e:
            self.log_step(f"Error loading data: {str(e)}")
            raise
    
    def handle_missing_values(self):
        """
        Handle missing values, duplicates, invalid records, and outliers
        """
        self.log_step("Handling missing values and invalid records...")
        
        initial_rows = len(self.df)
        
        # Clean datetime columns
        self.df['pickup_datetime'] = pd.to_datetime(self.df['pickup_datetime'], errors='coerce', utc=True)
        self.df['dropoff_datetime'] = pd.to_datetime(self.df['dropoff_datetime'], errors='coerce', utc=True)
        
        # Clean passenger count
        self.df['passenger_count'] = self.df['passenger_count'].fillna(1).astype(int).clip(lower=1)
        
        # Store records that will be removed for transparency
        invalid_mask = (
            self.df['pickup_datetime'].isna() | 
            self.df['dropoff_datetime'].isna() |
            (self.df['pickup_latitude'] == 0) | 
            (self.df['pickup_longitude'] == 0) |
            (self.df['dropoff_latitude'] == 0) | 
            (self.df['dropoff_longitude'] == 0)
        )
        
        self.removed_records = self.df[invalid_mask].copy()
        
        # Remove invalid records
        self.df = self.df[~invalid_mask]
        
        # Remove duplicates
        self.df = self.df.drop_duplicates()
        
        rows_removed = initial_rows - len(self.df)
        self.log_step(f"Removed {rows_removed} invalid/duplicate records")
        
        return self
    
    def normalize_data(self):
        """
        Normalize and format timestamps, coordinates, and numeric fields
        """
        self.log_step("Normalizing and formatting data...")
        
        # Ensure datetime columns are properly formatted
        self.df['pickup_datetime'] = pd.to_datetime(self.df['pickup_datetime'], utc=True)
        self.df['dropoff_datetime'] = pd.to_datetime(self.df['dropoff_datetime'], utc=True)
        
        # Round coordinates to reasonable precision
        coord_cols = ['pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude']
        for col in coord_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].round(6)
        
        # Ensure numeric types
        if 'passenger_count' in self.df.columns:
            self.df['passenger_count'] = self.df['passenger_count'].astype(int)
        
        if 'fare_amount' in self.df.columns:
            self.df['fare_amount'] = pd.to_numeric(self.df['fare_amount'], errors='coerce')
        
        self.log_step("Data normalization completed")
        return self
    
    def calculate_trip_distance(self, lat1, lon1, lat2, lon2):
        """Calculate Haversine distance between two points in kilometers"""
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        return 6371 * c  # Earth radius in km
    
    def create_derived_features(self):
        """
        Define and justify at least three derived features
        Creates: trip speed, idle time, fare per km, trip efficiency
        """
        self.log_step("Creating derived features...")
        
        # Calculate trip duration in seconds
        self.df['trip_duration_sec'] = (self.df['dropoff_datetime'] - self.df['pickup_datetime']).dt.total_seconds()
        self.df['trip_duration_sec'] = self.df['trip_duration_sec'].where(self.df['trip_duration_sec'] >= 0)
        
        # Calculate trip distance using Haversine formula
        self.df['trip_distance_km'] = self.df.apply(
            lambda row: self.calculate_trip_distance(
                row['pickup_latitude'], row['pickup_longitude'],
                row['dropoff_latitude'], row['dropoff_longitude']
            ), axis=1
        )
        
        # Feature 1: Trip Speed (km/h)
        self.df['trip_speed_kmh'] = self.df['trip_distance_km'] / (self.df['trip_duration_sec'] / 3600)
        self.df['trip_speed_kmh'] = self.df['trip_speed_kmh'].where(self.df['trip_duration_sec'] > 0)
        
        # Feature 2: Idle Time (seconds between trips for same vendor)
        self.df = self.df.sort_values(['vendor_id', 'pickup_datetime'])
        idle_times = []
        last_dropoff = {}
        
        for _, row in self.df.iterrows():
            vendor = row['vendor_id']
            pickup = row['pickup_datetime']
            
            idle = None
            if vendor in last_dropoff:
                diff = (pickup - last_dropoff[vendor]).total_seconds()
                idle = diff if diff >= 0 else None
            
            idle_times.append(idle)
            last_dropoff[vendor] = row['dropoff_datetime']
        
        self.df['idle_time_sec'] = idle_times
        
        # Feature 3: Fare per km
        if 'fare_amount' in self.df.columns:
            self.df['fare_per_km'] = self.df['fare_amount'] / self.df['trip_distance_km']
            self.df['fare_per_km'] = self.df['fare_per_km'].replace([float('inf'), -float('inf')], pd.NA)
        
        # Additional feature: Trip Efficiency
        self.df['trip_efficiency'] = self.df['trip_speed_kmh'] / SPEED_OUTLIER_THRESHOLD
        self.df['trip_efficiency'] = self.df['trip_efficiency'].clip(upper=1.0)
        
        self.log_step("Created derived features: trip_speed_kmh, idle_time_sec, fare_per_km, trip_efficiency")
        return self
    
    def detect_outliers(self):
        """
        Log excluded or suspicious records for transparency
        Uses manual linked list implementation to detect speed outliers
        """
        self.log_step("Detecting speed outliers using linked list...")
        
        outliers = LinkedList()
        for _, trip in self.df.iterrows():
            speed = trip.get("trip_speed_kmh")
            if pd.isna(speed):
                speed = 0
            if speed > SPEED_OUTLIER_THRESHOLD:
                outliers.add(trip.to_dict())
        
        self.outlier_records = outliers
        outlier_count = len(outliers.to_list())
        self.log_step(f"Detected {outlier_count} speed outliers (> {SPEED_OUTLIER_THRESHOLD} km/h)")
        
        return self
    
    def save_cleaned_data(self):
        """Save cleaned dataset and transparency logs"""
        self.log_step("Saving cleaned data and transparency logs...")
        
        # Save main cleaned dataset
        output_path = os.path.join(self.output_dir, "clean_train.csv")
        self.df.to_csv(output_path, index=False)
        self.log_step(f"Saved cleaned data to: {output_path}")
        
        # Save transparency logs
        self.save_transparency_logs()
        
        return self
    
    def save_transparency_logs(self):
        """Save logs for excluded or suspicious records"""
        try:
            # Save removed records
            if self.removed_records is not None and len(self.removed_records) > 0:
                removed_path = os.path.join(self.output_dir, "removed_invalid_records.csv")
                self.removed_records.to_csv(removed_path, index=False)
                self.log_step(f"Saved removed records to: {removed_path}")
            
            # Save outlier records
            if self.outlier_records is not None:
                outliers_list = self.outlier_records.to_list()
                if outliers_list:
                    outliers_df = pd.DataFrame(outliers_list)
                    outliers_path = os.path.join(self.output_dir, "speed_outliers.csv")
                    outliers_df.to_csv(outliers_path, index=False)
                    self.log_step(f"Saved speed outliers to: {outliers_path}")
            
            # Save processing log
            log_path = os.path.join(self.output_dir, "processing_log.txt")
            with open(log_path, 'w') as f:
                for step in self.cleaning_log:
                    f.write(step + '\n')
            self.log_step(f"Saved processing log to: {log_path}")
            
        except Exception as e:
            self.log_step(f"Error saving transparency logs: {str(e)}")
    
    def insert_to_database(self, db_path=None):
        """Insert cleaned data into SQLite database"""
        if db_path is None:
            # Set default database path relative to project root
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            db_path = os.path.join(project_root, "backend", "nyc_train.db")
        
        self.log_step("Inserting cleaned data into database...")
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Insert unique vendors (let database auto-assign vendor_id)
            vendor_ids = self.df['vendor_id'].unique()
            vendor_id_mapping = {}
            for vid in vendor_ids:
                cursor.execute("""
                    INSERT OR IGNORE INTO Vendors (vendor_name) VALUES (?)
                """, (f"Vendor {vid}",))
                # Get the auto-assigned vendor_id
                cursor.execute("SELECT vendor_id FROM Vendors WHERE vendor_name = ?", (f"Vendor {vid}",))
                result = cursor.fetchone()
                if result:
                    vendor_id_mapping[vid] = result[0]
            
            # Prepare trip records for insertion (matching your schema)
            trips_columns = [
                'vendor_id', 'pickup_datetime', 'dropoff_datetime',
                'passenger_count', 'pickup_longitude', 'pickup_latitude',
                'dropoff_longitude', 'dropoff_latitude', 'trip_duration_sec'
            ]
            
            # Filter columns that exist in the dataframe
            available_columns = [col for col in trips_columns if col in self.df.columns]
            
            insert_query = f"""
            INSERT INTO Trips ({', '.join(available_columns)})
            VALUES ({', '.join(['?']*len(available_columns))})
            """
            
            # Format datetime columns
            df_for_db = self.df[available_columns].copy()
            for col in ['pickup_datetime', 'dropoff_datetime']:
                if col in df_for_db.columns:
                    df_for_db[col] = df_for_db[col].apply(lambda x: x.isoformat() if not pd.isna(x) else None)
            
            # Map vendor_ids to database vendor_ids and prepare records
            df_for_db = df_for_db.copy()
            df_for_db['vendor_id'] = df_for_db['vendor_id'].map(vendor_id_mapping)
            
            # Insert records
            trip_records = df_for_db.replace({pd.NA: None, pd.NaT: None}).values.tolist()
            cursor.executemany(insert_query, trip_records)
            
            conn.commit()
            conn.close()
            
            self.log_step(f"Inserted {len(trip_records)} trip records into database")
            
        except Exception as e:
            self.log_step(f"Error inserting to database: {str(e)}")
        
        return self
    
    def print_summary(self):
        """Print processing summary"""
        self.log_step("Processing Summary:")
        self.log_step(f"Original dataset: {self.original_shape[0]} rows, {self.original_shape[1]} columns")
        self.log_step(f"Cleaned dataset: {self.df.shape[0]} rows, {self.df.shape[1]} columns")
        self.log_step(f"Data retention: {(self.df.shape[0] / self.original_shape[0] * 100):.2f}%")
        
        if self.outlier_records:
            outlier_count = len(self.outlier_records.to_list())
            self.log_step(f"Speed outliers detected: {outlier_count}")


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


def process_pipeline():
    """
    Main pipeline function that executes the full data processing workflow
    """
    processor = TrainDataProcessor()
    
    try:
        processor.load_data()
        processor.handle_missing_values()
        processor.normalize_data()
        processor.create_derived_features()
        processor.detect_outliers()
        processor.save_cleaned_data()
        processor.insert_to_database()
        processor.print_summary()
        
        print("[SUCCESS] Data processing pipeline completed successfully!")
        
    except Exception as e:
        print(f"[ERROR] Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    process_pipeline()
