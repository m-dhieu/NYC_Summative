import unittest
import sqlite3

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # use in-memory SQLite DB for testing
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()

        # create vendors and trips tables
        self.cursor.execute("""
            CREATE TABLE vendors (
                vendor_id INTEGER PRIMARY KEY,
                vendor_name TEXT NOT NULL
            )
        """)
        self.cursor.execute("""
            CREATE TABLE trips (
                trip_id INTEGER PRIMARY KEY,
                vendor_id INTEGER NOT NULL,
                pickup_datetime TEXT NOT NULL,
                dropoff_datetime TEXT NOT NULL,
                passenger_count INTEGER NOT NULL,
                pickup_latitude REAL NOT NULL,
                pickup_longitude REAL NOT NULL,
                dropoff_latitude REAL NOT NULL,
                dropoff_longitude REAL NOT NULL,
                trip_duration_sec INTEGER,
                trip_distance_km REAL,
                trip_speed_kmh REAL,
                trip_efficiency REAL,
                idle_time_sec REAL,
                fare_amount REAL,
                fare_per_km REAL,
                FOREIGN KEY(vendor_id) REFERENCES vendors(vendor_id)
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.cursor.close()
        self.conn.close()

    def test_insert_and_query_vendor(self):
        # insert vendor
        self.cursor.execute("INSERT INTO vendors (vendor_id, vendor_name) VALUES (?, ?)", (1, "Test Vendor"))
        self.conn.commit()

        # query vendor
        self.cursor.execute("SELECT vendor_name FROM vendors WHERE vendor_id = 1")
        vendor_name = self.cursor.fetchone()[0]

        self.assertEqual(vendor_name, "Test Vendor")

    def test_insert_and_query_trip(self):
        # insert vendor first
        self.cursor.execute("INSERT INTO vendors (vendor_id, vendor_name) VALUES (?, ?)", (1, "Test Vendor"))
        # insert trip
        trip_data = (
            1,  # trip_id
            1,  # vendor_id
            "2025-10-10T09:00:00Z",  # pickup_datetime
            "2025-10-10T09:15:00Z",  # dropoff_datetime
            2,  # passenger_count
            40.7128,  # pickup_latitude
            -74.0060,  # pickup_longitude
            40.7060,  # dropoff_latitude
            -74.0086,  # dropoff_longitude
            900,  # trip_duration_sec
            3.2,  # trip_distance_km
            12.8,  # trip_speed_kmh
            0.11,  # trip_efficiency
            300,  # idle_time_sec
            15.0,  # fare_amount
            4.69,  # fare_per_km
        )
        self.cursor.execute("""
            INSERT INTO trips (
                trip_id, vendor_id, pickup_datetime, dropoff_datetime,
                passenger_count, pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude,
                trip_duration_sec, trip_distance_km, trip_speed_kmh, trip_efficiency,
                idle_time_sec, fare_amount, fare_per_km
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, trip_data)
        self.conn.commit()

        # query trip by id
        self.cursor.execute("SELECT passenger_count, trip_speed_kmh FROM trips WHERE trip_id = 1")
        result = self.cursor.fetchone()

        self.assertEqual(result, (2, 12.8))


if __name__ == "__main__":
    unittest.main()
