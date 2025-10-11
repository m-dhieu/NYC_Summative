"""
Utility and helper functions for general purpose use across the app
"""

def calculate_trip_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculates distance between two points on Earth using the Haversine formula

    Args:
        lat1 (float): Latitude of point 1 in decimal degrees
        lon1 (float): Longitude of point 1 in decimal degrees
        lat2 (float): Latitude of point 2 in decimal degrees
        lon2 (float): Longitude of point 2 in decimal degrees

    Returns:
        float: Distance between points in kilometers
    """
    from math import radians, cos, sin, asin, sqrt

    # convert degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine calculation
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))

    R = 6371  # radius of Earth in km
    distance = R * c
    return distance
