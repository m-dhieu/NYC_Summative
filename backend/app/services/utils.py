#-----------------------------------------------------------------------------
# File Name:   utils.py
# Description: Provides a general-purpose utility function that calculates the
#              distance between two geographic coordinates using the Haversine 
#              formula, returning the distance in kilometers
# Author:      Monica Dhieu
# Date:        2025-10-14
#-----------------------------------------------------------------------------

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

    # Haversine formula
    """
    Calculates the shortest distance between two points on a sphere
    based on their latitude and longitude coordinates.

    a = sin²(Δlat / 2) + cos(lat1) * cos(lat2) * sin²(Δlon / 2)
      # 'a' is the square of half the chord length between the points

    c = 2 * asin(√a)
      # 'c' is the angular distance in radians

    distance = R * c
      # distance is the arc length on the sphere (Earth) surface,
      # where R is the radius of the Earth (mean radius = 6371 km)

    where:
    lat1, lat2 = latitudes of point 1 and point 2 in radians
    lon1, lon2 = longitudes of point 1 and point 2 in radians
    Δlat = lat2 - lat1
    Δlon = lon2 - lon1

    The result is the shortest distance over the Earth’s surface between
    the two points in kilometers.
    """
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))

    R = 6371
    distance = R * c
    return distance
