from .connection import get_connection
from .models import *
from .manager import TripManager, VendorManager

# initialize global manager instances to ease use across the package
trip_manager = TripManager()
vendor_manager = VendorManager()

__all__ = [
    'get_connection',
    'TripManager',
    'VendorManager',
    'trip_manager',
    'vendor_manager',
]
