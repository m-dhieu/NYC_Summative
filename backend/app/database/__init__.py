#---------------------------------------------------------------------------------
# Script Name: __init__.py
# Description: Initializes global manager instances to ease use across the package.
# Author:      Santhiana Ange Kaze
# Date:        2025-10-14
#----------------------------------------------------------------------------------

from .connection import get_connection
from .models import *
from .manager import TripManager, VendorManager

# Initialize global manager instances to ease use across the package
trip_manager = TripManager()
vendor_manager = VendorManager()

__all__ = [
    'get_connection',
    'TripManager',
    'VendorManager',
    'trip_manager',
    'vendor_manager',
]
