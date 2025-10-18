#---------------------------------------------------
# File Name:   vendor_controller.py
# Description: Handles vendor-related business logic
# Author:      Monica Dhieu
# Date:        2025-10-14
#---------------------------------------------------

"""
Controller layer managing vendor-related business logic.
It abstracts DB calls for vendors behind business rules.
"""

from typing import List, Optional
from app.database.manager import VendorManager

vendor_manager = VendorManager()

def get_all_vendors() -> List[dict]:
    """Retrieves all vendors"""
    return vendor_manager.get_all_vendors()

def get_vendor_by_id(vendor_id: int) -> Optional[dict]:
    """Retrieves vendor by ID"""
    return vendor_manager.get_vendor_by_id(vendor_id)

def create_vendor(vendor_name: str) -> int:
    """Creates new vendor & returns new vendor ID"""
    return vendor_manager.add_vendor(vendor_name)

def update_vendor(vendor_id: int, vendor_name: str) -> dict:
    """Updates vendor name & returns updated vendor dict."""
    if not vendor_manager.get_vendor_by_id(vendor_id):
        raise ValueError("Vendor not found")

    with vendor_manager.get_connection() as conn:
        conn.execute("UPDATE Vendors SET vendor_name = ? WHERE vendor_id = ?", (vendor_name, vendor_id))
        conn.commit()
    return vendor_manager.get_vendor_by_id(vendor_id)

def delete_vendor(vendor_id: int) -> bool:
    """Deletes vendor by ID & returns True if successful"""
    return vendor_manager.delete_vendor(vendor_id)
