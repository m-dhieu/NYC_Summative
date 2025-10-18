from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.database.manager import VendorManager
from app.database.connection import get_connection
from app.auth import get_current_active_user
from pydantic import BaseModel

router = APIRouter()
vendor_manager = VendorManager()

class VendorIn(BaseModel):
    vendor_name: str

class VendorOut(BaseModel):
    vendor_id: int
    vendor_name: str

@router.get("/", response_model=List[VendorOut])
def list_vendors(current_user=Depends(get_current_active_user)):
    """Retrieves all vendors (authentication required)"""
    return vendor_manager.get_all_vendors()

@router.get("/{vendor_id}", response_model=VendorOut)
def get_vendor(vendor_id: int, current_user=Depends(get_current_active_user)):
    """Retrieves vendor by ID (authentication required)"""
    vendor = vendor_manager.get_vendor_by_id(vendor_id)
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    return vendor

@router.post("/", response_model=VendorOut, status_code=201)
def create_vendor(vendor_in: VendorIn, current_user=Depends(get_current_active_user)):
    """Creates a new vendor (authentication required)"""
    vendor_id = vendor_manager.add_vendor(vendor_in.vendor_name)
    return vendor_manager.get_vendor_by_id(vendor_id)

@router.put("/{vendor_id}", response_model=VendorOut)
def update_vendor(vendor_id: int, vendor_in: VendorIn, current_user=Depends(get_current_active_user)):
    """Updates an existing vendor (authentication required)"""
    vendor = vendor_manager.get_vendor_by_id(vendor_id)
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    with get_connection() as conn:
        conn.execute("UPDATE Vendors SET vendor_name = ? WHERE vendor_id = ?", (vendor_in.vendor_name, vendor_id))
        conn.commit()
    return vendor_manager.get_vendor_by_id(vendor_id)

@router.delete("/{vendor_id}", status_code=204)
def delete_vendor(vendor_id: int, current_user=Depends(get_current_active_user)):
    """Deletes a vendor by ID (authentication required)"""
    success = vendor_manager.delete_vendor(vendor_id)
    if not success:
        raise HTTPException(status_code=404, detail="Vendor not found")
