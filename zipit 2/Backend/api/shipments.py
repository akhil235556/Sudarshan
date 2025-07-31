from fastapi import APIRouter, HTTPException, Depends, Query, Path
from typing import List, Optional, Dict
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field, validator, constr
from ..database import get_db
from enum import Enum
from math import ceil

router = APIRouter(prefix="/api/shipments", tags=["shipments"])

class ShipmentStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

class SortField(str, Enum):
    CREATED_AT = "created_at"
    PRIORITY = "priority"
    WEIGHT = "weight_kg"
    VOLUME = "volume_cbm"

class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"

class ShipmentCreate(BaseModel):
    shipment_id: str = Field(..., min_length=1, max_length=50)
    from_location: str = Field(..., min_length=1, max_length=100)
    to_location: str = Field(..., min_length=1, max_length=100)
    consignee_name: str = Field(..., min_length=1, max_length=255)
    material_code: str = Field(..., min_length=1, max_length=50)
    material_count: int = Field(..., gt=0)
    weight_kg: Decimal = Field(..., gt=0, description="Weight in kilograms")
    length_m: Decimal = Field(..., gt=0, description="Length in meters")
    width_m: Decimal = Field(..., gt=0, description="Width in meters")
    height_m: Decimal = Field(..., gt=0, description="Height in meters")
    priority: int = Field(1, ge=1, le=5, description="Priority level (1-5)")
    sla_hours: Optional[int] = Field(None, gt=0)
    request_id: Optional[str] = None

    @validator('shipment_id')
    def validate_shipment_id(cls, v):
        if not v.strip():
            raise ValueError("shipment_id cannot be empty")
        return v.upper()

    @validator('from_location', 'to_location')
    def validate_locations(cls, v):
        if not v.strip():
            raise ValueError("location cannot be empty")
        return v.strip()

    @validator('material_code')
    def validate_material_code(cls, v):
        if not v.strip():
            raise ValueError("material_code cannot be empty")
        return v.upper()

    @validator('consignee_name')
    def validate_consignee_name(cls, v):
        if not v.strip():
            raise ValueError("consignee_name cannot be empty")
        return v.strip()

class ShipmentResponse(BaseModel):
    shipment_id: str
    from_location: str
    to_location: str
    weight_kg: Decimal
    length_m: Decimal
    width_m: Decimal
    height_m: Decimal
    status: ShipmentStatus

    class Config:
        from_attributes = True

class BulkShipmentCreate(BaseModel):
    shipments: List[ShipmentCreate]

    @validator('shipments')
    def validate_bulk_size(cls, v):
        if not v:
            raise ValueError("At least one shipment is required")
        if len(v) > 100:
            raise ValueError("Maximum 100 shipments allowed per request")
        
        # Check for duplicate shipment IDs
        ids = [s.shipment_id for s in v]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate shipment IDs are not allowed")
        
        return v

class PaginatedResponse(BaseModel):
    items: List[ShipmentResponse]
    total: int
    page: int
    page_size: int
    total_pages: int

class StatusTransition(BaseModel):
    status: ShipmentStatus
    comment: Optional[str] = Field(None, max_length=500)
    vehicle_id: Optional[str] = Field(None, max_length=50)

    @validator('status')
    def validate_status_transition(cls, v, values, **kwargs):
        # Add any specific status transition rules here
        return v

@router.post("/", response_model=ShipmentResponse)
async def create_shipment(shipment: ShipmentCreate, db=Depends(get_db)):
    """Create a new shipment with validations"""
    try:
        # Validate locations are different
        if shipment.from_location.lower() == shipment.to_location.lower():
            raise HTTPException(
                status_code=400,
                detail="From and To locations cannot be the same"
            )

        # Calculate volume
        volume_cbm = (
            shipment.length_m * 
            shipment.width_m * 
            shipment.height_m
        )

        query = """
        INSERT INTO shipments (
            shipment_id, from_location, to_location, consignee_name,
            material_code, material_count, weight_kg, length_m,
            width_m, height_m, volume_cbm, priority,
            sla_hours, request_id, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING 
            shipment_id, from_location, to_location, weight_kg,
            length_m, width_m, height_m, status;
        """

        with db.cursor() as cursor:
            cursor.execute(query, (
                shipment.shipment_id,
                shipment.from_location.lower(),
                shipment.to_location.lower(),
                shipment.consignee_name,
                shipment.material_code,
                shipment.material_count,
                shipment.weight_kg,
                shipment.length_m,
                shipment.width_m,
                shipment.height_m,
                volume_cbm,
                shipment.priority,
                shipment.sla_hours,
                shipment.request_id,
                ShipmentStatus.PENDING
            ))
            result = cursor.fetchone()
            db.commit()

            return ShipmentResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk", response_model=List[ShipmentResponse])
async def create_shipments_bulk(
    shipments: BulkShipmentCreate,
    db=Depends(get_db)
):
    """Create multiple shipments in a single request"""
    try:
        created_shipments = []
        
        # Start transaction
        db.begin()
        try:
            for shipment in shipments.shipments:
                # Validate locations
                if shipment.from_location.lower() == shipment.to_location.lower():
                    raise ValueError(f"Shipment {shipment.shipment_id}: From and To locations cannot be the same")

                # Calculate volume
                volume_cbm = shipment.length_m * shipment.width_m * shipment.height_m

                query = """
                INSERT INTO shipments (
                    shipment_id, from_location, to_location, consignee_name,
                    material_code, material_count, weight_kg, length_m,
                    width_m, height_m, volume_cbm, priority,
                    sla_hours, request_id, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING 
                    shipment_id, from_location, to_location, weight_kg,
                    length_m, width_m, height_m, status;
                """

                with db.cursor() as cursor:
                    cursor.execute(query, (
                        shipment.shipment_id,
                        shipment.from_location.lower(),
                        shipment.to_location.lower(),
                        shipment.consignee_name,
                        shipment.material_code,
                        shipment.material_count,
                        shipment.weight_kg,
                        shipment.length_m,
                        shipment.width_m,
                        shipment.height_m,
                        volume_cbm,
                        shipment.priority,
                        shipment.sla_hours,
                        shipment.request_id,
                        ShipmentStatus.PENDING
                    ))
                    result = cursor.fetchone()
                    created_shipments.append(ShipmentResponse(**result))

            db.commit()
            return created_shipments

        except Exception as e:
            db.rollback()
            raise e

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", response_model=PaginatedResponse)
async def get_shipments(
    status: Optional[ShipmentStatus] = Query(None, description="Filter by status"),
    from_location: Optional[str] = Query(None, description="Filter by origin location"),
    to_location: Optional[str] = Query(None, description="Filter by destination location"),
    min_weight: Optional[float] = Query(None, gt=0, description="Filter by minimum weight (kg)"),
    max_weight: Optional[float] = Query(None, gt=0, description="Filter by maximum weight (kg)"),
    min_volume: Optional[float] = Query(None, gt=0, description="Filter by minimum volume (m³)"),
    max_volume: Optional[float] = Query(None, gt=0, description="Filter by maximum volume (m³)"),
    priority: Optional[int] = Query(None, ge=1, le=5, description="Filter by priority level"),
    created_after: Optional[datetime] = Query(None, description="Filter by creation date after"),
    created_before: Optional[datetime] = Query(None, description="Filter by creation date before"),
    sort_by: Optional[SortField] = Query(SortField.CREATED_AT, description="Field to sort by"),
    order: Optional[SortOrder] = Query(SortOrder.DESC, description="Sort order"),
    page: int = Query(1, gt=0, description="Page number"),
    page_size: int = Query(20, gt=0, le=100, description="Items per page"),
    db=Depends(get_db)
):
    """Get shipments with pagination, filtering and sorting"""
    try:
        # Build base query
        query = """
        SELECT 
            shipment_id,
            from_location,
            to_location,
            weight_kg,
            length_m,
            width_m,
            height_m,
            status
        FROM shipments
        WHERE 1=1
        """
        count_query = "SELECT COUNT(*) FROM shipments WHERE 1=1"
        params = []
        count_params = []

        # Add filters
        if status:
            query += " AND status = %s"
            count_query += " AND status = %s"
            params.append(status)
            count_params.append(status)

        if from_location:
            query += " AND from_location ILIKE %s"
            count_query += " AND from_location ILIKE %s"
            params.append(f"%{from_location}%")
            count_params.append(f"%{from_location}%")

        if to_location:
            query += " AND to_location ILIKE %s"
            count_query += " AND to_location ILIKE %s"
            params.append(f"%{to_location}%")
            count_params.append(f"%{to_location}%")

        if min_weight:
            query += " AND weight_kg >= %s"
            count_query += " AND weight_kg >= %s"
            params.append(min_weight)
            count_params.append(min_weight)

        if max_weight:
            query += " AND weight_kg <= %s"
            count_query += " AND weight_kg <= %s"
            params.append(max_weight)
            count_params.append(max_weight)

        if min_volume:
            query += " AND volume_cbm >= %s"
            count_query += " AND volume_cbm >= %s"
            params.append(min_volume)
            count_params.append(min_volume)

        if max_volume:
            query += " AND volume_cbm <= %s"
            count_query += " AND volume_cbm <= %s"
            params.append(max_volume)
            count_params.append(max_volume)

        if priority:
            query += " AND priority = %s"
            count_query += " AND priority = %s"
            params.append(priority)
            count_params.append(priority)

        if created_after:
            query += " AND created_at >= %s"
            count_query += " AND created_at >= %s"
            params.append(created_after)
            count_params.append(created_after)

        if created_before:
            query += " AND created_at <= %s"
            count_query += " AND created_at <= %s"
            params.append(created_before)
            count_params.append(created_before)

        # Add sorting
        sort_column = {
            SortField.CREATED_AT: "created_at",
            SortField.PRIORITY: "priority",
            SortField.WEIGHT: "weight_kg",
            SortField.VOLUME: "volume_cbm"
        }[sort_by]
        query += f" ORDER BY {sort_column} {order.upper()}"

        # Add pagination
        query += " LIMIT %s OFFSET %s"
        params.extend([page_size, (page - 1) * page_size])

        with db.cursor() as cursor:
            # Get total count
            cursor.execute(count_query, count_params)
            total = cursor.fetchone()['count']
            total_pages = ceil(total / page_size)

            # Get paginated results
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            return PaginatedResponse(
                items=[ShipmentResponse(**row) for row in results],
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{shipment_id}", response_model=ShipmentResponse)
async def get_shipment(shipment_id: str, db=Depends(get_db)):
    """Get a specific shipment by ID"""
    try:
        query = """
        SELECT 
            shipment_id,
            from_location,
            to_location,
            weight_kg,
            length_m,
            width_m,
            height_m,
            status
        FROM shipments
        WHERE shipment_id = %s
        """

        with db.cursor() as cursor:
            cursor.execute(query, (shipment_id,))
            result = cursor.fetchone()
            
            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Shipment {shipment_id} not found"
                )
            
            return ShipmentResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.patch("/{shipment_id}/status")
async def update_shipment_status(
    shipment_id: str = Path(..., description="The ID of the shipment to update"),
    transition: StatusTransition = None,
    db=Depends(get_db)
):
    """Update the status of a shipment with optional comment and vehicle assignment"""
    try:
        # Start transaction
        db.begin()
        try:
            # Get current status
            with db.cursor() as cursor:
                cursor.execute(
                    "SELECT status FROM shipments WHERE shipment_id = %s",
                    (shipment_id,)
                )
                result = cursor.fetchone()
                if not result:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Shipment {shipment_id} not found"
                    )

                current_status = result['status']

                # Validate status transition
                valid_transitions = {
                    'pending': ['assigned', 'cancelled'],
                    'assigned': ['in_transit', 'cancelled'],
                    'in_transit': ['delivered', 'cancelled'],
                    'delivered': [],
                    'cancelled': []
                }

                if transition.status not in valid_transitions[current_status]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid status transition from {current_status} to {transition.status}"
                    )

                # Update status and add history
                cursor.execute("""
                    UPDATE shipments
                    SET status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE shipment_id = %s
                    """,
                    (transition.status, shipment_id)
                )

            db.commit()
            
            return {
                "message": f"Shipment {shipment_id} status updated to {transition.status}",
                "previous_status": current_status,
                "new_status": transition.status
            }

        except Exception as e:
            db.rollback()
            raise e

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{shipment_id}")
async def delete_shipment(
    shipment_id: str = Path(..., description="The ID of the shipment to delete"),
    db=Depends(get_db)
):
    """Delete a shipment if it's in PENDING or CANCELLED status"""
    try:
        with db.cursor() as cursor:
            # Check current status
            cursor.execute(
                "SELECT status FROM shipments WHERE shipment_id = %s",
                (shipment_id,)
            )
            result = cursor.fetchone()
            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Shipment {shipment_id} not found"
                )

            if result['status'] not in ['pending', 'cancelled']:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot delete shipment in {result['status']} status"
                )

            # Delete shipment
            cursor.execute(
                "DELETE FROM shipments WHERE shipment_id = %s",
                (shipment_id,)
            )
            db.commit()

            return {"message": f"Shipment {shipment_id} deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 