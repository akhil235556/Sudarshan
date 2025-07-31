from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from decimal import Decimal
from pydantic import BaseModel, Field, validator
from ..database import get_db
from ..models.vehicle import VehicleType, VehicleCapacity, VehicleRepository
from enum import Enum

router = APIRouter(prefix="/api/vehicle-types", tags=["vehicle-types"])

class SortField(str, Enum):
    TYPE_CODE = "type_code"
    NAME = "name"
    WEIGHT = "weight_capacity_mt"
    VOLUME = "volume"

class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"

class VehicleTypeCreate(BaseModel):
    type_code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    length_m: Decimal = Field(..., gt=0, description="Length in meters")
    width_m: Decimal = Field(..., gt=0, description="Width in meters")
    height_m: Decimal = Field(..., gt=0, description="Height in meters")
    weight_capacity_mt: Decimal = Field(..., gt=0, description="Weight capacity in metric tons")
    min_weight_utilization: Decimal = Field(70.0, ge=0, le=100, description="Minimum weight utilization percentage")
    min_volume_utilization: Decimal = Field(60.0, ge=0, le=100, description="Minimum volume utilization percentage")

    @validator('type_code')
    def validate_type_code(cls, v):
        if not v.strip():
            raise ValueError("type_code cannot be empty")
        return v.upper()

    @validator('name')
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("name cannot be empty")
        return v.strip()

class VehicleTypeResponse(BaseModel):
    type_code: str
    name: str
    length_m: Decimal
    width_m: Decimal
    height_m: Decimal
    weight_capacity_mt: Decimal

    class Config:
        from_attributes = True

class VehicleTypeUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    length_m: Optional[Decimal] = Field(None, gt=0)
    width_m: Optional[Decimal] = Field(None, gt=0)
    height_m: Optional[Decimal] = Field(None, gt=0)
    weight_capacity_mt: Optional[Decimal] = Field(None, gt=0)
    min_weight_utilization: Optional[Decimal] = Field(None, ge=0, le=100)
    min_volume_utilization: Optional[Decimal] = Field(None, ge=0, le=100)

    @validator('name')
    def validate_name(cls, v):
        if v is not None and not v.strip():
            raise ValueError("name cannot be empty if provided")
        return v.strip() if v else v

class BulkVehicleTypeCreate(BaseModel):
    vehicle_types: List[VehicleTypeCreate]

    @validator('vehicle_types')
    def validate_bulk_size(cls, v):
        if not v:
            raise ValueError("At least one vehicle type is required")
        if len(v) > 100:
            raise ValueError("Maximum 100 vehicle types allowed per request")
        return v

@router.post("/", response_model=VehicleTypeResponse)
async def create_vehicle_type(vehicle_type: VehicleTypeCreate, db=Depends(get_db)):
    """Create a new vehicle type with capacity details"""
    try:
        repo = VehicleRepository(db)
        
        # Convert metric tons to kg for storage
        weight_capacity_kg = vehicle_type.weight_capacity_mt * 1000
        
        # Calculate volume in cubic meters
        volume_capacity_cbm = (
            vehicle_type.length_m * 
            vehicle_type.width_m * 
            vehicle_type.height_m
        )

        # Create vehicle type
        vt = VehicleType(
            type_code=vehicle_type.type_code,
            name=vehicle_type.name,
            description=vehicle_type.description
        )
        created_type = repo.create_vehicle_type(vt)

        # Create capacity
        capacity = VehicleCapacity(
            vehicle_type_id=created_type.id,
            weight_capacity_kg=weight_capacity_kg,
            volume_capacity_cbm=volume_capacity_cbm,
            min_weight_utilization=vehicle_type.min_weight_utilization,
            min_volume_utilization=vehicle_type.min_volume_utilization
        )
        repo.create_vehicle_capacity(capacity)

        return VehicleTypeResponse(
            type_code=created_type.type_code,
            name=created_type.name,
            length_m=vehicle_type.length_m,
            width_m=vehicle_type.width_m,
            height_m=vehicle_type.height_m,
            weight_capacity_mt=vehicle_type.weight_capacity_mt
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/", response_model=List[VehicleTypeResponse])
async def get_vehicle_types(
    type_code: Optional[str] = Query(None, description="Filter by type code"),
    min_weight: Optional[float] = Query(None, gt=0, description="Filter by minimum weight capacity (MT)"),
    max_weight: Optional[float] = Query(None, gt=0, description="Filter by maximum weight capacity (MT)"),
    sort_by: Optional[SortField] = Query(SortField.TYPE_CODE, description="Field to sort by"),
    order: Optional[SortOrder] = Query(SortOrder.ASC, description="Sort order"),
    db=Depends(get_db)
):
    """Get all vehicle types with filtering and sorting options"""
    try:
        query = """
        SELECT 
            vt.type_code,
            vt.name,
            vc.length_m,
            vc.width_m,
            vc.height_m,
            vc.weight_capacity_kg,
            vc.volume_capacity_cbm
        FROM vehicle_types vt
        JOIN vehicle_capacities vc ON vt.id = vc.vehicle_type_id
        WHERE 1=1
        """
        params = []

        if type_code:
            query += " AND vt.type_code ILIKE %s"
            params.append(f"%{type_code}%")

        if min_weight:
            query += " AND vc.weight_capacity_kg >= %s"
            params.append(min_weight * 1000)  # Convert to kg

        if max_weight:
            query += " AND vc.weight_capacity_kg <= %s"
            params.append(max_weight * 1000)  # Convert to kg

        # Add sorting
        sort_column = {
            SortField.TYPE_CODE: "vt.type_code",
            SortField.NAME: "vt.name",
            SortField.WEIGHT: "vc.weight_capacity_kg",
            SortField.VOLUME: "vc.volume_capacity_cbm"
        }[sort_by]

        query += f" ORDER BY {sort_column} {order.upper()}"

        with db.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            return [
                VehicleTypeResponse(
                    type_code=row['type_code'],
                    name=row['name'],
                    length_m=row['length_m'],
                    width_m=row['width_m'],
                    height_m=row['height_m'],
                    weight_capacity_mt=Decimal(str(row['weight_capacity_kg'])) / 1000
                )
                for row in results
            ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk", response_model=List[VehicleTypeResponse])
async def create_vehicle_types_bulk(
    vehicle_types: BulkVehicleTypeCreate,
    db=Depends(get_db)
):
    """Create multiple vehicle types in a single request"""
    try:
        repo = VehicleRepository(db)
        created_types = []

        # Start a transaction
        db.begin()
        try:
            for vt in vehicle_types.vehicle_types:
                # Convert metric tons to kg
                weight_capacity_kg = vt.weight_capacity_mt * 1000
                
                # Calculate volume
                volume_capacity_cbm = vt.length_m * vt.width_m * vt.height_m

                # Create vehicle type
                vehicle_type = VehicleType(
                    type_code=vt.type_code,
                    name=vt.name,
                    description=vt.description
                )
                created_type = repo.create_vehicle_type(vehicle_type)

                # Create capacity
                capacity = VehicleCapacity(
                    vehicle_type_id=created_type.id,
                    length_m=vt.length_m,
                    width_m=vt.width_m,
                    height_m=vt.height_m,
                    weight_capacity_kg=weight_capacity_kg,
                    volume_capacity_cbm=volume_capacity_cbm,
                    min_weight_utilization=vt.min_weight_utilization,
                    min_volume_utilization=vt.min_volume_utilization
                )
                repo.create_vehicle_capacity(capacity)

                created_types.append(VehicleTypeResponse(
                    type_code=created_type.type_code,
                    name=created_type.name,
                    length_m=vt.length_m,
                    width_m=vt.width_m,
                    height_m=vt.height_m,
                    weight_capacity_mt=vt.weight_capacity_mt
                ))

            db.commit()
            return created_types

        except Exception as e:
            db.rollback()
            raise e

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.patch("/{type_code}", response_model=VehicleTypeResponse)
async def update_vehicle_type(
    type_code: str,
    vehicle_type: VehicleTypeUpdate,
    db=Depends(get_db)
):
    """Update an existing vehicle type"""
    try:
        # Start transaction
        db.begin()
        try:
            # Get existing vehicle type
            query = """
            SELECT vt.id, vc.id as capacity_id
            FROM vehicle_types vt
            JOIN vehicle_capacities vc ON vt.id = vc.vehicle_type_id
            WHERE vt.type_code = %s
            """
            with db.cursor() as cursor:
                cursor.execute(query, (type_code,))
                result = cursor.fetchone()
                if not result:
                    raise HTTPException(status_code=404, detail="Vehicle type not found")

                vt_id = result['id']
                vc_id = result['capacity_id']

                # Update vehicle type if name provided
                if vehicle_type.name:
                    cursor.execute(
                        "UPDATE vehicle_types SET name = %s WHERE id = %s",
                        (vehicle_type.name, vt_id)
                    )

                # Update capacity if any dimension or capacity fields provided
                if any([
                    vehicle_type.length_m,
                    vehicle_type.width_m,
                    vehicle_type.height_m,
                    vehicle_type.weight_capacity_mt,
                    vehicle_type.min_weight_utilization,
                    vehicle_type.min_volume_utilization
                ]):
                    # Get current values
                    cursor.execute(
                        "SELECT * FROM vehicle_capacities WHERE id = %s",
                        (vc_id,)
                    )
                    current = cursor.fetchone()

                    # Prepare update values
                    updates = {
                        'length_m': vehicle_type.length_m or current['length_m'],
                        'width_m': vehicle_type.width_m or current['width_m'],
                        'height_m': vehicle_type.height_m or current['height_m'],
                        'weight_capacity_kg': (vehicle_type.weight_capacity_mt * 1000 if vehicle_type.weight_capacity_mt 
                                            else current['weight_capacity_kg']),
                        'volume_capacity_cbm': ((vehicle_type.length_m or current['length_m']) *
                                              (vehicle_type.width_m or current['width_m']) *
                                              (vehicle_type.height_m or current['height_m'])),
                        'min_weight_utilization': vehicle_type.min_weight_utilization or current['min_weight_utilization'],
                        'min_volume_utilization': vehicle_type.min_volume_utilization or current['min_volume_utilization']
                    }

                    cursor.execute("""
                        UPDATE vehicle_capacities
                        SET length_m = %s,
                            width_m = %s,
                            height_m = %s,
                            weight_capacity_kg = %s,
                            volume_capacity_cbm = %s,
                            min_weight_utilization = %s,
                            min_volume_utilization = %s
                        WHERE id = %s
                        """,
                        (*updates.values(), vc_id)
                    )

            db.commit()

            # Return updated vehicle type
            return await get_vehicle_types(type_code=type_code, db=db)[0]

        except Exception as e:
            db.rollback()
            raise e

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{type_code}")
async def delete_vehicle_type(type_code: str, db=Depends(get_db)):
    """Delete a vehicle type if it's not associated with any vehicles"""
    try:
        # Check if type is used by any vehicles
        with db.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM vehicles v
                JOIN vehicle_types vt ON v.vehicle_type_id = vt.id
                WHERE vt.type_code = %s
            """, (type_code,))
            
            if cursor.fetchone()['count'] > 0:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot delete vehicle type that is associated with vehicles"
                )

            # Delete vehicle type (cascade will handle capacity)
            cursor.execute("""
                DELETE FROM vehicle_types
                WHERE type_code = %s
                RETURNING id
            """, (type_code,))
            
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Vehicle type not found")

            db.commit()
            
            return {"message": f"Vehicle type {type_code} deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 