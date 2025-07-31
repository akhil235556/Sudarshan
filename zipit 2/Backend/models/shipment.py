from datetime import datetime
import uuid
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class Location:
    name: str
    code: str
    city: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None
    id: Optional[int] = None

@dataclass
class Material:
    code: str
    name: Optional[str] = None
    description: Optional[str] = None
    id: Optional[int] = None

@dataclass
class Shipment:
    shipment_id: str
    from_location: Location
    to_location: Location
    material_code: str
    material_count: int
    consignee_name: Optional[str] = None
    load_kg: Optional[Decimal] = None
    volume_cbm: Optional[Decimal] = None
    priority: Optional[str] = None
    placement_datetime: Optional[datetime] = None
    sla_hours: Optional[int] = None
    request_id: Optional[str] = None
    status: str = 'pending'
    id: Optional[int] = None

class ShipmentRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def create_location(self, location: Location) -> Location:
        """Create a new location in the database"""
        query = """
        INSERT INTO locations (name, code, city, latitude, longitude, address)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                location.name,
                location.code,
                location.city,
                location.latitude,
                location.longitude,
                location.address
            ))
            location.id = cursor.fetchone()[0]
            self.db.commit()
        return location

    def create_material(self, material: Material) -> Material:
        """Create a new material in the database"""
        query = """
        INSERT INTO materials (code, name, description)
        VALUES (%s, %s, %s)
        RETURNING id;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                material.code,
                material.name,
                material.description
            ))
            material.id = cursor.fetchone()[0]
            self.db.commit()
        return material

    def create_shipment(self, shipment: Shipment) -> Shipment:
        """Create a new shipment in the database with validations"""
        # Validate required fields
        if not all([
            shipment.shipment_id,
            shipment.from_location,
            shipment.to_location,
            shipment.material_code,
            shipment.material_count
        ]):
            raise ValueError("Missing required fields")

        # Validate load or volume is present
        if not (shipment.load_kg or shipment.volume_cbm):
            raise ValueError("Either load_kg or volume_cbm must be provided")

        # Validate locations are different
        if (shipment.from_location.code == shipment.to_location.code):
            raise ValueError("From and To locations cannot be the same")

        # Validate coordinates if provided
        for location in [shipment.from_location, shipment.to_location]:
            if location.latitude is not None or location.longitude is not None:
                if not (-90 <= location.latitude <= 90 and -180 <= location.longitude <= 180):
                    raise ValueError(f"Invalid coordinates for location {location.code}")

        # Set request_id if not provided
        if not shipment.request_id:
            shipment.request_id = str(uuid.uuid4())

        # Create locations if they don't exist
        from_location = self.get_or_create_location(shipment.from_location)
        to_location = self.get_or_create_location(shipment.to_location)

        query = """
        INSERT INTO shipments (
            shipment_id, from_location_id, to_location_id, consignee_name,
            material_code, material_count, load_kg, volume_cbm, priority,
            placement_datetime, sla_hours, request_id, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                shipment.shipment_id,
                from_location.id,
                to_location.id,
                shipment.consignee_name,
                shipment.material_code,
                shipment.material_count,
                shipment.load_kg,
                shipment.volume_cbm,
                shipment.priority,
                shipment.placement_datetime,
                shipment.sla_hours,
                shipment.request_id,
                shipment.status
            ))
            shipment.id = cursor.fetchone()[0]
            self.db.commit()
        return shipment

    def get_or_create_location(self, location: Location) -> Location:
        """Get existing location or create new one"""
        query = "SELECT id FROM locations WHERE code = %s;"
        with self.db.cursor() as cursor:
            cursor.execute(query, (location.code,))
            result = cursor.fetchone()
            if result:
                location.id = result[0]
                return location
            return self.create_location(location)

    def get_shipment_by_id(self, shipment_id: str) -> Optional[Shipment]:
        """Get shipment details by shipment_id"""
        query = """
        SELECT * FROM shipment_details
        WHERE shipment_id = %s;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (shipment_id,))
            result = cursor.fetchone()
            if result:
                return self._map_row_to_shipment(result)
        return None

    def get_shipments_by_status(self, status: str) -> List[Shipment]:
        """Get all shipments with a specific status"""
        query = """
        SELECT * FROM shipment_details
        WHERE status = %s
        ORDER BY created_at DESC;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (status,))
            return [self._map_row_to_shipment(row) for row in cursor.fetchall()]

    def update_shipment_status(self, shipment_id: str, status: str) -> bool:
        """Update shipment status"""
        if status not in ['pending', 'in_progress', 'completed', 'cancelled']:
            raise ValueError("Invalid status")

        query = """
        UPDATE shipments
        SET status = %s
        WHERE shipment_id = %s;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (status, shipment_id))
            rows_affected = cursor.rowcount
            self.db.commit()
            return rows_affected > 0

    def _map_row_to_shipment(self, row: Dict[str, Any]) -> Shipment:
        """Map database row to Shipment object"""
        from_location = Location(
            name=row['from_location'],
            city=row['from_city'],
            latitude=row['from_latitude'],
            longitude=row['from_longitude']
        )
        
        to_location = Location(
            name=row['to_location'],
            city=row['to_city'],
            latitude=row['to_latitude'],
            longitude=row['to_longitude']
        )

        return Shipment(
            shipment_id=row['shipment_id'],
            from_location=from_location,
            to_location=to_location,
            consignee_name=row['consignee_name'],
            material_code=row['material_code'],
            material_count=row['material_count'],
            load_kg=row['load_kg'],
            volume_cbm=row['volume_cbm'],
            priority=row['priority'],
            placement_datetime=row['placement_datetime'],
            sla_hours=row['sla_hours'],
            status=row['status']
        ) 