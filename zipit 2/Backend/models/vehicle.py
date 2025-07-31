from datetime import datetime, time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class VehicleType:
    type_code: str
    name: str
    description: Optional[str] = None
    id: Optional[int] = None

@dataclass
class VehicleCapacity:
    weight_capacity_kg: Decimal
    volume_capacity_cbm: Decimal
    min_weight_utilization: Decimal
    min_volume_utilization: Decimal
    vehicle_type_id: Optional[int] = None
    id: Optional[int] = None

@dataclass
class ServiceWindow:
    day_of_week: int
    start_time: time
    end_time: time
    vehicle_id: Optional[int] = None
    id: Optional[int] = None

@dataclass
class Vehicle:
    vehicle_id: str
    vehicle_type: VehicleType
    from_city: str
    to_city: str
    registration_number: Optional[str] = None
    cost_per_km: Optional[Decimal] = None
    fixed_cost: Optional[Decimal] = None
    min_speed_kmph: Optional[int] = None
    max_speed_kmph: Optional[int] = None
    break_time_minutes: Optional[int] = None
    max_continuous_driving_time_minutes: Optional[int] = None
    status: str = 'available'
    service_windows: List[ServiceWindow] = None
    capacity: Optional[VehicleCapacity] = None
    id: Optional[int] = None

class VehicleRepository:
    def __init__(self, db_connection):
        self.db = db_connection

    def create_vehicle_type(self, vehicle_type: VehicleType) -> VehicleType:
        """Create a new vehicle type"""
        query = """
        INSERT INTO vehicle_types (type_code, name, description)
        VALUES (%s, %s, %s)
        RETURNING id;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                vehicle_type.type_code,
                vehicle_type.name,
                vehicle_type.description
            ))
            vehicle_type.id = cursor.fetchone()[0]
            self.db.commit()
        return vehicle_type

    def create_vehicle_capacity(self, capacity: VehicleCapacity) -> VehicleCapacity:
        """Create vehicle capacity"""
        if not (0 <= capacity.min_weight_utilization <= 100 and 0 <= capacity.min_volume_utilization <= 100):
            raise ValueError("Utilization percentages must be between 0 and 100")

        query = """
        INSERT INTO vehicle_capacities (
            vehicle_type_id, weight_capacity_kg, volume_capacity_cbm,
            min_weight_utilization, min_volume_utilization
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                capacity.vehicle_type_id,
                capacity.weight_capacity_kg,
                capacity.volume_capacity_cbm,
                capacity.min_weight_utilization,
                capacity.min_volume_utilization
            ))
            capacity.id = cursor.fetchone()[0]
            self.db.commit()
        return capacity

    def create_vehicle(self, vehicle: Vehicle) -> Vehicle:
        """Create a new vehicle with validations"""
        # Validate required fields
        if not all([
            vehicle.vehicle_id,
            vehicle.vehicle_type,
            vehicle.from_city,
            vehicle.to_city
        ]):
            raise ValueError("Missing required fields")

        # Validate cities are different
        if vehicle.from_city.lower() == vehicle.to_city.lower():
            raise ValueError("From and To cities cannot be the same")

        # Validate speed constraints
        if vehicle.min_speed_kmph and vehicle.max_speed_kmph:
            if vehicle.min_speed_kmph >= vehicle.max_speed_kmph:
                raise ValueError("Maximum speed must be greater than minimum speed")

        # Create or get vehicle type
        vehicle_type = self.get_or_create_vehicle_type(vehicle.vehicle_type)
        
        # Create vehicle capacity if provided
        if vehicle.capacity:
            vehicle.capacity.vehicle_type_id = vehicle_type.id
            self.create_vehicle_capacity(vehicle.capacity)

        query = """
        INSERT INTO vehicles (
            vehicle_id, vehicle_type_id, registration_number, from_city, to_city,
            cost_per_km, fixed_cost, min_speed_kmph, max_speed_kmph,
            break_time_minutes, max_continuous_driving_time_minutes, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                vehicle.vehicle_id,
                vehicle_type.id,
                vehicle.registration_number,
                vehicle.from_city.lower(),
                vehicle.to_city.lower(),
                vehicle.cost_per_km,
                vehicle.fixed_cost,
                vehicle.min_speed_kmph,
                vehicle.max_speed_kmph,
                vehicle.break_time_minutes,
                vehicle.max_continuous_driving_time_minutes,
                vehicle.status
            ))
            vehicle.id = cursor.fetchone()[0]
            self.db.commit()

        # Create service windows if provided
        if vehicle.service_windows:
            for window in vehicle.service_windows:
                window.vehicle_id = vehicle.id
                self.create_service_window(window)

        return vehicle

    def create_service_window(self, window: ServiceWindow) -> ServiceWindow:
        """Create a service time window for a vehicle"""
        if not (0 <= window.day_of_week <= 6):
            raise ValueError("Day of week must be between 0 and 6")

        if window.start_time >= window.end_time:
            raise ValueError("End time must be after start time")

        query = """
        INSERT INTO vehicle_service_windows (
            vehicle_id, day_of_week, start_time, end_time
        )
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (
                window.vehicle_id,
                window.day_of_week,
                window.start_time,
                window.end_time
            ))
            window.id = cursor.fetchone()[0]
            self.db.commit()
        return window

    def get_or_create_vehicle_type(self, vehicle_type: VehicleType) -> VehicleType:
        """Get existing vehicle type or create new one"""
        query = "SELECT id FROM vehicle_types WHERE type_code = %s;"
        with self.db.cursor() as cursor:
            cursor.execute(query, (vehicle_type.type_code,))
            result = cursor.fetchone()
            if result:
                vehicle_type.id = result[0]
                return vehicle_type
            return self.create_vehicle_type(vehicle_type)

    def get_vehicle_by_id(self, vehicle_id: str) -> Optional[Vehicle]:
        """Get vehicle details by vehicle_id"""
        query = """
        SELECT * FROM vehicle_details
        WHERE vehicle_id = %s;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (vehicle_id,))
            result = cursor.fetchone()
            if result:
                return self._map_row_to_vehicle(result)
        return None

    def get_vehicles_by_route(self, from_city: str, to_city: str) -> List[Vehicle]:
        """Get all vehicles available for a specific route"""
        query = """
        SELECT * FROM vehicle_details
        WHERE from_city = %s AND to_city = %s AND status = 'available'
        ORDER BY created_at DESC;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (from_city.lower(), to_city.lower()))
            return [self._map_row_to_vehicle(row) for row in cursor.fetchall()]

    def update_vehicle_status(self, vehicle_id: str, status: str) -> bool:
        """Update vehicle status"""
        if status not in ['available', 'in_transit', 'maintenance', 'inactive']:
            raise ValueError("Invalid status")

        query = """
        UPDATE vehicles
        SET status = %s
        WHERE vehicle_id = %s;
        """
        with self.db.cursor() as cursor:
            cursor.execute(query, (status, vehicle_id))
            rows_affected = cursor.rowcount
            self.db.commit()
            return rows_affected > 0

    def _map_row_to_vehicle(self, row: Dict[str, Any]) -> Vehicle:
        """Map database row to Vehicle object"""
        vehicle_type = VehicleType(
            type_code=row['vehicle_type'],
            name=row['vehicle_type_name']
        )

        capacity = VehicleCapacity(
            weight_capacity_kg=row['weight_capacity_kg'],
            volume_capacity_cbm=row['volume_capacity_cbm'],
            min_weight_utilization=row['min_weight_utilization'],
            min_volume_utilization=row['min_volume_utilization']
        )

        return Vehicle(
            vehicle_id=row['vehicle_id'],
            vehicle_type=vehicle_type,
            registration_number=row['registration_number'],
            from_city=row['from_city'],
            to_city=row['to_city'],
            cost_per_km=row['cost_per_km'],
            fixed_cost=row['fixed_cost'],
            min_speed_kmph=row['min_speed_kmph'],
            max_speed_kmph=row['max_speed_kmph'],
            break_time_minutes=row['break_time_minutes'],
            max_continuous_driving_time_minutes=row['max_continuous_driving_time_minutes'],
            status=row['status'],
            capacity=capacity
        ) 