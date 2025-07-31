-- Create the vehicle types table
CREATE TABLE vehicle_types (
    id SERIAL PRIMARY KEY,
    type_code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(type_code)
);

-- Create the vehicle capacities table
CREATE TABLE vehicle_capacities (
    id SERIAL PRIMARY KEY,
    vehicle_type_id INTEGER NOT NULL REFERENCES vehicle_types(id),
    length_m DECIMAL(10, 2) CHECK (length_m > 0),
    width_m DECIMAL(10, 2) CHECK (width_m > 0),
    height_m DECIMAL(10, 2) CHECK (height_m > 0),
    weight_capacity_kg DECIMAL(10, 2) CHECK (weight_capacity_kg > 0),
    volume_capacity_cbm DECIMAL(10, 2) CHECK (volume_capacity_cbm > 0),
    min_weight_utilization DECIMAL(5, 2) CHECK (min_weight_utilization BETWEEN 0 AND 100),
    min_volume_utilization DECIMAL(5, 2) CHECK (min_volume_utilization BETWEEN 0 AND 100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT volume_matches_dimensions CHECK (
        ABS(volume_capacity_cbm - (length_m * width_m * height_m)) < 0.01
    )
);

-- Create the vehicles table
CREATE TABLE vehicles (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL UNIQUE,
    vehicle_type_id INTEGER NOT NULL REFERENCES vehicle_types(id),
    registration_number VARCHAR(50),
    from_city VARCHAR(100) NOT NULL,
    to_city VARCHAR(100) NOT NULL,
    cost_per_km DECIMAL(10, 2) CHECK (cost_per_km > 0),
    fixed_cost DECIMAL(10, 2) CHECK (fixed_cost >= 0),
    min_speed_kmph INTEGER CHECK (min_speed_kmph > 0),
    max_speed_kmph INTEGER CHECK (max_speed_kmph > min_speed_kmph),
    break_time_minutes INTEGER CHECK (break_time_minutes >= 0),
    max_continuous_driving_time_minutes INTEGER CHECK (max_continuous_driving_time_minutes > 0),
    status VARCHAR(50) DEFAULT 'available' CHECK (status IN ('available', 'in_transit', 'maintenance', 'inactive')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_cities CHECK (from_city != to_city)
);

-- Create the vehicle service time windows table
CREATE TABLE vehicle_service_windows (
    id SERIAL PRIMARY KEY,
    vehicle_id INTEGER NOT NULL REFERENCES vehicles(id),
    day_of_week INTEGER CHECK (day_of_week BETWEEN 0 AND 6),
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_time_window CHECK (start_time < end_time)
);

-- Create indexes for better query performance
CREATE INDEX idx_vehicles_type ON vehicles(vehicle_type_id);
CREATE INDEX idx_vehicles_cities ON vehicles(from_city, to_city);
CREATE INDEX idx_vehicles_status ON vehicles(status);
CREATE INDEX idx_service_windows_vehicle ON vehicle_service_windows(vehicle_id);
CREATE INDEX idx_service_windows_day ON vehicle_service_windows(day_of_week);

-- Create triggers for updated_at timestamps
CREATE TRIGGER update_vehicle_types_updated_at
    BEFORE UPDATE ON vehicle_types
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicle_capacities_updated_at
    BEFORE UPDATE ON vehicle_capacities
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicles_updated_at
    BEFORE UPDATE ON vehicles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicle_service_windows_updated_at
    BEFORE UPDATE ON vehicle_service_windows
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for easy vehicle querying with all details
CREATE OR REPLACE VIEW vehicle_details AS
SELECT 
    v.vehicle_id,
    vt.type_code as vehicle_type,
    vt.name as vehicle_type_name,
    v.registration_number,
    v.from_city,
    v.to_city,
    vc.length_m,
    vc.width_m,
    vc.height_m,
    vc.weight_capacity_kg,
    vc.volume_capacity_cbm,
    vc.min_weight_utilization,
    vc.min_volume_utilization,
    v.cost_per_km,
    v.fixed_cost,
    v.min_speed_kmph,
    v.max_speed_kmph,
    v.break_time_minutes,
    v.max_continuous_driving_time_minutes,
    v.status,
    v.created_at,
    v.updated_at
FROM 
    vehicles v
    JOIN vehicle_types vt ON v.vehicle_type_id = vt.id
    JOIN vehicle_capacities vc ON vt.id = vc.vehicle_type_id; 