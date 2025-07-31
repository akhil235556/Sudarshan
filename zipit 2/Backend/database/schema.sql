-- Create the locations table to store location information
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    code VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 8) CHECK (latitude >= -90 AND latitude <= 90),
    longitude DECIMAL(11, 8) CHECK (longitude >= -180 AND longitude <= 180),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code),
    CONSTRAINT valid_coordinates CHECK (
        (latitude IS NULL AND longitude IS NULL) OR
        (latitude IS NOT NULL AND longitude IS NOT NULL)
    )
);

-- Create the materials table
CREATE TABLE materials (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code)
);

-- Create the shipments table with all validations from the task validator
CREATE TABLE shipments (
    id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(50) NOT NULL UNIQUE,
    from_location_id INTEGER NOT NULL REFERENCES locations(id),
    to_location_id INTEGER NOT NULL REFERENCES locations(id),
    consignee_name VARCHAR(255),
    material_code VARCHAR(50) NOT NULL REFERENCES materials(code),
    material_count INTEGER NOT NULL CHECK (material_count > 0),
    load_kg DECIMAL(10, 2) CHECK (load_kg > 0),
    volume_cbm DECIMAL(10, 2) CHECK (volume_cbm > 0),
    priority VARCHAR(20) CHECK (priority IN ('high', 'medium', 'low')),
    placement_datetime TIMESTAMP,
    sla_hours INTEGER CHECK (sla_hours > 0),
    request_id UUID NOT NULL,
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed', 'cancelled')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT different_locations CHECK (from_location_id != to_location_id),
    CONSTRAINT load_or_volume_required CHECK (load_kg IS NOT NULL OR volume_cbm IS NOT NULL)
);

-- Create indexes for better query performance
CREATE INDEX idx_shipments_shipment_id ON shipments(shipment_id);
CREATE INDEX idx_shipments_from_location ON shipments(from_location_id);
CREATE INDEX idx_shipments_to_location ON shipments(to_location_id);
CREATE INDEX idx_shipments_status ON shipments(status);
CREATE INDEX idx_shipments_request_id ON shipments(request_id);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update the updated_at column
CREATE TRIGGER update_shipments_updated_at
    BEFORE UPDATE ON shipments
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_locations_updated_at
    BEFORE UPDATE ON locations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_materials_updated_at
    BEFORE UPDATE ON materials
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for easy shipment querying with location details
CREATE VIEW shipment_details AS
SELECT 
    s.shipment_id,
    fl.name as from_location,
    fl.city as from_city,
    fl.latitude as from_latitude,
    fl.longitude as from_longitude,
    tl.name as to_location,
    tl.city as to_city,
    tl.latitude as to_latitude,
    tl.longitude as to_longitude,
    s.consignee_name,
    s.material_code,
    s.material_count,
    s.load_kg,
    s.volume_cbm,
    s.priority,
    s.placement_datetime,
    s.sla_hours,
    s.status,
    s.created_at,
    s.updated_at
FROM 
    shipments s
    JOIN locations fl ON s.from_location_id = fl.id
    JOIN locations tl ON s.to_location_id = tl.id; 