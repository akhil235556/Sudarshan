-- Create the shipments table
CREATE TABLE shipments (
    id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(50) NOT NULL UNIQUE,
    from_location VARCHAR(100) NOT NULL,
    to_location VARCHAR(100) NOT NULL,
    consignee_name VARCHAR(255) NOT NULL,
    material_code VARCHAR(50) NOT NULL,
    material_count INTEGER NOT NULL CHECK (material_count > 0),
    weight_kg DECIMAL(10, 2) NOT NULL CHECK (weight_kg > 0),
    length_m DECIMAL(10, 2) NOT NULL CHECK (length_m > 0),
    width_m DECIMAL(10, 2) NOT NULL CHECK (width_m > 0),
    height_m DECIMAL(10, 2) NOT NULL CHECK (height_m > 0),
    volume_cbm DECIMAL(10, 2) NOT NULL CHECK (volume_cbm > 0),
    priority INTEGER NOT NULL CHECK (priority BETWEEN 1 AND 5),
    sla_hours INTEGER CHECK (sla_hours > 0),
    request_id VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'assigned', 'in_transit', 'delivered', 'cancelled')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_locations CHECK (from_location != to_location),
    CONSTRAINT valid_volume CHECK (
        ABS(volume_cbm - (length_m * width_m * height_m)) < 0.01
    )
);

-- Create indexes for better query performance
CREATE INDEX idx_shipments_status ON shipments(status);
CREATE INDEX idx_shipments_locations ON shipments(from_location, to_location);
CREATE INDEX idx_shipments_material ON shipments(material_code);
CREATE INDEX idx_shipments_created ON shipments(created_at);

-- Create trigger for updated_at timestamp
CREATE TRIGGER update_shipments_updated_at
    BEFORE UPDATE ON shipments
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column(); 