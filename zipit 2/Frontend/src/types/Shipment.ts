export interface Location {
    id: string;
    code: string;
    name: string;
    address: string;
    latitude: number;
    longitude: number;
}

export interface Shipment {
    id: string;
    label: string;
    fromLocation: Location;
    toLocation: Location;
    consignee: string;
    weight: number;
    volume: number;
    sla: string;
    priority: 'high' | 'medium' | 'low';
    serviceTime?: number;
    vehicleTypePreference?: string;
    specialHandlingRequirements?: string[];
    deliveryTimeWindow: {
        start: string;
        end: string;
    };
}

export interface ShipmentResponse {
    data: Shipment[];
    total: number;
    page: number;
    limit: number;
} 