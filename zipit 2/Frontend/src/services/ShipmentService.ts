import axios from 'axios';
import { Shipment, ShipmentResponse } from '../types/Shipment';

const BASE_URL = '/api/optimize/sequential-mid-mile';

export const ShipmentService = {
    // Get shipments with pagination
    getShipments: async (page: number, limit: number): Promise<ShipmentResponse> => {
        const response = await axios.get(`${BASE_URL}/shipments`, {
            params: {
                page,
                limit
            }
        });
        return response.data;
    },

    // Upload shipments for sequential mid-mile planning
    uploadShipments: async (file: File): Promise<{ success: boolean; message: string; count: number }> => {
        try {
            const formData = new FormData();
            formData.append('file', file);

            // Upload shipments using the sequential mid-mile planner API
            const response = await axios.post(`${BASE_URL}/shipments/upload`, formData, {
                headers: {
                    'Content-Type': 'multipart/form-data'
                }
            });

            if (response.data.success) {
                // Initiate planning process with selected shipments
                await axios.post(`${BASE_URL}/plan`, {
                    planningParameters: {
                        objectiveFunction: 'MIN_COST',
                        constraints: {
                            maxUtilization: 0.85,
                            allowPartialDelivery: false,
                            enforceVehicleReturn: true,
                            balanceWorkload: true
                        },
                        sequentialParameters: {
                            batchSize: 50,
                            overlapWindow: 30, // minutes
                            priorityRules: [
                                { type: 'PRIORITY', weight: 0.7 },
                                { type: 'DEADLINE', weight: 0.3 }
                            ]
                        },
                        timeWindow: {
                            start: new Date().toISOString(),
                            end: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
                        }
                    }
                });

                return {
                    success: true,
                    message: 'Shipments uploaded and planning initiated successfully',
                    count: response.data.count || 0
                };
            }

            return {
                success: false,
                message: response.data.message || 'Upload failed',
                count: 0
            };
        } catch (error: any) {
            console.error('Error uploading shipments:', error);
            return {
                success: false,
                message: error.response?.data?.message || 'Error uploading shipments',
                count: 0
            };
        }
    }
}; 