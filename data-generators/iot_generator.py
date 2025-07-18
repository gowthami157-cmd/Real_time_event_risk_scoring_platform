"""
IoT telemetry data generator with anomaly patterns.
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List
from faker import Faker
import json
import math

fake = Faker()

class IoTGenerator:
    """Generates realistic IoT telemetry data with anomaly patterns."""
    
    def __init__(self, seed: int = 42):
        """Initialize the IoT generator."""
        random.seed(seed)
        fake.seed_instance(seed)
        
        # Device types and their sensor configurations
        self.device_types = {
            'temperature_sensor': {
                'sensors': ['temperature', 'humidity'],
                'normal_ranges': {'temperature': (18, 25), 'humidity': (40, 60)},
                'anomaly_probability': 0.05,
                'criticality': 'medium'
            },
            'pressure_sensor': {
                'sensors': ['pressure', 'flow_rate'],
                'normal_ranges': {'pressure': (950, 1050), 'flow_rate': (10, 50)},
                'anomaly_probability': 0.03,
                'criticality': 'high'
            },
            'vibration_sensor': {
                'sensors': ['vibration_x', 'vibration_y', 'vibration_z'],
                'normal_ranges': {'vibration_x': (0, 5), 'vibration_y': (0, 5), 'vibration_z': (0, 5)},
                'anomaly_probability': 0.08,
                'criticality': 'high'
            },
            'environmental_sensor': {
                'sensors': ['co2', 'air_quality', 'noise_level'],
                'normal_ranges': {'co2': (350, 1000), 'air_quality': (0, 50), 'noise_level': (30, 70)},
                'anomaly_probability': 0.06,
                'criticality': 'medium'
            },
            'power_meter': {
                'sensors': ['voltage', 'current', 'power_consumption'],
                'normal_ranges': {'voltage': (220, 240), 'current': (0, 20), 'power_consumption': (0, 5000)},
                'anomaly_probability': 0.04,
                'criticality': 'high'
            },
            'security_sensor': {
                'sensors': ['motion_detected', 'door_status', 'tamper_alert'],
                'normal_ranges': {'motion_detected': (0, 1), 'door_status': (0, 1), 'tamper_alert': (0, 1)},
                'anomaly_probability': 0.02,
                'criticality': 'critical'
            }
        }
        
        # Anomaly patterns
        self.anomaly_patterns = {
            'spike': 'Sudden spike in sensor values',
            'drift': 'Gradual drift from normal range',
            'flatline': 'Sensor readings become constant',
            'noise': 'Excessive noise in sensor readings',
            'missing': 'Missing sensor readings',
            'correlation_break': 'Normal correlation between sensors breaks'
        }
        
        # Generate device pool
        self.device_pool = self._generate_device_pool(5000)
        
        # Maintain state for realistic time series
        self.device_states = {}
    
    def _generate_device_pool(self, size: int) -> List[Dict[str, Any]]:
        """Generate a pool of IoT devices with different characteristics."""
        devices = []
        
        for _ in range(size):
            device_type = random.choice(list(self.device_types.keys()))
            device_info = self.device_types[device_type]
            
            device = {
                'device_id': str(uuid.uuid4()),
                'device_type': device_type,
                'location': fake.address(),
                'installation_date': fake.date_between(start_date='-5y', end_date='today'),
                'firmware_version': f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
                'manufacturer': fake.company(),
                'model': fake.bothify(text='MODEL-####'),
                'criticality': device_info['criticality'],
                'maintenance_due': random.random() < 0.1,  # 10% due for maintenance
                'is_faulty': random.random() < 0.05,  # 5% faulty devices
                'network_quality': random.choice(['excellent', 'good', 'fair', 'poor']),
                'battery_level': random.randint(10, 100) if random.random() < 0.3 else None,
                'last_maintenance': fake.date_between(start_date='-2y', end_date='today'),
                'operating_hours': random.randint(1000, 50000),
            }
            devices.append(device)
        
        return devices
    
    def generate_telemetry(self, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a single IoT telemetry event."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Select device from pool
        device = random.choice(self.device_pool)
        device_type_info = self.device_types[device['device_type']]
        
        # Initialize device state if not exists
        if device['device_id'] not in self.device_states:
            self.device_states[device['device_id']] = {
                'last_readings': {},
                'anomaly_start': None,
                'anomaly_type': None,
                'anomaly_duration': 0
            }
        
        device_state = self.device_states[device['device_id']]
        
        # Determine if anomaly should occur
        is_anomaly = (device['is_faulty'] or 
                     random.random() < device_type_info['anomaly_probability'])
        
        # Generate sensor readings
        sensor_readings = {}
        for sensor in device_type_info['sensors']:
            if sensor in ['motion_detected', 'door_status', 'tamper_alert']:
                # Binary sensors
                if is_anomaly and sensor == 'tamper_alert':
                    reading = 1
                else:
                    reading = random.choice([0, 1])
            else:
                # Continuous sensors
                reading = self._generate_sensor_reading(
                    sensor, device_type_info['normal_ranges'][sensor], 
                    device_state, is_anomaly
                )
            
            sensor_readings[sensor] = reading
        
        # Calculate risk score
        risk_score = self._calculate_risk_score(sensor_readings, device, device_type_info)
        
        # Determine anomaly type if anomaly is detected
        anomaly_type = None
        if is_anomaly:
            anomaly_type = random.choice(list(self.anomaly_patterns.keys()))
            if device_state['anomaly_start'] is None:
                device_state['anomaly_start'] = timestamp
                device_state['anomaly_type'] = anomaly_type
            device_state['anomaly_duration'] += 1
        else:
            device_state['anomaly_start'] = None
            device_state['anomaly_type'] = None
            device_state['anomaly_duration'] = 0
        
        # Update device state
        device_state['last_readings'] = sensor_readings
        
        telemetry = {
            'telemetry_id': str(uuid.uuid4()),
            'device_id': device['device_id'],
            'timestamp': timestamp.isoformat(),
            'event_type': 'iot_telemetry',
            'device_type': device['device_type'],
            'location': device['location'],
            'criticality': device['criticality'],
            'risk_score': round(risk_score, 3),
            
            # Device metadata
            'firmware_version': device['firmware_version'],
            'manufacturer': device['manufacturer'],
            'model': device['model'],
            'battery_level': device['battery_level'],
            'network_quality': device['network_quality'],
            'operating_hours': device['operating_hours'],
            'maintenance_due': device['maintenance_due'],
            'is_faulty': device['is_faulty'],
            
            # Sensor readings
            'sensor_readings': sensor_readings,
            
            # Anomaly information
            'is_anomaly': is_anomaly,
            'anomaly_type': anomaly_type,
            'anomaly_duration': device_state['anomaly_duration'],
            'anomaly_severity': self._calculate_anomaly_severity(risk_score),
            
            # Quality metrics
            'signal_strength': random.randint(-90, -30),  # dBm
            'packet_loss': random.uniform(0, 0.1) if device['network_quality'] == 'poor' else random.uniform(0, 0.01),
            'latency_ms': random.randint(50, 500) if device['network_quality'] == 'poor' else random.randint(10, 100),
            'data_integrity_score': random.uniform(0.95, 1.0) if not is_anomaly else random.uniform(0.7, 0.95),
            
            # Operational metrics
            'cpu_usage': random.uniform(0.1, 0.9),
            'memory_usage': random.uniform(0.2, 0.8),
            'temperature': random.uniform(20, 70),  # Device temperature
            'uptime_hours': random.randint(1, 8760),
            
            # Alert information
            'alert_level': self._get_alert_level(risk_score),
            'requires_attention': risk_score > 0.7,
            'maintenance_required': device['maintenance_due'] or risk_score > 0.8,
        }
        
        return telemetry
    
    def _generate_sensor_reading(self, sensor: str, normal_range: tuple, 
                               device_state: Dict[str, Any], is_anomaly: bool) -> float:
        """Generate a realistic sensor reading."""
        min_val, max_val = normal_range
        
        # Get last reading for continuity
        last_reading = device_state['last_readings'].get(sensor, 
                                                        (min_val + max_val) / 2)
        
        if is_anomaly:
            # Generate anomalous reading
            anomaly_type = device_state.get('anomaly_type', 'spike')
            
            if anomaly_type == 'spike':
                # Sudden spike
                reading = last_reading + random.uniform(max_val - min_val, 
                                                       2 * (max_val - min_val))
            elif anomaly_type == 'drift':
                # Gradual drift
                drift = random.uniform(0.1, 0.3) * (max_val - min_val)
                reading = last_reading + drift
            elif anomaly_type == 'flatline':
                # Constant reading
                reading = last_reading
            elif anomaly_type == 'noise':
                # Excessive noise
                noise = random.uniform(-0.5, 0.5) * (max_val - min_val)
                reading = last_reading + noise
            else:
                # Default to spike
                reading = last_reading + random.uniform(max_val - min_val, 
                                                       2 * (max_val - min_val))
        else:
            # Generate normal reading with some variation
            # Add some continuity with previous reading
            continuity_factor = 0.3
            random_factor = 0.7
            
            target = random.uniform(min_val, max_val)
            reading = (continuity_factor * last_reading + 
                      random_factor * target)
            
            # Add small random variation
            variation = random.uniform(-0.05, 0.05) * (max_val - min_val)
            reading += variation
        
        return round(reading, 2)
    
    def _calculate_risk_score(self, sensor_readings: Dict[str, float], 
                            device: Dict[str, Any], 
                            device_type_info: Dict[str, Any]) -> float:
        """Calculate risk score based on sensor readings and device state."""
        risk_score = 0.0
        
        # Check each sensor against normal ranges
        for sensor, reading in sensor_readings.items():
            if sensor in device_type_info['normal_ranges']:
                min_val, max_val = device_type_info['normal_ranges'][sensor]
                
                if reading < min_val or reading > max_val:
                    # Out of range
                    if reading < min_val:
                        deviation = (min_val - reading) / (max_val - min_val)
                    else:
                        deviation = (reading - max_val) / (max_val - min_val)
                    
                    risk_score += min(deviation, 1.0) * 0.3
        
        # Device-specific risk factors
        if device['is_faulty']:
            risk_score += 0.4
        
        if device['maintenance_due']:
            risk_score += 0.2
        
        # Network quality impact
        network_risk = {
            'excellent': 0.0, 'good': 0.05, 'fair': 0.1, 'poor': 0.2
        }[device['network_quality']]
        risk_score += network_risk
        
        # Battery level impact
        if device['battery_level'] is not None and device['battery_level'] < 20:
            risk_score += 0.15
        
        # Criticality impact
        criticality_multiplier = {
            'low': 0.8, 'medium': 1.0, 'high': 1.2, 'critical': 1.5
        }[device['criticality']]
        risk_score *= criticality_multiplier
        
        return max(0.0, min(1.0, risk_score))
    
    def _calculate_anomaly_severity(self, risk_score: float) -> str:
        """Calculate anomaly severity based on risk score."""
        if risk_score < 0.3:
            return 'low'
        elif risk_score < 0.6:
            return 'medium'
        elif risk_score < 0.8:
            return 'high'
        else:
            return 'critical'
    
    def _get_alert_level(self, risk_score: float) -> str:
        """Get alert level based on risk score."""
        if risk_score < 0.4:
            return 'info'
        elif risk_score < 0.7:
            return 'warning'
        else:
            return 'critical'
    
    def generate_batch(self, batch_size: int, 
                      start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate a batch of telemetry events."""
        if start_time is None:
            start_time = datetime.utcnow()
        
        telemetry_events = []
        
        for i in range(batch_size):
            # Add temporal variation
            timestamp = start_time + timedelta(
                seconds=random.randint(0, 60),
                microseconds=random.randint(0, 999999)
            )
            
            telemetry = self.generate_telemetry(timestamp)
            telemetry_events.append(telemetry)
        
        return telemetry_events
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the JSON schema for generated telemetry."""
        return {
            "type": "object",
            "properties": {
                "telemetry_id": {"type": "string"},
                "device_id": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
                "event_type": {"type": "string"},
                "device_type": {"type": "string"},
                "location": {"type": "string"},
                "criticality": {"type": "string"},
                "risk_score": {"type": "number"},
                "sensor_readings": {"type": "object"},
                "is_anomaly": {"type": "boolean"},
                "anomaly_type": {"type": "string"},
                "anomaly_severity": {"type": "string"},
                "alert_level": {"type": "string"},
                "requires_attention": {"type": "boolean"},
                "maintenance_required": {"type": "boolean"}
            },
            "required": ["telemetry_id", "device_id", "timestamp", "event_type", "device_type", "risk_score"]
        }