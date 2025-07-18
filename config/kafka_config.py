"""
Kafka configuration settings for high-throughput event streaming.
"""
import os
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class KafkaConfig:
    """Kafka configuration with production-ready settings."""
    
    # Connection settings
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    
    # Producer settings for high throughput
    producer_config: Dict[str, Any] = None
    
    # Consumer settings
    consumer_config: Dict[str, Any] = None
    
    # Topic settings
    topic_config: Dict[str, Any] = None
    
    def __post_init__(self):
        """Initialize configurations after object creation."""
        if self.producer_config is None:
            self.producer_config = {
                'bootstrap.servers': self.bootstrap_servers,
                'security.protocol': self.security_protocol,
                'compression.type': 'snappy',
                'batch.size': 65536,  # 64KB
                'linger.ms': 10,      # Wait up to 10ms for batching
                'buffer.memory': 67108864,  # 64MB
                'max.in.flight.requests.per.connection': 5,
                'retries': 3,
                'acks': '1',          # Wait for leader acknowledgment
                'enable.idempotence': True,
                'max.request.size': 1048576,  # 1MB
                'delivery.timeout.ms': 120000,
                'request.timeout.ms': 30000,
                'retry.backoff.ms': 100,
            }
        
        if self.consumer_config is None:
            self.consumer_config = {
                'bootstrap.servers': self.bootstrap_servers,
                'security.protocol': self.security_protocol,
                'group.id': 'risk-scoring-consumer',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False,
                'fetch.min.bytes': 1024,
                'fetch.max.wait.ms': 500,
                'max.partition.fetch.bytes': 1048576,  # 1MB
                'session.timeout.ms': 30000,
                'heartbeat.interval.ms': 3000,
                'max.poll.records': 1000,
                'max.poll.interval.ms': 300000,
            }
        
        if self.topic_config is None:
            self.topic_config = {
                'num.partitions': 12,
                'replication.factor': 1,
                'cleanup.policy': 'delete',
                'retention.ms': 604800000,  # 7 days
                'segment.ms': 3600000,      # 1 hour
                'compression.type': 'snappy',
                'max.message.bytes': 1048576,  # 1MB
                'min.insync.replicas': 1,
            }

# Topic definitions
TOPICS = {
    'insurance-claims': {
        'name': 'insurance-claims',
        'partitions': 12,
        'replication_factor': 1,
        'description': 'Insurance claims events for risk scoring'
    },
    'financial-transactions': {
        'name': 'financial-transactions',
        'partitions': 12,
        'replication_factor': 1,
        'description': 'Financial transaction events for fraud detection'
    },
    'iot-telemetry': {
        'name': 'iot-telemetry',
        'partitions': 8,
        'replication_factor': 1,
        'description': 'IoT sensor telemetry data'
    },
    'risk-scores': {
        'name': 'risk-scores',
        'partitions': 6,
        'replication_factor': 1,
        'description': 'Computed risk scores output'
    },
    'alerts': {
        'name': 'alerts',
        'partitions': 4,
        'replication_factor': 1,
        'description': 'High-risk event alerts'
    }
}

def get_kafka_config() -> KafkaConfig:
    """Get Kafka configuration with environment variable overrides."""
    return KafkaConfig(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        security_protocol=os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
    )

def get_producer_config() -> Dict[str, Any]:
    """Get producer configuration."""
    config = get_kafka_config()
    return config.producer_config

def get_consumer_config(group_id: str = None) -> Dict[str, Any]:
    """Get consumer configuration with optional group ID override."""
    config = get_kafka_config()
    consumer_config = config.consumer_config.copy()
    
    if group_id:
        consumer_config['group.id'] = group_id
    
    return consumer_config

def get_topic_config() -> Dict[str, Any]:
    """Get topic configuration."""
    config = get_kafka_config()
    return config.topic_config