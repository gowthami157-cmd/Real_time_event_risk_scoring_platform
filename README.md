# Real-Time Event Risk Scoring Platform

A high-performance streaming ML platform for real-time risk scoring of insurance claims, financial transactions, and IoT telemetry data.

## 🚀 Features

### Step 1: Foundation & Data Simulation
- **Multi-source data generators** for realistic event simulation
- **High-throughput Kafka producer** supporting 50K+ messages/second
- **Comprehensive Docker infrastructure** with all necessary services
- **Realistic risk patterns** including fraud detection and anomaly simulation

### Data Sources
- **Insurance Claims**: Realistic claim patterns with fraud indicators
- **Financial Transactions**: Transaction data with fraud patterns and velocity checks
- **IoT Telemetry**: Sensor data with anomaly detection and device health monitoring

### Infrastructure
- **Kafka Cluster**: High-throughput message streaming with optimized partitioning
- **Redis**: Feature caching and real-time storage
- **InfluxDB**: Time-series data storage for telemetry
- **Elasticsearch**: Event logging and search capabilities
- **Grafana**: Real-time monitoring and visualization

## 🛠️ Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- 8GB+ RAM recommended

### 1. Start Infrastructure
```bash
# Start all services
docker-compose up -d

# Wait for services to be ready (takes ~2-3 minutes)
docker-compose logs -f kafka
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Data Generation
```bash
# Generate 50K events/second
python data-generators/producer.py --throughput 50000

# Generate for specific duration
python data-generators/producer.py --throughput 25000 --duration 300
```

### 4. Monitor Performance
- **Kafka**: http://localhost:9092
- **Grafana**: http://localhost:3000 (admin/admin)
- **Kibana**: http://localhost:5601
- **InfluxDB**: http://localhost:8086

## 📊 Data Generation Details

### Insurance Claims
- **Volume**: ~35% of total events
- **Features**: 25+ risk-relevant attributes
- **Patterns**: Fraud indicators, seasonal trends, location-based risk
- **Risk Factors**: Weather, location, user history, claim amount

### Financial Transactions
- **Volume**: ~40% of total events
- **Features**: 20+ transaction attributes
- **Patterns**: Card testing, account takeover, synthetic identity fraud
- **Risk Factors**: Velocity, location, merchant type, user behavior

### IoT Telemetry
- **Volume**: ~25% of total events
- **Device Types**: Temperature, pressure, vibration, environmental sensors
- **Patterns**: Sensor drift, spikes, flatlines, correlation breaks
- **Risk Factors**: Device health, maintenance due, criticality level

## 🔧 Configuration

### Kafka Configuration
```python
# High-throughput producer settings
PRODUCER_CONFIG = {
    'batch.size': 65536,        # 64KB batches
    'linger.ms': 10,            # 10ms batching delay
    'compression.type': 'snappy',
    'buffer.memory': 67108864,  # 64MB buffer
    'max.in.flight.requests.per.connection': 5,
    'acks': '1'                 # Wait for leader ack
}
```

### Topic Configuration
- **insurance-claims**: 12 partitions, 7-day retention
- **financial-transactions**: 12 partitions, 7-day retention
- **iot-telemetry**: 8 partitions, 7-day retention
- **risk-scores**: 6 partitions, 7-day retention

## 📈 Performance Metrics

### Target Performance
- **Throughput**: 50,000+ events/second
- **Latency**: <85ms p95 for risk scoring
- **Availability**: 99.9% uptime
- **Data Quality**: >99% message delivery success

### Monitoring
- **Real-time throughput** monitoring
- **Message delivery statistics**
- **Error rates and failed messages**
- **Resource utilization metrics**

## 🔍 Event Schemas

### Insurance Claim Event
```json
{
  "claim_id": "uuid",
  "user_id": "uuid",
  "timestamp": "ISO8601",
  "event_type": "insurance_claim",
  "claim_type": "auto|property|health|life",
  "claim_amount": 8500.00,
  "risk_score": 0.65,
  "fraud_probability": 0.23,
  "fraud_indicators": ["unusual_claim_amount"],
  "location": "Miami, FL",
  "priority": "high|medium|low"
}
```

### Financial Transaction Event
```json
{
  "transaction_id": "uuid",
  "user_id": "uuid",
  "timestamp": "ISO8601",
  "event_type": "financial_transaction",
  "amount": 150.00,
  "category": "retail",
  "merchant_name": "Example Store",
  "location": "New York, NY",
  "is_fraud": false,
  "risk_score": 0.15,
  "payment_method": "credit_card"
}
```

### IoT Telemetry Event
```json
{
  "telemetry_id": "uuid",
  "device_id": "uuid",
  "timestamp": "ISO8601",
  "event_type": "iot_telemetry",
  "device_type": "temperature_sensor",
  "sensor_readings": {
    "temperature": 23.5,
    "humidity": 45.2
  },
  "risk_score": 0.12,
  "is_anomaly": false,
  "alert_level": "info"
}
```

## 🚦 Next Steps

This foundation sets up the data generation and streaming infrastructure. Upcoming steps will include:

1. **Stream Processing**: Kafka + Spark Structured Streaming
2. **Feature Store**: Feast integration for feature materialization
3. **ML Pipeline**: Real-time risk scoring models
4. **API Layer**: FastAPI for low-latency predictions
5. **MLOps**: Model deployment, monitoring, and retraining

## 🛡️ Security & Compliance

- **Data Privacy**: No PII in generated data
- **Encryption**: TLS for all inter-service communication
- **Authentication**: Service-to-service authentication
- **Audit Logging**: Complete event audit trail

## 📋 Troubleshooting

### Common Issues
1. **Out of Memory**: Increase Docker memory allocation
2. **Slow Startup**: Wait for Kafka to initialize completely
3. **High CPU**: Reduce producer throughput temporarily
4. **Disk Space**: Configure log retention policies

### Performance Tuning
```bash
# Monitor Kafka performance
docker exec kafka kafka-run-class.sh kafka.tools.ConsumerPerformance \
  --broker-list localhost:9092 --topic financial-transactions \
  --messages 100000

# Check producer lag
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups
```

## 📖 Documentation

- [Kafka Configuration Guide](docs/kafka-setup.md)
- [Data Generator Reference](docs/data-generators.md)
- [Performance Tuning](docs/performance.md)
- [Monitoring Setup](docs/monitoring.md)

---

**Built with**: Kafka, Python, Docker, Redis, InfluxDB, Elasticsearch, Grafana
**License**: MIT
**Status**: Step 1 Complete ✅