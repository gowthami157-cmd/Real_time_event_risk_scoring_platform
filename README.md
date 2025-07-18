# Real-Time Event Risk Scoring Platform

A high-performance streaming ML platform for real-time risk scoring of insurance claims, financial transactions, and IoT telemetry data with Spark Structured Streaming.

## 🚀 Features

### Step 1: Foundation & Data Simulation ✅
- **Multi-source data generators** for realistic event simulation
- **High-throughput Kafka producer** supporting 50K+ messages/second
- **Comprehensive Docker infrastructure** with all necessary services
- **Realistic risk patterns** including fraud detection and anomaly simulation

### Step 2: Streaming Processing with Spark ✅
- **Spark Structured Streaming** for real-time event processing
- **Advanced feature engineering** with rolling aggregations
- **Multi-stream processing** handling all event types in parallel
- **Watermark handling** for late-arriving data
- **Feature materialization** to Feast feature store

### Data Sources
- **Insurance Claims**: Realistic claim patterns with fraud indicators
- **Financial Transactions**: Transaction data with fraud patterns and velocity checks
- **IoT Telemetry**: Sensor data with anomaly detection and device health monitoring

### Infrastructure
- **Kafka Cluster**: High-throughput message streaming with optimized partitioning
- **Apache Spark**: Distributed stream processing with Structured Streaming
- **Redis**: Feature caching and real-time storage
- **InfluxDB**: Time-series data storage for telemetry
- **Elasticsearch**: Event logging and search capabilities
- **Grafana**: Real-time monitoring and visualization
- **Feast**: Feature store for ML feature management

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

### 3. Start Spark Streaming
```bash
# Start Spark cluster
docker-compose up -d spark-master spark-worker

# Run streaming application
cd streaming
chmod +x spark-submit.sh
./spark-submit.sh
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
- **Spark UI**: http://localhost:8080
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

## 🔄 Stream Processing Features

### Real-Time Feature Engineering
- **Rolling Aggregations**: 1min, 5min, 1hr, 24hr, 7-day windows
- **Cross-Event Correlation**: User behavior across claims and transactions
- **Anomaly Detection**: Statistical deviation and pattern recognition
- **Risk Scoring**: Composite scores from multiple risk factors

### Advanced Windowing
- **Watermark Handling**: 2-minute tolerance for late data
- **Session Windows**: User activity session detection
- **Sliding Windows**: Overlapping time-based aggregations
- **Tumbling Windows**: Non-overlapping fixed intervals

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

### Spark Configuration
```bash
# High-throughput streaming settings
--conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints
--conf spark.sql.adaptive.enabled=true
--conf spark.executor.memory=2g
--conf spark.executor.cores=2
```

### Topic Configuration
- **insurance-claims**: 12 partitions, 7-day retention
- **financial-transactions**: 12 partitions, 7-day retention
- **iot-telemetry**: 8 partitions, 7-day retention
- **risk-scores**: 6 partitions, 7-day retention

## 📈 Performance Metrics

### Target Performance
- **Throughput**: 50,000+ events/second processing
- **Latency**: <85ms p95 for risk scoring
- **Availability**: 99.9% uptime
- **Data Quality**: >99% message delivery success

### Monitoring
- **Real-time throughput** monitoring
- **Message delivery statistics**
- **Error rates and failed messages**
- **Resource utilization metrics**

## 🧪 Testing Streaming Components

### Test Spark Streaming
```bash
# Test streaming components
cd streaming
python test_streaming.py

# Test with live Kafka data
python test_streaming.py --live-kafka
```

### Monitor Streaming Jobs
```bash
# Check Spark applications
curl http://localhost:8080/api/v1/applications

# Monitor streaming metrics
curl http://localhost:4040/api/v1/applications
```

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

## 🏗️ Feature Engineering

### Claims Features
- `claim_count_1hr`, `claim_count_24hr`: Velocity features
- `avg_claim_amount_24hr`: Historical spending patterns
- `claim_frequency_score`: Risk based on claim frequency
- `composite_risk_score`: Multi-factor risk assessment

### Transaction Features  
- `transaction_count_1hr`: Transaction velocity
- `velocity_risk_score`: Rapid transaction detection
- `amount_spike_risk`: Unusual amount detection
- `composite_fraud_score`: Multi-factor fraud assessment

### IoT Features
- `iot_anomaly_score_5min`: Short-term anomaly detection
- `sensor_deviation_score`: Statistical deviation from normal
- `composite_anomaly_score`: Multi-sensor anomaly assessment
- `predicted_failure_hours`: Predictive maintenance timing

## 📊 Feature Store Integration

### Feast Configuration
- **Online Store**: Redis for low-latency serving
- **Offline Store**: Parquet files for training
- **Feature Refresh**: 60-second materialization intervals
- **Entity Types**: Users, devices, merchants
- **Feature Versioning**: Automatic timestamp-based versioning

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

### Spark Troubleshooting
```bash
# Check Spark cluster status
curl http://localhost:8080/json/

# View Spark application logs
docker logs spark-master
docker logs spark-worker
```

## 📖 Documentation

- [Kafka Configuration Guide](docs/kafka-setup.md)
- [Data Generator Reference](docs/data-generators.md)
- [Spark Streaming Guide](docs/spark-streaming.md)
- [Performance Tuning](docs/performance.md)
- [Monitoring Setup](docs/monitoring.md)

---

**Built with**: Kafka, Spark, Python, Docker, Redis, InfluxDB, Elasticsearch, Grafana, Feast
**License**: MIT
**Status**: Step 2 Complete ✅