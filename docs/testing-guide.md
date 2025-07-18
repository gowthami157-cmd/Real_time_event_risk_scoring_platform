# Testing Guide

This guide provides comprehensive instructions for testing the Real-Time Event Risk Scoring Platform.

## Quick Start Testing

### 1. Run the Quick Test
```bash
# Make script executable and run
chmod +x scripts/quick-test.sh
./scripts/quick-test.sh
```

This will:
- ✅ Check prerequisites (Docker, Python)
- 🐳 Start all infrastructure services
- 🧪 Test infrastructure connectivity
- 📦 Install Python dependencies
- 🔧 Test data generators
- 🚀 Run a 30-second producer test
- 📊 Verify data in Kafka topics

## Detailed Testing

### 2. Infrastructure Testing
```bash
# Test all infrastructure components
chmod +x scripts/test-infrastructure.sh
./scripts/test-infrastructure.sh
```

Tests:
- Docker services status
- Kafka broker connectivity
- Redis connectivity
- InfluxDB health
- Elasticsearch cluster health
- Web interface accessibility

### 3. Data Generation Testing
```bash
# Test data generators independently
python3 scripts/test-data-generation.py
```

Tests:
- Claims generator functionality
- Transaction generator functionality
- IoT generator functionality
- Data quality validation
- Performance benchmarks

### 4. Load Testing

#### Basic Throughput Test
```bash
# Test 10,000 events/sec for 60 seconds
python3 scripts/test-producer-load.py --test-type throughput --throughput 10000 --duration 60
```

#### Scaling Test
```bash
# Test multiple throughput levels
python3 scripts/test-producer-load.py --test-type scaling --scaling-levels 5000 10000 25000 50000
```

#### Endurance Test
```bash
# Test stability over 10 minutes
python3 scripts/test-producer-load.py --test-type endurance --throughput 25000 --duration 600
```

## Manual Testing

### 5. Start Data Generation
```bash
# Generate 50,000 events/second
python3 data-generators/producer.py --throughput 50000

# Generate for specific duration (5 minutes)
python3 data-generators/producer.py --throughput 25000 --duration 300
```

### 6. Monitor Real-time

#### Kafka Topics
```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check topic details
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic insurance-claims

# Monitor consumer lag
docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

#### Consume Sample Messages
```bash
# Insurance claims
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic insurance-claims --from-beginning --max-messages 5

# Financial transactions
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic financial-transactions --from-beginning --max-messages 5

# IoT telemetry
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic iot-telemetry --from-beginning --max-messages 5
```

### 7. Web Interface Testing

#### Grafana (http://localhost:3000)
- Username: `admin`
- Password: `admin`
- Create dashboards for real-time monitoring
- Import Kafka and system metrics

#### Kibana (http://localhost:5601)
- Create index patterns for log analysis
- Build visualizations for event data
- Set up alerts for anomalies

#### InfluxDB (http://localhost:8086)
- Username: `admin`
- Password: `password123`
- Query time-series data
- Create custom dashboards

## Performance Benchmarks

### Expected Performance
- **Throughput**: 50,000+ events/second
- **Latency**: <10ms producer latency
- **Success Rate**: >99.5%
- **Memory Usage**: <2GB for producer
- **CPU Usage**: <80% during peak load

### Monitoring Commands
```bash
# Check Docker resource usage
docker stats

# Monitor Kafka performance
docker exec kafka kafka-run-class.sh kafka.tools.ConsumerPerformance \
  --broker-list localhost:9092 --topic financial-transactions --messages 100000

# Check system resources
htop
iostat -x 1
```

## Troubleshooting

### Common Issues

#### 1. Services Not Starting
```bash
# Check service logs
docker-compose logs kafka
docker-compose logs zookeeper

# Restart specific service
docker-compose restart kafka
```

#### 2. Low Throughput
```bash
# Check Kafka configuration
docker exec kafka cat /etc/kafka/server.properties | grep -E "(batch.size|linger.ms|buffer.memory)"

# Monitor system resources
docker stats kafka
```

#### 3. Connection Issues
```bash
# Test Kafka connectivity
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Test Redis connectivity
docker exec redis redis-cli ping
```

#### 4. Memory Issues
```bash
# Increase Docker memory allocation
# Edit Docker Desktop settings or docker-compose.yml

# Check Java heap size for Kafka
docker exec kafka ps aux | grep kafka
```

### Performance Tuning

#### Kafka Optimization
```bash
# Increase partition count for better parallelism
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --alter --topic insurance-claims --partitions 24

# Check consumer lag
docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group risk-scoring-consumer
```

#### Producer Optimization
```python
# Adjust producer configuration in config/kafka_config.py
producer_config = {
    'batch.size': 131072,      # Increase batch size
    'linger.ms': 20,           # Increase linger time
    'buffer.memory': 134217728, # Increase buffer memory
    'compression.type': 'lz4'   # Try different compression
}
```

## Test Data Validation

### Sample Event Structures

#### Insurance Claim
```json
{
  "claim_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "event_type": "insurance_claim",
  "claim_type": "auto",
  "claim_amount": 8500.00,
  "risk_score": 0.65,
  "fraud_probability": 0.23,
  "location": "Miami, FL"
}
```

#### Financial Transaction
```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440001",
  "user_id": "123e4567-e89b-12d3-a456-426614174001",
  "timestamp": "2024-01-15T10:30:01.000Z",
  "event_type": "financial_transaction",
  "amount": 150.00,
  "category": "retail",
  "is_fraud": false,
  "risk_score": 0.15
}
```

#### IoT Telemetry
```json
{
  "telemetry_id": "550e8400-e29b-41d4-a716-446655440002",
  "device_id": "123e4567-e89b-12d3-a456-426614174002",
  "timestamp": "2024-01-15T10:30:02.000Z",
  "event_type": "iot_telemetry",
  "device_type": "temperature_sensor",
  "sensor_readings": {"temperature": 23.5, "humidity": 45.2},
  "risk_score": 0.12,
  "is_anomaly": false
}
```

## Continuous Testing

### Automated Testing Pipeline
```bash
# Create a test script for CI/CD
cat > test-pipeline.sh << 'EOF'
#!/bin/bash
set -e

echo "Running automated test pipeline..."

# 1. Infrastructure tests
./scripts/test-infrastructure.sh

# 2. Data generation tests
python3 scripts/test-data-generation.py

# 3. Quick load test
python3 scripts/test-producer-load.py --test-type throughput --throughput 5000 --duration 30

echo "All tests passed!"
EOF

chmod +x test-pipeline.sh
./test-pipeline.sh
```

### Health Checks
```bash
# Create health check endpoint
curl -f http://localhost:8086/health  # InfluxDB
curl -f http://localhost:9200/_cluster/health  # Elasticsearch
docker exec redis redis-cli ping  # Redis
```

This comprehensive testing approach ensures your platform is production-ready and performing at the expected levels.