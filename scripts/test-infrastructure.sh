#!/bin/bash

# Infrastructure testing script
set -e

echo "🧪 Testing Real-Time Event Risk Scoring Platform Infrastructure..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS") echo -e "${GREEN}✅ $message${NC}" ;;
        "ERROR") echo -e "${RED}❌ $message${NC}" ;;
        "WARNING") echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "INFO") echo -e "ℹ️  $message" ;;
    esac
}

# Test Docker services
test_docker_services() {
    print_status "INFO" "Testing Docker services..."
    
    # Check if docker-compose is running
    if ! docker-compose ps | grep -q "Up"; then
        print_status "ERROR" "Docker services are not running. Run 'docker-compose up -d' first."
        return 1
    fi
    
    # Test individual services
    services=("kafka" "zookeeper" "redis" "influxdb" "grafana" "elasticsearch")
    
    for service in "${services[@]}"; do
        if docker-compose ps $service | grep -q "Up"; then
            print_status "SUCCESS" "$service is running"
        else
            print_status "ERROR" "$service is not running"
            return 1
        fi
    done
    
    return 0
}

# Test Kafka connectivity
test_kafka() {
    print_status "INFO" "Testing Kafka connectivity..."
    
    # Test Kafka broker
    if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        print_status "SUCCESS" "Kafka broker is accessible"
    else
        print_status "ERROR" "Cannot connect to Kafka broker"
        return 1
    fi
    
    # List existing topics
    topics=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)
    if [ -n "$topics" ]; then
        print_status "SUCCESS" "Kafka topics found: $(echo $topics | tr '\n' ' ')"
    else
        print_status "WARNING" "No Kafka topics found yet"
    fi
    
    return 0
}

# Test Redis connectivity
test_redis() {
    print_status "INFO" "Testing Redis connectivity..."
    
    if docker exec redis redis-cli ping | grep -q "PONG"; then
        print_status "SUCCESS" "Redis is responding"
    else
        print_status "ERROR" "Redis is not responding"
        return 1
    fi
    
    return 0
}

# Test InfluxDB connectivity
test_influxdb() {
    print_status "INFO" "Testing InfluxDB connectivity..."
    
    # Test InfluxDB health endpoint
    if curl -s http://localhost:8086/health | grep -q "pass"; then
        print_status "SUCCESS" "InfluxDB is healthy"
    else
        print_status "ERROR" "InfluxDB health check failed"
        return 1
    fi
    
    return 0
}

# Test Elasticsearch connectivity
test_elasticsearch() {
    print_status "INFO" "Testing Elasticsearch connectivity..."
    
    # Test Elasticsearch cluster health
    if curl -s http://localhost:9200/_cluster/health | grep -q "yellow\|green"; then
        print_status "SUCCESS" "Elasticsearch cluster is healthy"
    else
        print_status "ERROR" "Elasticsearch cluster health check failed"
        return 1
    fi
    
    return 0
}

# Test web interfaces
test_web_interfaces() {
    print_status "INFO" "Testing web interfaces..."
    
    # Test Grafana
    if curl -s http://localhost:3000 | grep -q "Grafana"; then
        print_status "SUCCESS" "Grafana web interface is accessible"
    else
        print_status "WARNING" "Grafana web interface may not be ready yet"
    fi
    
    # Test Kibana
    if curl -s http://localhost:5601 | grep -q "kibana"; then
        print_status "SUCCESS" "Kibana web interface is accessible"
    else
        print_status "WARNING" "Kibana web interface may not be ready yet"
    fi
    
    return 0
}

# Main test execution
main() {
    echo "Starting infrastructure tests..."
    echo "================================"
    
    # Run tests
    test_docker_services || exit 1
    sleep 2
    test_kafka || exit 1
    sleep 2
    test_redis || exit 1
    sleep 2
    test_influxdb || exit 1
    sleep 2
    test_elasticsearch || exit 1
    sleep 2
    test_web_interfaces
    
    echo ""
    echo "================================"
    print_status "SUCCESS" "All infrastructure tests completed!"
    echo ""
    echo "🌐 Web Interfaces:"
    echo "   Grafana:  http://localhost:3000 (admin/admin)"
    echo "   Kibana:   http://localhost:5601"
    echo "   InfluxDB: http://localhost:8086"
    echo ""
    echo "🚀 Ready to start data generation!"
}

# Run main function
main "$@"