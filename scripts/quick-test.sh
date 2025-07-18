#!/bin/bash

# Quick test script for the entire platform
set -e

echo "🚀 Quick Test - Real-Time Event Risk Scoring Platform"
echo "====================================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_step() {
    echo -e "\n${BLUE}📋 Step $1: $2${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Step 1: Check prerequisites
print_step "1" "Checking Prerequisites"

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
    exit 1
fi
print_success "Docker is installed"

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed"
    exit 1
fi
print_success "Docker Compose is installed"

if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed"
    exit 1
fi
print_success "Python 3 is installed"

# Step 2: Start infrastructure
print_step "2" "Starting Infrastructure"

if ! docker-compose ps | grep -q "Up"; then
    echo "Starting Docker services..."
    docker-compose up -d
    echo "Waiting for services to initialize..."
    sleep 30
else
    print_success "Docker services already running"
fi

# Step 3: Test infrastructure
print_step "3" "Testing Infrastructure"

chmod +x scripts/test-infrastructure.sh
if ./scripts/test-infrastructure.sh; then
    print_success "Infrastructure tests passed"
else
    print_error "Infrastructure tests failed"
    exit 1
fi

# Step 4: Install Python dependencies
print_step "4" "Installing Python Dependencies"

if pip3 install -r requirements.txt > /dev/null 2>&1; then
    print_success "Python dependencies installed"
else
    print_error "Failed to install Python dependencies"
    exit 1
fi

# Step 5: Test data generators
print_step "5" "Testing Data Generators"

if python3 scripts/test-data-generation.py; then
    print_success "Data generator tests passed"
else
    print_error "Data generator tests failed"
    exit 1
fi

# Step 6: Quick producer test
print_step "6" "Testing Producer (30 seconds)"

echo "Starting producer with 5,000 events/sec for 30 seconds..."
timeout 35 python3 data-generators/producer.py --throughput 5000 --duration 30 || true

print_success "Producer test completed"

# Step 7: Verify data in Kafka
print_step "7" "Verifying Data in Kafka"

# Check if topics have data
topics=("insurance-claims" "financial-transactions" "iot-telemetry")
for topic in "${topics[@]}"; do
    message_count=$(docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 --topic $topic --time -1 2>/dev/null | \
        awk -F: '{sum += $3} END {print sum}' || echo "0")
    
    if [ "$message_count" -gt 0 ]; then
        print_success "$topic: $message_count messages"
    else
        print_warning "$topic: No messages found"
    fi
done

# Final summary
echo ""
echo "====================================================="
echo -e "${GREEN}🎉 Quick Test Completed Successfully!${NC}"
echo ""
echo "🌐 Access Points:"
echo "   Grafana:  http://localhost:3000 (admin/admin)"
echo "   Kibana:   http://localhost:5601"
echo "   InfluxDB: http://localhost:8086"
echo ""
echo "🚀 Next Steps:"
echo "   1. Run full load test: python3 scripts/test-producer-load.py --test-type scaling"
echo "   2. Monitor with Grafana: http://localhost:3000"
echo "   3. Start continuous generation: python3 data-generators/producer.py --throughput 50000"
echo ""
echo "📊 To check Kafka topics:"
echo "   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list"
echo ""
echo "🔍 To consume messages:"
echo "   docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic insurance-claims --from-beginning --max-messages 5"