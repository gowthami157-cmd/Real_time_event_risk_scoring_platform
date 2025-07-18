#!/bin/bash

# Setup script for Real-Time Event Risk Scoring Platform
set -e

echo "🚀 Setting up Real-Time Event Risk Scoring Platform..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check system requirements
echo "🔍 Checking system requirements..."
TOTAL_MEM=$(free -g | awk '/^Mem:/{print $2}')
if [ "$TOTAL_MEM" -lt 8 ]; then
    echo "⚠️  Warning: System has less than 8GB RAM. Performance may be affected."
fi

# Create environment file
if [ ! -f .env ]; then
    echo "📝 Creating environment file..."
    cp .env.example .env
    echo "✅ Environment file created. Edit .env if needed."
fi

# Create data directories
echo "📁 Creating data directories..."
mkdir -p data/kafka
mkdir -p data/redis
mkdir -p data/influxdb
mkdir -p data/elasticsearch
mkdir -p data/grafana
mkdir -p logs

# Set permissions
chmod 755 data/
chmod 755 logs/

# Start infrastructure
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check Kafka is ready
echo "🔍 Checking Kafka connection..."
timeout 60 bash -c 'until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done'

if [ $? -eq 0 ]; then
    echo "✅ Kafka is ready"
else
    echo "❌ Kafka failed to start properly"
    exit 1
fi

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Start data generation: python data-generators/producer.py --throughput 50000"
echo "2. Monitor with Grafana: http://localhost:3000 (admin/admin)"
echo "3. View logs with Kibana: http://localhost:5601"
echo "4. Check InfluxDB: http://localhost:8086"
echo ""
echo "📊 To check service status:"
echo "docker-compose ps"
echo ""
echo "🔧 To view logs:"
echo "docker-compose logs -f [service-name]"