#!/bin/bash

# Spark Structured Streaming submission script
set -e

echo "🚀 Starting Spark Structured Streaming Application..."

# Configuration
SPARK_HOME=${SPARK_HOME:-/opt/spark}
APP_NAME="RiskScoringStreamProcessor"
MAIN_CLASS="spark_consumer"
JAR_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.1"

# Spark configuration for high throughput
SPARK_CONF=(
    "--conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints"
    "--conf spark.sql.streaming.stateStore.maintenanceInterval=60s"
    "--conf spark.sql.adaptive.enabled=true"
    "--conf spark.sql.adaptive.coalescePartitions.enabled=true"
    "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
    "--conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true"
    "--conf spark.executor.memory=2g"
    "--conf spark.executor.cores=2"
    "--conf spark.executor.instances=2"
    "--conf spark.driver.memory=1g"
    "--conf spark.driver.cores=2"
    "--conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=false"
    "--conf spark.sql.streaming.maxBatchesToRetainInMemory=5"
)

# Environment variables
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}
export INFLUXDB_URL=${INFLUXDB_URL:-http://localhost:8086}
export CHECKPOINT_LOCATION=${CHECKPOINT_LOCATION:-/tmp/spark-checkpoints}
export OUTPUT_PATH=${OUTPUT_PATH:-/tmp/streaming-output}

# Create necessary directories
mkdir -p $CHECKPOINT_LOCATION
mkdir -p $OUTPUT_PATH

echo "📋 Configuration:"
echo "  Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "  Redis Host: $REDIS_HOST:$REDIS_PORT"
echo "  InfluxDB URL: $INFLUXDB_URL"
echo "  Checkpoint Location: $CHECKPOINT_LOCATION"
echo "  Output Path: $OUTPUT_PATH"

# Submit Spark application
$SPARK_HOME/bin/spark-submit \
    --name "$APP_NAME" \
    --packages "$JAR_PACKAGES" \
    "${SPARK_CONF[@]}" \
    --py-files feature_engineering.py,window_functions.py,output_sinks.py \
    spark_consumer.py

echo "✅ Spark Structured Streaming application completed"