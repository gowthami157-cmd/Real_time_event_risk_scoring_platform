"""
Main Spark Structured Streaming application for real-time event processing.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime
import os

from feature_engineering import FeatureEngineer
from window_functions import WindowAggregator
from output_sinks import OutputSinkManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealTimeStreamProcessor:
    """Main streaming processor for real-time risk scoring events."""
    
    def __init__(self, app_name="RiskScoringStreamProcessor"):
        """Initialize the streaming processor."""
        self.app_name = app_name
        self.spark = self._create_spark_session()
        self.feature_engineer = FeatureEngineer()
        self.window_aggregator = WindowAggregator()
        self.output_sink_manager = OutputSinkManager()
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.checkpoint_location = os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints')
        
        # Processing configuration
        self.watermark_delay = "2 minutes"
        self.trigger_interval = "60 seconds"
        self.max_files_per_trigger = 1000
        
    def _create_spark_session(self):
        """Create optimized Spark session for streaming."""
        return SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
            .getOrCreate()
    
    def create_kafka_stream(self, topics):
        """Create Kafka streaming DataFrame."""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", ",".join(topics)) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 50000) \
            .option("kafka.consumer.max.poll.records", 10000) \
            .load()
    
    def parse_event_data(self, kafka_df):
        """Parse JSON event data from Kafka messages."""
        # Define schema for different event types
        base_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("risk_score", DoubleType(), True)
        ])
        
        # Parse JSON and add metadata
        parsed_df = kafka_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), base_schema).alias("data")
        ).select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("data.*"),
            to_timestamp(col("data.timestamp")).alias("event_timestamp")
        ).filter(col("event_timestamp").isNotNull())
        
        return parsed_df
    
    def process_insurance_claims(self, events_df):
        """Process insurance claims events."""
        claims_df = events_df.filter(col("event_type") == "insurance_claim")
        
        # Add watermark for late data handling
        claims_with_watermark = claims_df.withWatermark("event_timestamp", self.watermark_delay)
        
        # Compute rolling features
        windowed_claims = self.window_aggregator.compute_claim_windows(claims_with_watermark)
        
        # Apply feature engineering
        enriched_claims = self.feature_engineer.enrich_claims(windowed_claims)
        
        return enriched_claims
    
    def process_financial_transactions(self, events_df):
        """Process financial transaction events."""
        transactions_df = events_df.filter(col("event_type") == "financial_transaction")
        
        # Add watermark
        transactions_with_watermark = transactions_df.withWatermark("event_timestamp", self.watermark_delay)
        
        # Compute rolling features
        windowed_transactions = self.window_aggregator.compute_transaction_windows(transactions_with_watermark)
        
        # Apply feature engineering
        enriched_transactions = self.feature_engineer.enrich_transactions(windowed_transactions)
        
        return enriched_transactions
    
    def process_iot_telemetry(self, events_df):
        """Process IoT telemetry events."""
        iot_df = events_df.filter(col("event_type") == "iot_telemetry")
        
        # Add watermark
        iot_with_watermark = iot_df.withWatermark("event_timestamp", self.watermark_delay)
        
        # Compute rolling features
        windowed_iot = self.window_aggregator.compute_iot_windows(iot_with_watermark)
        
        # Apply feature engineering
        enriched_iot = self.feature_engineer.enrich_iot_data(windowed_iot)
        
        return enriched_iot
    
    def start_streaming(self):
        """Start the streaming application."""
        logger.info(f"Starting {self.app_name}...")
        
        # Define topics to consume
        topics = ["insurance-claims", "financial-transactions", "iot-telemetry"]
        
        # Create Kafka stream
        kafka_stream = self.create_kafka_stream(topics)
        
        # Parse event data
        parsed_events = self.parse_event_data(kafka_stream)
        
        # Process different event types in parallel
        claims_stream = self.process_insurance_claims(parsed_events)
        transactions_stream = self.process_financial_transactions(parsed_events)
        iot_stream = self.process_iot_telemetry(parsed_events)
        
        # Start streaming queries
        queries = []
        
        # Claims processing query
        claims_query = self.output_sink_manager.write_to_feast(
            claims_stream,
            "claims_features",
            self.trigger_interval
        )
        queries.append(claims_query)
        
        # Transactions processing query
        transactions_query = self.output_sink_manager.write_to_feast(
            transactions_stream,
            "transaction_features", 
            self.trigger_interval
        )
        queries.append(transactions_query)
        
        # IoT processing query
        iot_query = self.output_sink_manager.write_to_feast(
            iot_stream,
            "iot_features",
            self.trigger_interval
        )
        queries.append(iot_query)
        
        # Monitoring query
        monitoring_query = self.output_sink_manager.write_monitoring_metrics(
            parsed_events,
            self.trigger_interval
        )
        queries.append(monitoring_query)
        
        logger.info(f"Started {len(queries)} streaming queries")
        
        # Wait for termination
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping streaming application...")
            for query in queries:
                query.stop()
        finally:
            self.spark.stop()

def main():
    """Main entry point."""
    processor = RealTimeStreamProcessor()
    processor.start_streaming()

if __name__ == "__main__":
    main()