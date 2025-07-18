"""
Output sinks for writing processed features to Feast and downstream systems.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os
import json

logger = logging.getLogger(__name__)

class OutputSinkManager:
    """Manage output sinks for processed streaming data."""
    
    def __init__(self):
        """Initialize output sink manager."""
        self.feast_endpoint = os.getenv('FEAST_ENDPOINT', 'localhost:6566')
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.influxdb_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        self.output_path = os.getenv('OUTPUT_PATH', '/tmp/streaming-output')
        
        # Ensure output directory exists
        os.makedirs(self.output_path, exist_ok=True)
    
    def write_to_feast(self, df: DataFrame, feature_table_name: str, 
                      trigger_interval: str):
        """Write processed features to Feast feature store."""
        
        def write_to_feast_batch(batch_df, batch_id):
            """Write batch to Feast feature store."""
            try:
                # Convert to feature format expected by Feast
                feature_df = self._prepare_feast_features(batch_df, feature_table_name)
                
                # Write to parquet for Feast offline store
                parquet_path = f"{self.output_path}/feast/{feature_table_name}"
                feature_df.write \
                    .mode("append") \
                    .partitionBy("event_date") \
                    .parquet(parquet_path)
                
                # Write to Redis for online serving (simplified)
                self._write_to_redis(feature_df, feature_table_name)
                
                logger.info(f"Batch {batch_id}: Wrote {feature_df.count()} records to {feature_table_name}")
                
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to Feast: {e}")
        
        return df.writeStream \
            .foreachBatch(write_to_feast_batch) \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{self.output_path}/checkpoints/{feature_table_name}") \
            .start()
    
    def _prepare_feast_features(self, df: DataFrame, feature_table_name: str) -> DataFrame:
        """Prepare DataFrame for Feast feature store format."""
        
        # Add required Feast columns
        feast_df = df.withColumn(
            "event_timestamp", col("event_timestamp")
        ).withColumn(
            "created_timestamp", current_timestamp()
        ).withColumn(
            "event_date", to_date(col("event_timestamp"))
        )
        
        # Select relevant columns based on feature table type
        if feature_table_name == "claims_features":
            return feast_df.select(
                col("user_id").alias("entity_id"),
                col("event_timestamp"),
                col("created_timestamp"),
                col("event_date"),
                col("claim_count_1hr"),
                col("claim_count_24hr"),
                col("avg_claim_amount_24hr"),
                col("composite_risk_score"),
                col("claim_frequency_score"),
                col("requires_manual_review"),
                col("risk_category")
            )
        
        elif feature_table_name == "transaction_features":
            return feast_df.select(
                col("user_id").alias("entity_id"),
                col("event_timestamp"),
                col("created_timestamp"),
                col("event_date"),
                col("transaction_count_1hr"),
                col("transaction_count_24hr"),
                col("avg_transaction_amount_24hr"),
                col("composite_fraud_score"),
                col("velocity_risk_score"),
                col("fraud_probability"),
                col("should_block_transaction"),
                col("requires_additional_auth")
            )
        
        elif feature_table_name == "iot_features":
            return feast_df.select(
                col("device_id").alias("entity_id"),
                col("event_timestamp"),
                col("created_timestamp"),
                col("event_date"),
                col("iot_anomaly_score_5min"),
                col("anomaly_count_1hr"),
                col("composite_anomaly_score"),
                col("alert_level"),
                col("requires_immediate_attention"),
                col("predicted_failure_hours")
            )
        
        else:
            return feast_df
    
    def _write_to_redis(self, df: DataFrame, feature_table_name: str):
        """Write features to Redis for online serving."""
        try:
            # Convert to JSON and write to Redis (simplified implementation)
            # In production, use proper Feast Redis writer
            redis_data = df.collect()
            
            for row in redis_data:
                entity_id = row['entity_id']
                features = {col: row[col] for col in row.asDict().keys() 
                           if col not in ['entity_id', 'event_timestamp', 'created_timestamp', 'event_date']}
                
                # Redis key format: feature_table:entity_id
                redis_key = f"{feature_table_name}:{entity_id}"
                
                # In production, use Redis client to write
                logger.debug(f"Would write to Redis: {redis_key} -> {features}")
                
        except Exception as e:
            logger.error(f"Error writing to Redis: {e}")
    
    def write_monitoring_metrics(self, df: DataFrame, trigger_interval: str):
        """Write monitoring metrics to InfluxDB."""
        
        def write_metrics_batch(batch_df, batch_id):
            """Write monitoring metrics for each batch."""
            try:
                # Compute batch metrics
                metrics = batch_df.agg(
                    count("*").alias("total_events"),
                    countDistinct("user_id").alias("unique_users"),
                    countDistinct("event_type").alias("event_types"),
                    avg("risk_score").alias("avg_risk_score"),
                    sum(when(col("risk_score") > 0.7, 1).otherwise(0)).alias("high_risk_events")
                ).collect()[0]
                
                # Create metrics record
                metrics_record = {
                    "measurement": "streaming_metrics",
                    "timestamp": int(time.time() * 1000000000),  # nanoseconds
                    "fields": {
                        "total_events": metrics["total_events"],
                        "unique_users": metrics["unique_users"],
                        "event_types": metrics["event_types"],
                        "avg_risk_score": float(metrics["avg_risk_score"] or 0),
                        "high_risk_events": metrics["high_risk_events"],
                        "batch_id": batch_id
                    },
                    "tags": {
                        "application": "risk_scoring_platform",
                        "environment": "production"
                    }
                }
                
                # Write to InfluxDB (simplified)
                self._write_to_influxdb(metrics_record)
                
                logger.info(f"Batch {batch_id}: Wrote monitoring metrics - {metrics['total_events']} events processed")
                
            except Exception as e:
                logger.error(f"Error writing monitoring metrics for batch {batch_id}: {e}")
        
        return df.writeStream \
            .foreachBatch(write_metrics_batch) \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{self.output_path}/checkpoints/monitoring") \
            .start()
    
    def _write_to_influxdb(self, metrics_record):
        """Write metrics to InfluxDB."""
        try:
            # In production, use InfluxDB client
            logger.debug(f"Would write to InfluxDB: {metrics_record}")
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")
    
    def write_alerts(self, df: DataFrame, trigger_interval: str):
        """Write high-risk alerts to downstream systems."""
        
        # Filter for high-risk events that require alerts
        alert_df = df.filter(
            (col("composite_risk_score") > 0.8) |
            (col("composite_fraud_score") > 0.8) |
            (col("composite_anomaly_score") > 0.8) |
            (col("requires_manual_review") == True) |
            (col("should_block_transaction") == True) |
            (col("requires_immediate_attention") == True)
        )
        
        def write_alerts_batch(batch_df, batch_id):
            """Write alerts for each batch."""
            try:
                alerts = batch_df.collect()
                
                for alert in alerts:
                    alert_record = {
                        "alert_id": f"alert_{batch_id}_{alert['user_id'] or alert['device_id']}",
                        "timestamp": alert["event_timestamp"],
                        "event_type": alert["event_type"],
                        "entity_id": alert.get("user_id") or alert.get("device_id"),
                        "risk_score": alert.get("composite_risk_score") or alert.get("composite_fraud_score") or alert.get("composite_anomaly_score"),
                        "alert_level": "critical" if alert.get("risk_score", 0) > 0.9 else "high",
                        "requires_action": True,
                        "details": {
                            "event_data": {k: v for k, v in alert.asDict().items() if v is not None}
                        }
                    }
                    
                    # Write to alert system (Kafka topic, webhook, etc.)
                    self._send_alert(alert_record)
                
                if len(alerts) > 0:
                    logger.info(f"Batch {batch_id}: Sent {len(alerts)} alerts")
                    
            except Exception as e:
                logger.error(f"Error writing alerts for batch {batch_id}: {e}")
        
        return alert_df.writeStream \
            .foreachBatch(write_alerts_batch) \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{self.output_path}/checkpoints/alerts") \
            .start()
    
    def _send_alert(self, alert_record):
        """Send alert to downstream systems."""
        try:
            # In production, send to Kafka alerts topic, webhook, or notification service
            logger.info(f"ALERT: {alert_record['alert_level']} - {alert_record['alert_id']}")
        except Exception as e:
            logger.error(f"Error sending alert: {e}")

import time