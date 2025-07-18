"""
Real-time feature engineering for risk scoring events.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """Feature engineering for real-time risk scoring."""
    
    def __init__(self):
        """Initialize feature engineer."""
        self.risk_thresholds = {
            'low': 0.3,
            'medium': 0.7,
            'high': 1.0
        }
    
    def enrich_claims(self, claims_df: DataFrame) -> DataFrame:
        """Enrich insurance claims with computed features."""
        enriched = claims_df.withColumn(
            "claim_amount_zscore",
            (col("claim_amount") - col("avg_claim_amount_24hr")) / 
            (col("stddev_claim_amount_24hr") + lit(0.001))
        ).withColumn(
            "claim_frequency_score",
            when(col("claim_count_24hr") > 3, lit(1.0))
            .when(col("claim_count_24hr") > 1, lit(0.6))
            .otherwise(lit(0.1))
        ).withColumn(
            "time_since_last_claim_hours",
            (unix_timestamp(col("event_timestamp")) - 
             unix_timestamp(col("last_claim_timestamp"))) / 3600
        ).withColumn(
            "is_weekend_claim",
            when(dayofweek(col("event_timestamp")).isin([1, 7]), lit(1)).otherwise(lit(0))
        ).withColumn(
            "is_night_claim", 
            when(hour(col("event_timestamp")).between(22, 6), lit(1)).otherwise(lit(0))
        ).withColumn(
            "claim_velocity_risk",
            when(col("claim_count_1hr") >= 2, lit(1.0))
            .when(col("claim_count_6hr") >= 3, lit(0.8))
            .otherwise(lit(0.0))
        ).withColumn(
            "amount_deviation_risk",
            when(abs(col("claim_amount_zscore")) > 3, lit(1.0))
            .when(abs(col("claim_amount_zscore")) > 2, lit(0.7))
            .otherwise(lit(0.0))
        ).withColumn(
            "composite_risk_score",
            (col("risk_score") * 0.4 + 
             col("claim_frequency_score") * 0.2 +
             col("claim_velocity_risk") * 0.2 +
             col("amount_deviation_risk") * 0.2)
        ).withColumn(
            "risk_category",
            when(col("composite_risk_score") >= self.risk_thresholds['high'], lit("high"))
            .when(col("composite_risk_score") >= self.risk_thresholds['medium'], lit("medium"))
            .otherwise(lit("low"))
        ).withColumn(
            "requires_manual_review",
            (col("composite_risk_score") > 0.8) |
            (col("claim_amount") > 50000) |
            (col("claim_count_24hr") > 2)
        ).withColumn(
            "processing_timestamp",
            current_timestamp()
        )
        
        return enriched
    
    def enrich_transactions(self, transactions_df: DataFrame) -> DataFrame:
        """Enrich financial transactions with computed features."""
        enriched = transactions_df.withColumn(
            "amount_zscore",
            (col("amount") - col("avg_transaction_amount_24hr")) /
            (col("stddev_transaction_amount_24hr") + lit(0.001))
        ).withColumn(
            "velocity_risk_score",
            when(col("transaction_count_1hr") > 10, lit(1.0))
            .when(col("transaction_count_1hr") > 5, lit(0.7))
            .when(col("transaction_count_1hr") > 2, lit(0.4))
            .otherwise(lit(0.0))
        ).withColumn(
            "amount_spike_risk",
            when(col("amount") > col("max_transaction_amount_7d") * 2, lit(1.0))
            .when(col("amount") > col("avg_transaction_amount_7d") * 5, lit(0.8))
            .otherwise(lit(0.0))
        ).withColumn(
            "time_pattern_risk",
            when(hour(col("event_timestamp")).between(2, 5), lit(0.8))
            .when(hour(col("event_timestamp")).between(22, 24), lit(0.4))
            .otherwise(lit(0.0))
        ).withColumn(
            "merchant_risk_score",
            when(col("merchant_transaction_count_24hr") < 5, lit(0.6))
            .when(col("merchant_transaction_count_24hr") > 1000, lit(0.3))
            .otherwise(lit(0.1))
        ).withColumn(
            "location_change_risk",
            when(col("distinct_locations_24hr") > 5, lit(0.9))
            .when(col("distinct_locations_24hr") > 3, lit(0.6))
            .otherwise(lit(0.0))
        ).withColumn(
            "composite_fraud_score",
            (col("risk_score") * 0.3 +
             col("velocity_risk_score") * 0.25 +
             col("amount_spike_risk") * 0.2 +
             col("time_pattern_risk") * 0.1 +
             col("merchant_risk_score") * 0.1 +
             col("location_change_risk") * 0.05)
        ).withColumn(
            "fraud_probability",
            when(col("composite_fraud_score") > 0.8, lit(0.95))
            .when(col("composite_fraud_score") > 0.6, lit(0.75))
            .when(col("composite_fraud_score") > 0.4, lit(0.45))
            .otherwise(lit(0.1))
        ).withColumn(
            "should_block_transaction",
            col("composite_fraud_score") > 0.85
        ).withColumn(
            "requires_additional_auth",
            (col("composite_fraud_score") > 0.6) & (col("composite_fraud_score") <= 0.85)
        ).withColumn(
            "processing_timestamp",
            current_timestamp()
        )
        
        return enriched
    
    def enrich_iot_data(self, iot_df: DataFrame) -> DataFrame:
        """Enrich IoT telemetry with computed features."""
        enriched = iot_df.withColumn(
            "sensor_deviation_score",
            when(col("sensor_readings_deviation_5min") > 3, lit(1.0))
            .when(col("sensor_readings_deviation_5min") > 2, lit(0.7))
            .otherwise(lit(0.0))
        ).withColumn(
            "anomaly_frequency_risk",
            when(col("anomaly_count_1hr") > 5, lit(1.0))
            .when(col("anomaly_count_1hr") > 2, lit(0.6))
            .otherwise(lit(0.0))
        ).withColumn(
            "device_health_score",
            lit(1.0) - 
            (col("maintenance_overdue_days") / 365.0) * 0.5 -
            (col("operating_hours") / 50000.0) * 0.3
        ).withColumn(
            "connectivity_risk",
            when(col("avg_packet_loss_1hr") > 0.05, lit(0.8))
            .when(col("avg_packet_loss_1hr") > 0.02, lit(0.4))
            .otherwise(lit(0.0))
        ).withColumn(
            "sensor_correlation_break",
            when(col("correlation_score_5min") < 0.3, lit(1.0))
            .when(col("correlation_score_5min") < 0.6, lit(0.5))
            .otherwise(lit(0.0))
        ).withColumn(
            "composite_anomaly_score",
            (col("risk_score") * 0.4 +
             col("sensor_deviation_score") * 0.25 +
             col("anomaly_frequency_risk") * 0.15 +
             col("connectivity_risk") * 0.1 +
             col("sensor_correlation_break") * 0.1)
        ).withColumn(
            "alert_level",
            when(col("composite_anomaly_score") > 0.8, lit("critical"))
            .when(col("composite_anomaly_score") > 0.6, lit("high"))
            .when(col("composite_anomaly_score") > 0.3, lit("medium"))
            .otherwise(lit("low"))
        ).withColumn(
            "requires_immediate_attention",
            (col("composite_anomaly_score") > 0.8) |
            (col("device_criticality") == "critical") & (col("composite_anomaly_score") > 0.5)
        ).withColumn(
            "predicted_failure_hours",
            when(col("composite_anomaly_score") > 0.9, lit(2))
            .when(col("composite_anomaly_score") > 0.7, lit(24))
            .when(col("composite_anomaly_score") > 0.5, lit(168))
            .otherwise(lit(8760))
        ).withColumn(
            "processing_timestamp",
            current_timestamp()
        )
        
        return enriched
    
    def compute_cross_event_features(self, claims_df: DataFrame, 
                                   transactions_df: DataFrame) -> DataFrame:
        """Compute features across different event types."""
        # Join claims and transactions by user_id within time windows
        user_activity = claims_df.select(
            col("user_id"),
            col("event_timestamp"),
            col("composite_risk_score").alias("claim_risk"),
            lit("claim").alias("event_source")
        ).union(
            transactions_df.select(
                col("user_id"),
                col("event_timestamp"), 
                col("composite_fraud_score").alias("claim_risk"),
                lit("transaction").alias("event_source")
            )
        )
        
        # Compute cross-event patterns
        cross_features = user_activity.groupBy(
            col("user_id"),
            window(col("event_timestamp"), "1 hour")
        ).agg(
            count("*").alias("total_events_1hr"),
            sum(when(col("event_source") == "claim", 1).otherwise(0)).alias("claims_1hr"),
            sum(when(col("event_source") == "transaction", 1).otherwise(0)).alias("transactions_1hr"),
            avg("claim_risk").alias("avg_risk_1hr"),
            max("claim_risk").alias("max_risk_1hr")
        ).withColumn(
            "cross_event_risk_score",
            when((col("claims_1hr") > 0) & (col("transactions_1hr") > 10), lit(0.9))
            .when(col("avg_risk_1hr") > 0.7, lit(0.8))
            .otherwise(lit(0.1))
        )
        
        return cross_features