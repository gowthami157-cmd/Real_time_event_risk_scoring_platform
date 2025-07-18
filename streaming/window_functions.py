"""
Rolling window aggregations for real-time feature computation.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

class WindowAggregator:
    """Compute rolling window aggregations for different event types."""
    
    def __init__(self):
        """Initialize window aggregator."""
        self.window_sizes = {
            '1min': '1 minute',
            '5min': '5 minutes', 
            '1hr': '1 hour',
            '6hr': '6 hours',
            '24hr': '24 hours',
            '7d': '7 days'
        }
    
    def compute_claim_windows(self, claims_df: DataFrame) -> DataFrame:
        """Compute rolling windows for insurance claims."""
        # 1-hour windows
        claims_1hr = claims_df.groupBy(
            col("user_id"),
            window(col("event_timestamp"), self.window_sizes['1hr'])
        ).agg(
            count("*").alias("claim_count_1hr"),
            sum("claim_amount").alias("total_claim_amount_1hr"),
            avg("claim_amount").alias("avg_claim_amount_1hr"),
            max("claim_amount").alias("max_claim_amount_1hr"),
            stddev("claim_amount").alias("stddev_claim_amount_1hr"),
            max("event_timestamp").alias("last_claim_timestamp_1hr")
        )
        
        # 6-hour windows
        claims_6hr = claims_df.groupBy(
            col("user_id"),
            window(col("event_timestamp"), self.window_sizes['6hr'])
        ).agg(
            count("*").alias("claim_count_6hr"),
            sum("claim_amount").alias("total_claim_amount_6hr"),
            avg("claim_amount").alias("avg_claim_amount_6hr"),
            countDistinct("claim_type").alias("distinct_claim_types_6hr")
        )
        
        # 24-hour windows
        claims_24hr = claims_df.groupBy(
            col("user_id"),
            window(col("event_timestamp"), self.window_sizes['24hr'])
        ).agg(
            count("*").alias("claim_count_24hr"),
            sum("claim_amount").alias("total_claim_amount_24hr"),
            avg("claim_amount").alias("avg_claim_amount_24hr"),
            stddev("claim_amount").alias("stddev_claim_amount_24hr"),
            max("claim_amount").alias("max_claim_amount_24hr"),
            min("claim_amount").alias("min_claim_amount_24hr"),
            countDistinct("location").alias("distinct_locations_24hr"),
            max("event_timestamp").alias("last_claim_timestamp")
        )
        
        # Join all windows back to original data
        windowed_claims = claims_df.alias("base") \
            .join(claims_1hr.alias("w1hr"), 
                  (col("base.user_id") == col("w1hr.user_id")) &
                  (col("base.event_timestamp") >= col("w1hr.window.start")) &
                  (col("base.event_timestamp") < col("w1hr.window.end")), "left") \
            .join(claims_6hr.alias("w6hr"),
                  (col("base.user_id") == col("w6hr.user_id")) &
                  (col("base.event_timestamp") >= col("w6hr.window.start")) &
                  (col("base.event_timestamp") < col("w6hr.window.end")), "left") \
            .join(claims_24hr.alias("w24hr"),
                  (col("base.user_id") == col("w24hr.user_id")) &
                  (col("base.event_timestamp") >= col("w24hr.window.start")) &
                  (col("base.event_timestamp") < col("w24hr.window.end")), "left") \
            .select("base.*", 
                   "w1hr.claim_count_1hr", "w1hr.total_claim_amount_1hr", 
                   "w1hr.avg_claim_amount_1hr", "w1hr.max_claim_amount_1hr",
                   "w1hr.stddev_claim_amount_1hr", "w1hr.last_claim_timestamp_1hr",
                   "w6hr.claim_count_6hr", "w6hr.total_claim_amount_6hr",
                   "w6hr.avg_claim_amount_6hr", "w6hr.distinct_claim_types_6hr",
                   "w24hr.claim_count_24hr", "w24hr.total_claim_amount_24hr",
                   "w24hr.avg_claim_amount_24hr", "w24hr.stddev_claim_amount_24hr",
                   "w24hr.max_claim_amount_24hr", "w24hr.min_claim_amount_24hr",
                   "w24hr.distinct_locations_24hr", "w24hr.last_claim_timestamp")
        
        return windowed_claims
    
    def compute_transaction_windows(self, transactions_df: DataFrame) -> DataFrame:
        """Compute rolling windows for financial transactions."""
        # 1-hour windows
        trans_1hr = transactions_df.groupBy(
            col("user_id"),
            window(col("event_timestamp"), self.window_sizes['1hr'])
        ).agg(
            count("*").alias("transaction_count_1hr"),
            sum("amount").alias("total_amount_1hr"),
            avg("amount").alias("avg_transaction_amount_1hr"),
            max("amount").alias("max_transaction_amount_1hr"),
            countDistinct("merchant_name").alias("distinct_merchants_1hr"),
            countDistinct("location").alias("distinct_locations_1hr"),
            sum(when(col("is_online") == True, 1).otherwise(0)).alias("online_transactions_1hr")
        )
        
        # 24-hour windows  
        trans_24hr = transactions_df.groupBy(
            col("user_id"),
            window(col("event_timestamp"), self.window_sizes['24hr'])
        ).agg(
            count("*").alias("transaction_count_24hr"),
            sum("amount").alias("total_amount_24hr"),
            avg("amount").alias("avg_transaction_amount_24hr"),
            stddev("amount").alias("stddev_transaction_amount_24hr"),
            max("amount").alias("max_transaction_amount_24hr"),
            min("amount").alias("min_transaction_amount_24hr"),
            countDistinct("merchant_name").alias("distinct_merchants_24hr"),
            countDistinct("location").alias("distinct_locations_24hr"),
            countDistinct("category").alias("distinct_categories_24hr")
        )
        
        # 7-day windows
        trans_7d = transactions_df.groupBy(
            col("user_id"),
            window(col("event_timestamp"), self.window_sizes['7d'])
        ).agg(
            count("*").alias("transaction_count_7d"),
            avg("amount").alias("avg_transaction_amount_7d"),
            max("amount").alias("max_transaction_amount_7d"),
            countDistinct("merchant_name").alias("distinct_merchants_7d")
        )
        
        # Merchant-specific windows
        merchant_24hr = transactions_df.groupBy(
            col("merchant_name"),
            window(col("event_timestamp"), self.window_sizes['24hr'])
        ).agg(
            count("*").alias("merchant_transaction_count_24hr"),
            avg("amount").alias("merchant_avg_amount_24hr"),
            countDistinct("user_id").alias("merchant_unique_users_24hr")
        )
        
        # Join all windows
        windowed_transactions = transactions_df.alias("base") \
            .join(trans_1hr.alias("w1hr"),
                  (col("base.user_id") == col("w1hr.user_id")) &
                  (col("base.event_timestamp") >= col("w1hr.window.start")) &
                  (col("base.event_timestamp") < col("w1hr.window.end")), "left") \
            .join(trans_24hr.alias("w24hr"),
                  (col("base.user_id") == col("w24hr.user_id")) &
                  (col("base.event_timestamp") >= col("w24hr.window.start")) &
                  (col("base.event_timestamp") < col("w24hr.window.end")), "left") \
            .join(trans_7d.alias("w7d"),
                  (col("base.user_id") == col("w7d.user_id")) &
                  (col("base.event_timestamp") >= col("w7d.window.start")) &
                  (col("base.event_timestamp") < col("w7d.window.end")), "left") \
            .join(merchant_24hr.alias("m24hr"),
                  (col("base.merchant_name") == col("m24hr.merchant_name")) &
                  (col("base.event_timestamp") >= col("m24hr.window.start")) &
                  (col("base.event_timestamp") < col("m24hr.window.end")), "left") \
            .select("base.*",
                   "w1hr.transaction_count_1hr", "w1hr.total_amount_1hr",
                   "w1hr.avg_transaction_amount_1hr", "w1hr.max_transaction_amount_1hr",
                   "w1hr.distinct_merchants_1hr", "w1hr.distinct_locations_1hr",
                   "w1hr.online_transactions_1hr",
                   "w24hr.transaction_count_24hr", "w24hr.total_amount_24hr",
                   "w24hr.avg_transaction_amount_24hr", "w24hr.stddev_transaction_amount_24hr",
                   "w24hr.max_transaction_amount_24hr", "w24hr.min_transaction_amount_24hr",
                   "w24hr.distinct_merchants_24hr", "w24hr.distinct_locations_24hr",
                   "w24hr.distinct_categories_24hr",
                   "w7d.transaction_count_7d", "w7d.avg_transaction_amount_7d",
                   "w7d.max_transaction_amount_7d", "w7d.distinct_merchants_7d",
                   "m24hr.merchant_transaction_count_24hr", "m24hr.merchant_avg_amount_24hr",
                   "m24hr.merchant_unique_users_24hr")
        
        return windowed_transactions
    
    def compute_iot_windows(self, iot_df: DataFrame) -> DataFrame:
        """Compute rolling windows for IoT telemetry."""
        # 5-minute windows for sensor readings
        iot_5min = iot_df.groupBy(
            col("device_id"),
            window(col("event_timestamp"), self.window_sizes['5min'])
        ).agg(
            count("*").alias("reading_count_5min"),
            sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count_5min"),
            avg("risk_score").alias("avg_risk_score_5min"),
            max("risk_score").alias("max_risk_score_5min"),
            # Compute sensor reading statistics (assuming temperature sensor)
            avg(col("sensor_readings.temperature")).alias("avg_temperature_5min"),
            stddev(col("sensor_readings.temperature")).alias("stddev_temperature_5min"),
            max(col("sensor_readings.temperature")).alias("max_temperature_5min"),
            min(col("sensor_readings.temperature")).alias("min_temperature_5min")
        ).withColumn(
            "sensor_readings_deviation_5min",
            abs(col("max_temperature_5min") - col("min_temperature_5min")) / 
            (col("stddev_temperature_5min") + lit(0.001))
        ).withColumn(
            "iot_anomaly_score_5min",
            when(col("anomaly_count_5min") > 0, 
                 col("anomaly_count_5min").cast("double") / col("reading_count_5min"))
            .otherwise(lit(0.0))
        )
        
        # 1-hour windows
        iot_1hr = iot_df.groupBy(
            col("device_id"),
            window(col("event_timestamp"), self.window_sizes['1hr'])
        ).agg(
            count("*").alias("reading_count_1hr"),
            sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count_1hr"),
            avg("packet_loss").alias("avg_packet_loss_1hr"),
            avg("latency_ms").alias("avg_latency_1hr"),
            max("latency_ms").alias("max_latency_1hr")
        )
        
        # 24-hour windows
        iot_24hr = iot_df.groupBy(
            col("device_id"),
            window(col("event_timestamp"), self.window_sizes['24hr'])
        ).agg(
            count("*").alias("reading_count_24hr"),
            sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count_24hr"),
            avg("risk_score").alias("avg_risk_score_24hr"),
            avg("battery_level").alias("avg_battery_level_24hr"),
            min("battery_level").alias("min_battery_level_24hr")
        )
        
        # Device type aggregations
        device_type_1hr = iot_df.groupBy(
            col("device_type"),
            window(col("event_timestamp"), self.window_sizes['1hr'])
        ).agg(
            count("*").alias("device_type_count_1hr"),
            avg("risk_score").alias("device_type_avg_risk_1hr"),
            sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("device_type_anomalies_1hr")
        )
        
        # Compute sensor correlation (simplified for temperature/humidity)
        correlation_5min = iot_df.filter(col("device_type") == "temperature_sensor") \
            .groupBy(
                col("device_id"),
                window(col("event_timestamp"), self.window_sizes['5min'])
            ).agg(
                corr(col("sensor_readings.temperature"), 
                     col("sensor_readings.humidity")).alias("temp_humidity_correlation_5min")
            ).withColumn(
                "correlation_score_5min",
                when(col("temp_humidity_correlation_5min").isNull(), lit(1.0))
                .otherwise(abs(col("temp_humidity_correlation_5min")))
            )
        
        # Join all windows
        windowed_iot = iot_df.alias("base") \
            .join(iot_5min.alias("w5min"),
                  (col("base.device_id") == col("w5min.device_id")) &
                  (col("base.event_timestamp") >= col("w5min.window.start")) &
                  (col("base.event_timestamp") < col("w5min.window.end")), "left") \
            .join(iot_1hr.alias("w1hr"),
                  (col("base.device_id") == col("w1hr.device_id")) &
                  (col("base.event_timestamp") >= col("w1hr.window.start")) &
                  (col("base.event_timestamp") < col("w1hr.window.end")), "left") \
            .join(iot_24hr.alias("w24hr"),
                  (col("base.device_id") == col("w24hr.device_id")) &
                  (col("base.event_timestamp") >= col("w24hr.window.start")) &
                  (col("base.event_timestamp") < col("w24hr.window.end")), "left") \
            .join(device_type_1hr.alias("dt1hr"),
                  (col("base.device_type") == col("dt1hr.device_type")) &
                  (col("base.event_timestamp") >= col("dt1hr.window.start")) &
                  (col("base.event_timestamp") < col("dt1hr.window.end")), "left") \
            .join(correlation_5min.alias("corr5min"),
                  (col("base.device_id") == col("corr5min.device_id")) &
                  (col("base.event_timestamp") >= col("corr5min.window.start")) &
                  (col("base.event_timestamp") < col("corr5min.window.end")), "left") \
            .select("base.*",
                   "w5min.reading_count_5min", "w5min.anomaly_count_5min",
                   "w5min.avg_risk_score_5min", "w5min.max_risk_score_5min",
                   "w5min.sensor_readings_deviation_5min", "w5min.iot_anomaly_score_5min",
                   "w1hr.reading_count_1hr", "w1hr.anomaly_count_1hr",
                   "w1hr.avg_packet_loss_1hr", "w1hr.avg_latency_1hr", "w1hr.max_latency_1hr",
                   "w24hr.reading_count_24hr", "w24hr.anomaly_count_24hr",
                   "w24hr.avg_risk_score_24hr", "w24hr.avg_battery_level_24hr",
                   "w24hr.min_battery_level_24hr",
                   "dt1hr.device_type_count_1hr", "dt1hr.device_type_avg_risk_1hr",
                   "dt1hr.device_type_anomalies_1hr",
                   "corr5min.correlation_score_5min") \
            .withColumn(
                "maintenance_overdue_days",
                datediff(col("event_timestamp"), col("last_maintenance"))
            )
        
        return windowed_iot