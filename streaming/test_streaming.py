#!/usr/bin/env python3
"""
Test script for Spark Structured Streaming components.
"""
import sys
import os
import logging
import time
from datetime import datetime, timedelta

# Add streaming directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_spark_session():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("StreamingTest") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def test_feature_engineering():
    """Test feature engineering components."""
    logger.info("Testing Feature Engineering...")
    
    try:
        from feature_engineering import FeatureEngineer
        
        spark = create_test_spark_session()
        feature_engineer = FeatureEngineer()
        
        # Create test claims data
        test_claims_data = [
            {
                "user_id": "user1",
                "event_timestamp": datetime.now(),
                "claim_amount": 5000.0,
                "risk_score": 0.6,
                "claim_count_1hr": 1,
                "claim_count_24hr": 2,
                "avg_claim_amount_24hr": 4500.0,
                "stddev_claim_amount_24hr": 500.0,
                "last_claim_timestamp": datetime.now() - timedelta(hours=2)
            }
        ]
        
        claims_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("claim_amount", DoubleType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("claim_count_1hr", IntegerType(), True),
            StructField("claim_count_24hr", IntegerType(), True),
            StructField("avg_claim_amount_24hr", DoubleType(), True),
            StructField("stddev_claim_amount_24hr", DoubleType(), True),
            StructField("last_claim_timestamp", TimestampType(), True)
        ])
        
        claims_df = spark.createDataFrame(test_claims_data, claims_schema)
        
        # Test feature engineering
        enriched_claims = feature_engineer.enrich_claims(claims_df)
        
        # Verify results
        result = enriched_claims.collect()[0]
        assert "composite_risk_score" in result.asDict()
        assert "risk_category" in result.asDict()
        
        logger.info("✅ Feature Engineering: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Feature Engineering: FAILED - {e}")
        return False
    finally:
        spark.stop()

def test_window_functions():
    """Test window aggregation functions."""
    logger.info("Testing Window Functions...")
    
    try:
        from window_functions import WindowAggregator
        
        spark = create_test_spark_session()
        window_aggregator = WindowAggregator()
        
        # Create test transaction data
        test_data = []
        base_time = datetime.now()
        
        for i in range(10):
            test_data.append({
                "user_id": "user1",
                "event_timestamp": base_time - timedelta(minutes=i*5),
                "amount": 100.0 + i*10,
                "merchant_name": f"merchant_{i%3}",
                "location": f"location_{i%2}",
                "is_online": i % 2 == 0,
                "category": "retail"
            })
        
        transactions_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("is_online", BooleanType(), True),
            StructField("category", StringType(), True)
        ])
        
        transactions_df = spark.createDataFrame(test_data, transactions_schema)
        
        # Test window aggregations
        windowed_transactions = window_aggregator.compute_transaction_windows(transactions_df)
        
        # Verify results
        results = windowed_transactions.collect()
        assert len(results) > 0
        
        # Check if window columns exist
        columns = windowed_transactions.columns
        assert "transaction_count_1hr" in columns
        assert "avg_transaction_amount_1hr" in columns
        
        logger.info("✅ Window Functions: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Window Functions: FAILED - {e}")
        return False
    finally:
        spark.stop()

def test_output_sinks():
    """Test output sink functionality."""
    logger.info("Testing Output Sinks...")
    
    try:
        from output_sinks import OutputSinkManager
        
        spark = create_test_spark_session()
        sink_manager = OutputSinkManager()
        
        # Create test data
        test_data = [
            {
                "user_id": "user1",
                "event_timestamp": datetime.now(),
                "composite_risk_score": 0.8,
                "risk_category": "high",
                "requires_manual_review": True
            }
        ]
        
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("composite_risk_score", DoubleType(), True),
            StructField("risk_category", StringType(), True),
            StructField("requires_manual_review", BooleanType(), True)
        ])
        
        df = spark.createDataFrame(test_data, schema)
        
        # Test Feast feature preparation
        feast_df = sink_manager._prepare_feast_features(df, "claims_features")
        
        # Verify results
        result = feast_df.collect()[0]
        assert "entity_id" in result.asDict()
        assert "event_date" in result.asDict()
        
        logger.info("✅ Output Sinks: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Output Sinks: FAILED - {e}")
        return False
    finally:
        spark.stop()

def test_kafka_integration():
    """Test Kafka integration (requires running Kafka)."""
    logger.info("Testing Kafka Integration...")
    
    try:
        spark = create_test_spark_session()
        
        # Test Kafka connection (read schema only)
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "insurance-claims") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Verify schema
        expected_columns = ["key", "value", "topic", "partition", "offset", "timestamp"]
        actual_columns = kafka_df.columns
        
        for col in expected_columns:
            assert col in actual_columns, f"Missing column: {col}"
        
        logger.info("✅ Kafka Integration: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Kafka Integration: FAILED - {e}")
        logger.warning("Make sure Kafka is running on localhost:9092")
        return False
    finally:
        spark.stop()

def main():
    """Run all streaming tests."""
    logger.info("🧪 Starting Spark Streaming Tests...")
    logger.info("=" * 50)
    
    tests = [
        test_feature_engineering,
        test_window_functions,
        test_output_sinks,
        test_kafka_integration
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            logger.error(f"Test failed with exception: {e}")
            results.append(False)
        
        logger.info("-" * 30)
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    logger.info("=" * 50)
    logger.info(f"📊 Test Results: {passed}/{total} passed")
    
    if passed == total:
        logger.info("🎉 All streaming tests passed!")
        return 0
    else:
        logger.error("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())