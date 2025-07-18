#!/usr/bin/env python3
"""
Test script for data generation components.
"""
import sys
import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generators.claims_generator import ClaimsGenerator
from data_generators.transaction_generator import TransactionGenerator
from data_generators.iot_generator import IoTGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_claims_generator():
    """Test the claims generator."""
    logger.info("Testing Claims Generator...")
    
    try:
        generator = ClaimsGenerator()
        
        # Test single claim generation
        claim = generator.generate_claim()
        
        # Validate required fields
        required_fields = ['claim_id', 'user_id', 'timestamp', 'event_type', 
                          'claim_type', 'claim_amount', 'risk_score']
        
        for field in required_fields:
            assert field in claim, f"Missing required field: {field}"
        
        # Validate data types and ranges
        assert isinstance(claim['claim_amount'], (int, float)), "claim_amount must be numeric"
        assert claim['claim_amount'] > 0, "claim_amount must be positive"
        assert 0 <= claim['risk_score'] <= 1, "risk_score must be between 0 and 1"
        assert claim['event_type'] == 'insurance_claim', "event_type must be 'insurance_claim'"
        
        # Test batch generation
        batch = generator.generate_batch(10)
        assert len(batch) == 10, "Batch size mismatch"
        
        # Test schema
        schema = generator.get_schema()
        assert 'properties' in schema, "Schema must have properties"
        
        logger.info("✅ Claims Generator: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Claims Generator: FAILED - {e}")
        return False

def test_transaction_generator():
    """Test the transaction generator."""
    logger.info("Testing Transaction Generator...")
    
    try:
        generator = TransactionGenerator()
        
        # Test single transaction generation
        transaction = generator.generate_transaction()
        
        # Validate required fields
        required_fields = ['transaction_id', 'user_id', 'timestamp', 'event_type', 
                          'amount', 'risk_score']
        
        for field in required_fields:
            assert field in transaction, f"Missing required field: {field}"
        
        # Validate data types and ranges
        assert isinstance(transaction['amount'], (int, float)), "amount must be numeric"
        assert transaction['amount'] > 0, "amount must be positive"
        assert 0 <= transaction['risk_score'] <= 1, "risk_score must be between 0 and 1"
        assert transaction['event_type'] == 'financial_transaction', "event_type must be 'financial_transaction'"
        assert isinstance(transaction['is_fraud'], bool), "is_fraud must be boolean"
        
        # Test batch generation
        batch = generator.generate_batch(10)
        assert len(batch) == 10, "Batch size mismatch"
        
        # Test fraud detection
        fraud_count = sum(1 for t in batch if t['is_fraud'])
        logger.info(f"Fraud rate in batch: {fraud_count/len(batch)*100:.1f}%")
        
        logger.info("✅ Transaction Generator: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Transaction Generator: FAILED - {e}")
        return False

def test_iot_generator():
    """Test the IoT generator."""
    logger.info("Testing IoT Generator...")
    
    try:
        generator = IoTGenerator()
        
        # Test single telemetry generation
        telemetry = generator.generate_telemetry()
        
        # Validate required fields
        required_fields = ['telemetry_id', 'device_id', 'timestamp', 'event_type', 
                          'device_type', 'risk_score']
        
        for field in required_fields:
            assert field in telemetry, f"Missing required field: {field}"
        
        # Validate data types and ranges
        assert 0 <= telemetry['risk_score'] <= 1, "risk_score must be between 0 and 1"
        assert telemetry['event_type'] == 'iot_telemetry', "event_type must be 'iot_telemetry'"
        assert isinstance(telemetry['is_anomaly'], bool), "is_anomaly must be boolean"
        assert 'sensor_readings' in telemetry, "sensor_readings must be present"
        
        # Test batch generation
        batch = generator.generate_batch(10)
        assert len(batch) == 10, "Batch size mismatch"
        
        # Test anomaly detection
        anomaly_count = sum(1 for t in batch if t['is_anomaly'])
        logger.info(f"Anomaly rate in batch: {anomaly_count/len(batch)*100:.1f}%")
        
        logger.info("✅ IoT Generator: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ IoT Generator: FAILED - {e}")
        return False

def test_data_quality():
    """Test data quality across all generators."""
    logger.info("Testing Data Quality...")
    
    try:
        # Generate sample data from all sources
        claims_gen = ClaimsGenerator()
        trans_gen = TransactionGenerator()
        iot_gen = IoTGenerator()
        
        claims_batch = claims_gen.generate_batch(100)
        trans_batch = trans_gen.generate_batch(100)
        iot_batch = iot_gen.generate_batch(100)
        
        # Test timestamp consistency
        for batch, name in [(claims_batch, 'claims'), (trans_batch, 'transactions'), (iot_batch, 'iot')]:
            timestamps = [datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00')) for item in batch]
            time_diffs = [(timestamps[i+1] - timestamps[i]).total_seconds() for i in range(len(timestamps)-1)]
            
            # Check that timestamps are reasonable
            assert all(diff >= 0 for diff in time_diffs), f"{name}: Timestamps not in order"
            logger.info(f"✅ {name.title()}: Timestamp consistency check passed")
        
        # Test risk score distribution
        for batch, name in [(claims_batch, 'claims'), (trans_batch, 'transactions'), (iot_batch, 'iot')]:
            risk_scores = [item['risk_score'] for item in batch]
            avg_risk = sum(risk_scores) / len(risk_scores)
            high_risk_count = sum(1 for score in risk_scores if score > 0.7)
            
            logger.info(f"{name.title()}: Avg risk score: {avg_risk:.3f}, High risk: {high_risk_count}%")
            
            # Ensure reasonable distribution
            assert 0.1 <= avg_risk <= 0.9, f"{name}: Risk score distribution seems unrealistic"
        
        logger.info("✅ Data Quality: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data Quality: FAILED - {e}")
        return False

def test_performance():
    """Test data generation performance."""
    logger.info("Testing Performance...")
    
    try:
        generators = [
            (ClaimsGenerator(), 'Claims'),
            (TransactionGenerator(), 'Transactions'),
            (IoTGenerator(), 'IoT')
        ]
        
        for generator, name in generators:
            # Test batch generation speed
            start_time = time.time()
            batch = generator.generate_batch(1000)
            end_time = time.time()
            
            duration = end_time - start_time
            rate = len(batch) / duration
            
            logger.info(f"{name}: Generated {len(batch)} events in {duration:.2f}s ({rate:.0f} events/sec)")
            
            # Ensure reasonable performance (at least 500 events/sec per generator)
            assert rate >= 500, f"{name}: Generation rate too slow: {rate:.0f} events/sec"
        
        logger.info("✅ Performance: PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Performance: FAILED - {e}")
        return False

def main():
    """Run all data generation tests."""
    logger.info("🧪 Starting Data Generation Tests...")
    logger.info("=" * 50)
    
    tests = [
        test_claims_generator,
        test_transaction_generator,
        test_iot_generator,
        test_data_quality,
        test_performance
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
        logger.info("🎉 All data generation tests passed!")
        return 0
    else:
        logger.error("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())