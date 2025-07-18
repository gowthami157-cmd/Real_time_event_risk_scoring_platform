#!/usr/bin/env python3
"""
Test script for the high-throughput producer.
"""
import time
import logging
import argparse
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generators.producer import HighThroughputProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_producer_basic():
    """Test basic producer functionality."""
    logger.info("Testing basic producer functionality...")
    
    producer = HighThroughputProducer(target_throughput=1000)
    
    try:
        # Start producer
        producer.start()
        
        # Run for 10 seconds
        time.sleep(10)
        
        # Get stats
        stats = producer.get_stats()
        logger.info(f"Producer stats: {stats}")
        
        # Stop producer
        producer.stop()
        
        # Verify stats
        assert stats['messages_sent'] > 0, "No messages were sent"
        assert stats['success_rate'] > 0.95, f"Success rate too low: {stats['success_rate']}"
        
        logger.info("✅ Basic producer test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Basic producer test failed: {e}")
        return False

def test_producer_high_throughput():
    """Test high-throughput producer functionality."""
    logger.info("Testing high-throughput producer...")
    
    producer = HighThroughputProducer(target_throughput=10000)
    
    try:
        # Start producer
        producer.start()
        
        # Run for 30 seconds
        start_time = time.time()
        while time.time() - start_time < 30:
            time.sleep(5)
            stats = producer.get_stats()
            logger.info(f"Throughput: {stats['messages_per_second']:.1f} msg/sec")
        
        # Get final stats
        final_stats = producer.get_stats()
        logger.info(f"Final stats: {final_stats}")
        
        # Stop producer
        producer.stop()
        
        # Verify performance
        assert final_stats['messages_sent'] > 0, "No messages were sent"
        assert final_stats['messages_per_second'] > 5000, \
            f"Throughput too low: {final_stats['messages_per_second']}"
        assert final_stats['success_rate'] > 0.95, \
            f"Success rate too low: {final_stats['success_rate']}"
        
        logger.info("✅ High-throughput producer test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ High-throughput producer test failed: {e}")
        return False

def test_data_generators():
    """Test data generators independently."""
    logger.info("Testing data generators...")
    
    try:
        from data_generators.claims_generator import ClaimsGenerator
        from data_generators.transaction_generator import TransactionGenerator
        from data_generators.iot_generator import IoTGenerator
        
        # Test claims generator
        claims_gen = ClaimsGenerator()
        claim = claims_gen.generate_claim()
        assert 'claim_id' in claim, "Missing claim_id"
        assert 'risk_score' in claim, "Missing risk_score"
        assert 0 <= claim['risk_score'] <= 1, "Invalid risk_score range"
        
        # Test transaction generator
        trans_gen = TransactionGenerator()
        transaction = trans_gen.generate_transaction()
        assert 'transaction_id' in transaction, "Missing transaction_id"
        assert 'risk_score' in transaction, "Missing risk_score"
        assert 0 <= transaction['risk_score'] <= 1, "Invalid risk_score range"
        
        # Test IoT generator
        iot_gen = IoTGenerator()
        telemetry = iot_gen.generate_telemetry()
        assert 'telemetry_id' in telemetry, "Missing telemetry_id"
        assert 'risk_score' in telemetry, "Missing risk_score"
        assert 0 <= telemetry['risk_score'] <= 1, "Invalid risk_score range"
        
        logger.info("✅ Data generators test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data generators test failed: {e}")
        return False

def main():
    """Main test function."""
    parser = argparse.ArgumentParser(description='Test producer functionality')
    parser.add_argument('--test', choices=['basic', 'throughput', 'generators', 'all'],
                       default='all', help='Test to run')
    
    args = parser.parse_args()
    
    logger.info(f"Starting tests: {args.test}")
    
    results = []
    
    if args.test in ['generators', 'all']:
        results.append(test_data_generators())
    
    if args.test in ['basic', 'all']:
        results.append(test_producer_basic())
    
    if args.test in ['throughput', 'all']:
        results.append(test_producer_high_throughput())
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    logger.info(f"\n{'='*50}")
    logger.info(f"Test Results: {passed}/{total} passed")
    
    if passed == total:
        logger.info("🎉 All tests passed!")
        sys.exit(0)
    else:
        logger.error("❌ Some tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main()