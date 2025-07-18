#!/usr/bin/env python3
"""
Load testing script for the high-throughput producer.
"""
import sys
import os
import time
import logging
import threading
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generators.producer import HighThroughputProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoadTester:
    """Load testing framework for the producer."""
    
    def __init__(self):
        self.results = []
        self.test_running = False
    
    def run_throughput_test(self, target_throughput: int, duration: int) -> Dict[str, Any]:
        """Run a throughput test for specified duration."""
        logger.info(f"🚀 Starting throughput test: {target_throughput:,} events/sec for {duration}s")
        
        producer = HighThroughputProducer(target_throughput=target_throughput)
        
        try:
            # Start producer
            start_time = time.time()
            producer.start()
            
            # Monitor for specified duration
            stats_history = []
            
            while time.time() - start_time < duration:
                time.sleep(5)  # Sample every 5 seconds
                stats = producer.get_stats()
                stats['elapsed_time'] = time.time() - start_time
                stats_history.append(stats.copy())
                
                logger.info(
                    f"⏱️  {stats['elapsed_time']:.0f}s - "
                    f"Rate: {stats['messages_per_second']:.0f} msg/sec, "
                    f"Total: {stats['messages_sent']:,}, "
                    f"Success: {stats['success_rate']:.1%}"
                )
            
            # Get final stats
            final_stats = producer.get_stats()
            
            # Stop producer
            producer.stop()
            
            # Calculate test results
            test_result = {
                'target_throughput': target_throughput,
                'duration': duration,
                'final_stats': final_stats,
                'stats_history': stats_history,
                'achieved_throughput': final_stats['messages_per_second'],
                'throughput_ratio': final_stats['messages_per_second'] / target_throughput,
                'success_rate': final_stats['success_rate'],
                'total_messages': final_stats['messages_sent'],
                'total_bytes': final_stats['bytes_sent'],
                'avg_message_size': final_stats['bytes_sent'] / final_stats['messages_sent'] if final_stats['messages_sent'] > 0 else 0
            }
            
            return test_result
            
        except Exception as e:
            logger.error(f"❌ Throughput test failed: {e}")
            producer.stop()
            raise
    
    def run_scaling_test(self, throughput_levels: List[int], duration_per_level: int) -> List[Dict[str, Any]]:
        """Run scaling tests at different throughput levels."""
        logger.info(f"📈 Starting scaling test with levels: {throughput_levels}")
        
        results = []
        
        for throughput in throughput_levels:
            logger.info(f"\n{'='*60}")
            logger.info(f"Testing throughput level: {throughput:,} events/sec")
            logger.info(f"{'='*60}")
            
            try:
                result = self.run_throughput_test(throughput, duration_per_level)
                results.append(result)
                
                # Brief pause between tests
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Failed at throughput level {throughput:,}: {e}")
                break
        
        return results
    
    def run_endurance_test(self, throughput: int, duration: int) -> Dict[str, Any]:
        """Run an endurance test for extended duration."""
        logger.info(f"🏃 Starting endurance test: {throughput:,} events/sec for {duration}s ({duration/60:.1f} minutes)")
        
        producer = HighThroughputProducer(target_throughput=throughput)
        
        try:
            start_time = time.time()
            producer.start()
            
            # Monitor with detailed logging
            stats_history = []
            last_report_time = start_time
            
            while time.time() - start_time < duration:
                time.sleep(10)  # Sample every 10 seconds
                current_time = time.time()
                stats = producer.get_stats()
                stats['elapsed_time'] = current_time - start_time
                stats_history.append(stats.copy())
                
                # Report every minute
                if current_time - last_report_time >= 60:
                    elapsed_minutes = (current_time - start_time) / 60
                    remaining_minutes = (duration - (current_time - start_time)) / 60
                    
                    logger.info(
                        f"🕐 {elapsed_minutes:.1f}m elapsed, {remaining_minutes:.1f}m remaining - "
                        f"Rate: {stats['messages_per_second']:.0f} msg/sec, "
                        f"Total: {stats['messages_sent']:,}, "
                        f"Success: {stats['success_rate']:.1%}"
                    )
                    last_report_time = current_time
            
            # Get final stats
            final_stats = producer.get_stats()
            producer.stop()
            
            # Analyze stability
            throughput_samples = [s['messages_per_second'] for s in stats_history[-10:]]  # Last 10 samples
            avg_throughput = sum(throughput_samples) / len(throughput_samples)
            throughput_std = (sum((x - avg_throughput) ** 2 for x in throughput_samples) / len(throughput_samples)) ** 0.5
            stability_coefficient = throughput_std / avg_throughput if avg_throughput > 0 else 1
            
            test_result = {
                'throughput': throughput,
                'duration': duration,
                'final_stats': final_stats,
                'stats_history': stats_history,
                'avg_throughput': avg_throughput,
                'throughput_stability': 1 - stability_coefficient,  # Higher is more stable
                'success_rate': final_stats['success_rate'],
                'total_messages': final_stats['messages_sent'],
                'total_bytes': final_stats['bytes_sent']
            }
            
            return test_result
            
        except Exception as e:
            logger.error(f"❌ Endurance test failed: {e}")
            producer.stop()
            raise
    
    def print_test_summary(self, results: List[Dict[str, Any]]):
        """Print a summary of test results."""
        logger.info("\n" + "="*80)
        logger.info("📊 LOAD TEST SUMMARY")
        logger.info("="*80)
        
        for i, result in enumerate(results, 1):
            logger.info(f"\nTest {i}:")
            logger.info(f"  Target Throughput: {result.get('target_throughput', result.get('throughput', 'N/A')):,} events/sec")
            logger.info(f"  Achieved Throughput: {result.get('achieved_throughput', result.get('avg_throughput', 0)):.0f} events/sec")
            logger.info(f"  Success Rate: {result['success_rate']:.1%}")
            logger.info(f"  Total Messages: {result['total_messages']:,}")
            logger.info(f"  Total Data: {result['total_bytes']/1024/1024:.1f} MB")
            
            if 'throughput_ratio' in result:
                logger.info(f"  Throughput Ratio: {result['throughput_ratio']:.1%}")
            
            if 'throughput_stability' in result:
                logger.info(f"  Stability: {result['throughput_stability']:.1%}")

def main():
    """Main load testing function."""
    parser = argparse.ArgumentParser(description='Load test the high-throughput producer')
    parser.add_argument('--test-type', choices=['throughput', 'scaling', 'endurance'], 
                       default='throughput', help='Type of load test to run')
    parser.add_argument('--throughput', type=int, default=10000,
                       help='Target throughput for throughput/endurance tests')
    parser.add_argument('--duration', type=int, default=60,
                       help='Test duration in seconds')
    parser.add_argument('--scaling-levels', nargs='+', type=int, 
                       default=[5000, 10000, 25000, 50000],
                       help='Throughput levels for scaling test')
    parser.add_argument('--scaling-duration', type=int, default=30,
                       help='Duration per level for scaling test')
    
    args = parser.parse_args()
    
    tester = LoadTester()
    
    try:
        if args.test_type == 'throughput':
            logger.info("🧪 Running Throughput Test")
            result = tester.run_throughput_test(args.throughput, args.duration)
            tester.print_test_summary([result])
            
        elif args.test_type == 'scaling':
            logger.info("🧪 Running Scaling Test")
            results = tester.run_scaling_test(args.scaling_levels, args.scaling_duration)
            tester.print_test_summary(results)
            
        elif args.test_type == 'endurance':
            logger.info("🧪 Running Endurance Test")
            result = tester.run_endurance_test(args.throughput, args.duration)
            tester.print_test_summary([result])
        
        logger.info("\n🎉 Load testing completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Load testing interrupted by user")
    except Exception as e:
        logger.error(f"\n❌ Load testing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()