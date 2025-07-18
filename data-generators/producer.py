"""
High-throughput Kafka producer for streaming event data.
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dataclasses import dataclass

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from claims_generator import ClaimsGenerator
from transaction_generator import TransactionGenerator
from iot_generator import IoTGenerator
from config.kafka_config import get_producer_config, TOPICS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ProducerStats:
    """Statistics for monitoring producer performance."""
    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    total_latency: float = 0.0
    start_time: float = 0.0
    last_report_time: float = 0.0

class HighThroughputProducer:
    """High-throughput Kafka producer for streaming event data."""
    
    def __init__(self, target_throughput: int = 50000):
        """Initialize the producer with target throughput."""
        self.target_throughput = target_throughput
        self.producer_config = get_producer_config()
        self.producer = Producer(self.producer_config)
        
        # Initialize data generators
        self.claims_generator = ClaimsGenerator()
        self.transaction_generator = TransactionGenerator()
        self.iot_generator = IoTGenerator()
        
        # Statistics
        self.stats = ProducerStats()
        self.stats_lock = threading.Lock()
        
        # Control flags
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Create topics if they don't exist
        self._create_topics()
    
    def _create_topics(self):
        """Create Kafka topics if they don't exist."""
        admin_client = AdminClient({'bootstrap.servers': self.producer_config['bootstrap.servers']})
        
        # Get existing topics
        existing_topics = admin_client.list_topics().topics
        
        # Create new topics
        new_topics = []
        for topic_name, topic_config in TOPICS.items():
            if topic_name not in existing_topics:
                new_topic = NewTopic(
                    topic_name,
                    num_partitions=topic_config['partitions'],
                    replication_factor=topic_config['replication_factor']
                )
                new_topics.append(new_topic)
        
        if new_topics:
            logger.info(f"Creating {len(new_topics)} new topics...")
            fs = admin_client.create_topics(new_topics)
            
            # Wait for topic creation
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Topic {topic} created successfully")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")
        else:
            logger.info("All topics already exist")
    
    def _delivery_callback(self, err, msg):
        """Callback for delivery reports."""
        with self.stats_lock:
            if err:
                self.stats.messages_failed += 1
                logger.error(f"Message delivery failed: {err}")
            else:
                self.stats.messages_sent += 1
                self.stats.bytes_sent += len(msg.value()) if msg.value() else 0
    
    def _generate_event_batch(self, event_type: str, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of events based on type."""
        timestamp = datetime.utcnow()
        
        if event_type == 'claims':
            return self.claims_generator.generate_batch(batch_size, timestamp)
        elif event_type == 'transactions':
            return self.transaction_generator.generate_batch(batch_size, timestamp)
        elif event_type == 'iot':
            return self.iot_generator.generate_batch(batch_size, timestamp)
        else:
            raise ValueError(f"Unknown event type: {event_type}")
    
    def _send_batch(self, topic: str, events: List[Dict[str, Any]]):
        """Send a batch of events to Kafka."""
        for event in events:
            try:
                # Use user_id or device_id as partition key for better distribution
                key = event.get('user_id', event.get('device_id', str(hash(event['timestamp']) % 1000)))
                
                # Serialize event
                value = json.dumps(event, default=str)
                
                # Send to Kafka
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback
                )
                
            except Exception as e:
                logger.error(f"Failed to send event to {topic}: {e}")
                with self.stats_lock:
                    self.stats.messages_failed += 1
    
    def _producer_worker(self, event_type: str, topic: str, events_per_second: int):
        """Worker function for producing events."""
        batch_size = min(100, events_per_second // 10)  # Adjust batch size
        batch_interval = batch_size / events_per_second
        
        logger.info(f"Starting {event_type} producer: {events_per_second} events/sec, batch size: {batch_size}")
        
        while not self.shutdown_event.is_set():
            try:
                start_time = time.time()
                
                # Generate and send batch
                events = self._generate_event_batch(event_type, batch_size)
                self._send_batch(topic, events)
                
                # Flush producer to ensure delivery
                self.producer.flush(timeout=0.1)
                
                # Calculate sleep time to maintain target rate
                elapsed = time.time() - start_time
                sleep_time = max(0, batch_interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in {event_type} producer: {e}")
                time.sleep(0.1)
    
    def _stats_reporter(self):
        """Report producer statistics periodically."""
        while not self.shutdown_event.is_set():
            time.sleep(10)  # Report every 10 seconds
            
            with self.stats_lock:
                current_time = time.time()
                
                if self.stats.start_time == 0:
                    self.stats.start_time = current_time
                    self.stats.last_report_time = current_time
                    continue
                
                # Calculate rates
                total_duration = current_time - self.stats.start_time
                report_duration = current_time - self.stats.last_report_time
                
                if report_duration > 0:
                    messages_per_sec = self.stats.messages_sent / total_duration
                    bytes_per_sec = self.stats.bytes_sent / total_duration
                    
                    logger.info(
                        f"Producer Stats - "
                        f"Messages: {self.stats.messages_sent:,} "
                        f"({messages_per_sec:.1f}/sec), "
                        f"Failed: {self.stats.messages_failed:,}, "
                        f"Bytes: {self.stats.bytes_sent:,} "
                        f"({bytes_per_sec/1024/1024:.2f} MB/sec)"
                    )
                
                self.stats.last_report_time = current_time
    
    def start(self):
        """Start the high-throughput producer."""
        if self.running:
            logger.warning("Producer is already running")
            return
        
        self.running = True
        self.shutdown_event.clear()
        
        # Reset statistics
        with self.stats_lock:
            self.stats = ProducerStats()
        
        # Calculate events per second for each type
        # Distribution: 40% transactions, 35% claims, 25% IoT
        transaction_eps = int(self.target_throughput * 0.4)
        claims_eps = int(self.target_throughput * 0.35)
        iot_eps = int(self.target_throughput * 0.25)
        
        logger.info(f"Starting producer with target throughput: {self.target_throughput:,} events/sec")
        logger.info(f"Distribution - Transactions: {transaction_eps:,}, Claims: {claims_eps:,}, IoT: {iot_eps:,}")
        
        # Start worker threads
        self.workers = [
            threading.Thread(
                target=self._producer_worker,
                args=('transactions', 'financial-transactions', transaction_eps),
                name='TransactionProducer'
            ),
            threading.Thread(
                target=self._producer_worker,
                args=('claims', 'insurance-claims', claims_eps),
                name='ClaimsProducer'
            ),
            threading.Thread(
                target=self._producer_worker,
                args=('iot', 'iot-telemetry', iot_eps),
                name='IoTProducer'
            ),
            threading.Thread(
                target=self._stats_reporter,
                name='StatsReporter'
            )
        ]
        
        for worker in self.workers:
            worker.start()
        
        logger.info("All producer workers started")
    
    def stop(self):
        """Stop the producer gracefully."""
        if not self.running:
            logger.warning("Producer is not running")
            return
        
        logger.info("Stopping producer...")
        self.shutdown_event.set()
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5)
        
        # Final flush
        self.producer.flush(timeout=5)
        
        self.running = False
        logger.info("Producer stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current producer statistics."""
        with self.stats_lock:
            current_time = time.time()
            duration = current_time - self.stats.start_time if self.stats.start_time > 0 else 0
            
            return {
                'messages_sent': self.stats.messages_sent,
                'messages_failed': self.stats.messages_failed,
                'bytes_sent': self.stats.bytes_sent,
                'duration_seconds': duration,
                'messages_per_second': self.stats.messages_sent / duration if duration > 0 else 0,
                'bytes_per_second': self.stats.bytes_sent / duration if duration > 0 else 0,
                'success_rate': self.stats.messages_sent / (self.stats.messages_sent + self.stats.messages_failed) 
                              if (self.stats.messages_sent + self.stats.messages_failed) > 0 else 0,
                'running': self.running
            }

def main():
    """Main function for running the producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='High-throughput Kafka producer')
    parser.add_argument('--throughput', type=int, default=50000,
                       help='Target throughput in events per second')
    parser.add_argument('--duration', type=int, default=0,
                       help='Duration to run in seconds (0 for indefinite)')
    
    args = parser.parse_args()
    
    producer = HighThroughputProducer(target_throughput=args.throughput)
    
    try:
        producer.start()
        
        if args.duration > 0:
            logger.info(f"Running for {args.duration} seconds...")
            time.sleep(args.duration)
        else:
            logger.info("Running indefinitely. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        producer.stop()
        
        # Final stats
        final_stats = producer.get_stats()
        logger.info(f"Final stats: {final_stats}")

if __name__ == "__main__":
    main()