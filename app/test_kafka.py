#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
TEST_TOPIC = 'test_topic'

def main():
    try:
        # Initialize the producer
        print("Initializing producer...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print("Producer initialized, sending a test message...")
        # Send a test message
        data = {"message": "Hello Kafka", "timestamp": time.time()}
        future = producer.send(TEST_TOPIC, data)
        
        # Wait for the result
        record_metadata = future.get(timeout=10)
        print("Message sent to {}: Partition={}, Offset={}".format(
            TEST_TOPIC, record_metadata.partition, record_metadata.offset))
        print("Data: {}".format(json.dumps(data, indent=2)))
        
        print("Test complete. Closing producer...")
        producer.close()
        print("Producer closed.")
        
    except Exception as e:
        print("Error: {}".format(str(e)))

if __name__ == "__main__":
    main() 