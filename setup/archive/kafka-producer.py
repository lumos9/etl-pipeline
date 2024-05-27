from confluent_kafka import Producer

import signal
import sys

def cleanup():
    print("Cleanup function is executing...")
    producer.flush()

def handle_signal(signum, frame):
    print(f"Signal {signum} caught. Exiting gracefully...")
    cleanup()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)  #

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'csv-producer',
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.kbytes': 1048576,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 100000,
    'compression.type': 'gzip',
}

# Create Kafka producer
producer = Producer(conf)

# Acknowledgement callback
def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced: {msg.key()}")

# Number of records to produce
num_records = 1_000_000

# Produce records
for i in range(num_records):
    record = f'{i},name_{i},value_{i}'
    try:
        producer.produce('sample-stream', key=str(i), value=record, callback=acked)
    except BufferError:
        print("Queue is full, waiting for free space...")
        producer.poll(1)  # Waits for 1 second and serves delivery reports

    producer.poll(0)  # Poll to serve delivery reports and free up space

# Ensure all messages are delivered before exiting
producer.flush()