from confluent_kafka import Consumer, KafkaError
#import csv
import signal
import sys

def cleanup():
    print("Cleanup function is executing...")
    consumer.close()

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
    'group.id': 'csv-consumer-group-new',  # Use a new consumer group to avoid old offsets
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
}

# Create Kafka consumer
consumer = Consumer(conf)
consumer.subscribe(['sample-stream'])

print(f'Kafka Consumer listening at: {conf["bootstrap.servers"]}')
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("No message yet..")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'End of partition reached {msg.topic()} [{msg.partition()}]')
            elif msg.error():
                print(f'Error occurred: {msg.error().str()}')
        else:
            # Process message
            record = msg.value().decode('utf-8').split(',')
            print(f'Consumed record: {record}')
            consumer.commit()  # Commit the message as processed
except Exception as e:
    print(f"Exception caught: {e}")
    cleanup()


# Output file configuration (optional)
output_file = 'output.csv'

with open(output_file, 'w', newline='') as csvfile:
    # writer = csv.writer(csvfile)
    # writer.writerow(['id', 'name', 'value'])  # Header
    print(f'Kafka Consumer listening at: {conf["bootstrap.servers"]}')
    # print(f'Wrote header record to: {output_file}')
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("No message yet..")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'End of partition reached {msg.topic()} [{msg.partition()}]')
            elif msg.error():
                print(f'Error occurred: {msg.error().str()}')
        else:
            # Process message
            record = msg.value().decode('utf-8').split(',')
            # writer.writerow(record)
            print(f'Consumed record: {record}')
            consumer.commit()  # Commit the message as processed

consumer.close()