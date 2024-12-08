import json
from kafka import KafkaConsumer
from time import sleep

consumer = KafkaConsumer(
    'JokeTest',
    bootstrap_servers='localhost:9092',
    group_id='test-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)
def consume_data():
    for message in consumer:
        joke = message.value
        print(f"Received joke: {joke['joke']}")
        sleep(5)

if __name__ == "__main__":
    consume_data()
