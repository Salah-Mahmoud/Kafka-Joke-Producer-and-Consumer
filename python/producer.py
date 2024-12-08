import json
import requests
from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(
    bootstrap_servers='localhost:9092 ',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_joke():
    url = 'https://v2.jokeapi.dev/joke/Programming?type=single'
    response = requests.get(url)
    data = response.json()
    return data

def produce_data():
    while True:
        joke = fetch_joke()
        producer.send('JokeTest', value=joke)
        print(f"Sent joke to Kafka: {joke}")
        sleep(10)

if __name__ == "__main__":
    produce_data()
