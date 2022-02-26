import tweepy
from tweepy import OAuthHandler
from tweepy.streaming import Stream
from kafka import KafkaConsumer, KafkaProducer
import socket

ACCES_TOKEN = ""
ACCESS_SECRET = ""
CONSUMER_KEY = ""
CONSUMER_SECRET = ""

kafka_producer = KafkaProducer(bootstrap_servers="localhost:9092", client_id=socket.gethostname())
class StdOutListener(tweepy.Stream):
    def on_data(self, data):
        kafka_producer.send("tweets", value=data).get(timeout=10)
        print(data)
        return True
    def on_error(self, status):
        print(status)

        
l = StdOutListener(CONSUMER_KEY, CONSUMER_SECRET, ACCES_TOKEN, ACCESS_SECRET)
l.filter(track=["btc"])
