from cryptocompare.cryptocomparestreaming import CryptoCompareStreaming
from confluent_kafka import Producer
import logging
import json
import argparse

class CCStreaming2Kafka(CryptoCompareStreaming):

  _subsMessage = ""
  _producer = None
  _topic = ""

  def __init__(self, apiKey, subsMessage):
    logging.basicConfig(level=logging.INFO)
    super().__init__(apiKey)
    self._subsMessage = subsMessage

  def connect(self, brokers, topic):
    super().connect()
    self._producer = Producer({'bootstrap.servers': brokers})
    self._topic = topic
    
  def on_open(self, ws):
    super().on_open(ws)
    ws.send(json.dumps(self._subsMessage))

  def on_message(self, ws, message):
    super().on_message(ws, message)
    logging.debug("on_message: %s" % message)
    self._producer.produce(self._topic, message)

# main
parser = argparse.ArgumentParser()
parser.add_argument("apiKey", help="API key to query CryptoCompare's API")
parser.add_argument("subsFile", help="Path to the JSON file containing the subscription message")
args = parser.parse_args()

# 1. Get the subscription message from a file
with open(args.subsFile, "r") as jsonFile:
  subsMessage = json.load(jsonFile)
# 2. Create the connection to the streaming API
conn = CCStreaming2Kafka(args.apiKey, subsMessage)
conn.connect('localhost','cryptocompare')
conn.run_forever()
