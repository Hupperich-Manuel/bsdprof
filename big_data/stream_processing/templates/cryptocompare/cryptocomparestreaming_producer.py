from datetime import datetime, timedelta
from confluent_kafka import Producer
import websocket
import argparse
import logging
import json

class CryptoCompareStreaming:

  APIURL = "wss://streamer.cryptocompare.com/v2?api_key=%s"
  _api_key = ""
  _websocket = None
  _topic = ""
  _producer = None
  _subs_message = ""

  _last_message = None
  _throtle = 5

  def __init__(self, api_key, subs_message, throtle):
    logging.basicConfig(level=logging.INFO)
    self._api_key = api_key
    self._subs_message = subs_message
    self._throtle = throtle
    logging.debug("__init__: New CryptoCompareStreaming instance with ApiKey '%s' has been created" % self._api_key)

  def connect(self, brokers, topic):
    logging.debug("connect: Connecting to CryptoCompare streaming API")
    self._webSocket = websocket.WebSocketApp(self.APIURL % self._api_key,
                                             on_open = self.on_open,
                                             on_message = self.on_message,
                                             on_error = self.on_error,
                                             on_close = self.on_close)
    #self._producer = Producer({'bootstrap.servers': brokers})
    self._topic = topic
    self._last_message = datetime.now()
    logging.info("connect: Connected to CryptoCompare streaming API")

  def on_open(self, ws):
    logging.debug("on_open method has been called")
    ws.send(json.dumps(self._subs_message))
    
  def on_message(self, ws, data):
    logging.debug("on_message: %s" % data)
    if datetime.now() > (self._last_message + timedelta(seconds=self._throtle)):
      message = data_processing(data)
      logging.debug("on_message: %s" % message)
      self._last_message = datetime.now()
      
  def on_error(self, ws, error):
    logging.debug("on_error method has been called")

  def on_close(self, ws):
    logging.debug("on_close method has been called")

  def run_forever(self):
    self._webSocket.run_forever()

def data_processing(data):
  print ("Data has been processed")

  return data

# main
parser = argparse.ArgumentParser()
parser.add_argument("api_key", help="API key to query CryptoCompare's API")
parser.add_argument("subs_file", help="Path to the JSON file containing the subscription message")
args = parser.parse_args()

# 1. Get the subscription message from a file
with open(args.subs_file, "r") as jsonFile:
  subs_message = json.load(jsonFile)
# 2. Create the connection to the streaming API
conn = CryptoCompareStreaming(args.api_key, subs_message, 5)
conn.connect('localhost','cryptocompare')
conn.run_forever()
