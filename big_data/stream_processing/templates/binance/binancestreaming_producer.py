from binance.websocket.spot.websocket_client import SpotWebsocketClient as WebsocketClient
from confluent_kafka import Producer
import argparse
import logging
import socket
import json

# Auxiliary functions
#

def event_to_csv(event_type, event_raw):
  line = ""

  if event_type == "kline":
    ts = event_raw['k']['t']
    symbol = event_raw['k']['s']
    interval = event_raw['k']['i']
    open = event_raw['k']['o']
    close = event_raw['k']['c']
    high = event_raw['k']['h']
    low = event_raw['k']['l']
    volume = event_raw['k']['v']
    line = f"{ts}|{symbol}|{interval}|{open}|{close}|{high}|{low}|{volume}"

  return line

def binance_callback_decorator(producer, topic):

  def stream_callback(message):
    logging.debug(f"message={message}")
    if message != None:
      event_type = message['e']
      # 1. Log the message for debugging purposes
      logging.info(f"{event_to_csv(event_type, message)}")
      # 2. Publish the CSV record in the kafka topic if the producer is set up
      if producer != None:
        producer.produce(topic, value=event_to_csv(event_type, message))
        producer.flush()

  return stream_callback

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument("stream", help="Stream to subscribe: ticker, kline_<interval> (1m, 5m)")
  parser.add_argument("symbols", help="Comma-separated list of symbols (ex. btcbusd, ethbusd)")
  parser.add_argument("-b", "--broker",
                      help="server:port of the Kafka broker where messages will be published")
  parser.add_argument("-t", "--topic",
                      help="topic where messages will be published")
  parser.add_argument("-th", "--throttle",
                      help="the number of seconds to wait before processing a message coming in")
  args = parser.parse_args()

  # 1. Check stream provided
  if args.stream in ["ticker", "kline_1m", "kline_5m"]:
    # a. Create Kafka producer
    producer = None
    topic = args.topic
    if args.broker != None:
      conf = {'bootstrap.servers': args.broker,
              'client.id': socket.gethostname()}
      producer = Producer(conf)
    # b. Create the websocket client to Binance
    ws_client = WebsocketClient()
    ws_client.start()
    # c. Go over symbols and register the callback function to the stream
    for symbol_raw in args.symbols.split(","):
      symbol = symbol_raw.strip()
      logging.info(f"Subscribing symbols to stream '{symbol}@{args.stream}'")
      ws_client.instant_subscribe(stream = f"{symbol}@{args.stream}",
                                  callback = binance_callback_decorator(producer, topic))
  else:
    print(f"ERROR: '{args.stream}' is not a valid stream.")
