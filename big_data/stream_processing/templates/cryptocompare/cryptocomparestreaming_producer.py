from datetime import datetime, timedelta
from confluent_kafka import Producer
import websocket
import argparse
import logging
import socket
import json

# Auxiliary functions
#

def process_data(data, subs_message, producer, topic):
  ''' This auxiliary function takes the data coming from CryptoCompare.

          Parameters:
                  data (string): what comes from CryptoCompare, a JSON document.
                  subs_message (string): the subscription message just in case is needed
                                         to decide the kind of processing to apply.
                  producer (Producer): a Producer instance to send events to a Kafka broker.
                  topic (string): the Kafka topic where the event is going to be sent over.

          Returns:
                  nothing
  '''
  # 1. String(JSON) to Python object format
  d_data = json.loads(data)
  # 2. Python object format to CSV record
  csv_record = ""
  if d_data['TYPE']=="5":
    market = d_data['MARKET'] if 'MARKET' in d_data.keys() else "-"
    fromsymbol = d_data['FROMSYMBOL'] if 'FROMSYMBOL' in d_data.keys() else "-"
    price = d_data['PRICE'] if 'PRICE' in d_data.keys() else 0.0
    lastupdate = d_data['LASTUPDATE'] if 'LASTUPDATE' in d_data.keys() else 0

    csv_record = "%s|%s|%s|%f|%d" % \
                 (d_data['TYPE'],market,fromsymbol,price,lastupdate)
  elif d_data['TYPE']=="2":
    # implement the logic
    pass
  if csv_record!="":
    # 3. Display the CSV record
    print (csv_record)
    # 4. Publish the CSV record in the Kafka topic.
    producer.produce(topic, value=csv_record)
    producer.flush()

# Classes with the logic
#

class CryptoCompareStreaming:
  '''
  Class handling the interaction with CryptoCompare via the Websockets interface.

  Attributes
  ----------
  APIURL : string
      constant representing the Websocket end point; it contains the api_key query
      parameter that has to be provided at connection time.
  _api_key : string
      API key that has to be provided to access the service.
  _websocket : WebSocketApp
      the WebSocketApp instance dealing with the Websocket interface.
  _subs_message : string
      subscription message to tell CryptoCompare what we want to get out of all 
      the information available.
  _producer : Producer
      the Producer instance doing all the interaction with the Kafka broker.
  _broker : string
      Kafka broker where the events are going to be sent over.
  _topic : string
      Kafka topic where the event is going to be published.
  _last_message : timestamp
      it keeps when the last processed message was processed (used for throttling)
  _throttle : int
      seconds to wait before processing data coming from CryptoCompare (used for
      throttling)

  Methods
  -------
  __init__
      class constructor
  connect
      method doing the connection to the CryptoCompare API and other
      initializations (Kafka producer and other attributes)
  on_open
      method executed when the connection to CryptoCompare is open; it's
      used to send the subscription message.
  on_open
      method executed when the connection to CryptoCompare is open; it's
  on_message
      method executed when a message from CryptoCompare is coming.
  on_error
      method executed when an error in the interaction with CryptoCompare happens.
  on_close
      method executed when the connection to CryptoCompare is closed.
  run_forever
      method that starts the loop which waits for data from CryptoCompare.
  '''

  APIURL = "wss://streamer.cryptocompare.com/v2?api_key=%s"
  _api_key = ""
  _websocket = None
  _subs_message = ""
  _producer = None
  _broker = ""
  _topic = ""
  _last_message = None
  _throtle = 5

  def __init__(self, api_key, subs_message, throttle):
    ''' Class constructor

            Parameters:
                    api_key (string): API key to access CryptoCompare
                    subs_message (string): subscription message to get the data of interest
                    throttle (int): number of seconds to wait before processing data coming in

            Returns:
                    a new initialized instance.
    '''
    self._api_key = api_key
    self._subs_message = subs_message
    self._throttle = throttle

    logging.debug("__init__: New CryptoCompareStreaming instance with ApiKey '%s' has been created" % self._api_key)

  def connect(self, broker, topic):
    ''' This method initializes the connection to the CryptoCompare websocket API.

            Parameters:
                    broker (string): URL of the Kafka broker where data will be sent over.
                    topic (string): Kafka topic where events will be published into.

            Returns:
                    nothing
    '''
    logging.debug("connect: Connecting to CryptoCompare streaming API")

    conf = {'bootstrap.servers': broker,
            'client.id': socket.gethostname()}

    self._webSocket = websocket.WebSocketApp(self.APIURL % self._api_key,
                                             on_open = self.on_open,
                                             on_message = self.on_message,
                                             on_error = self.on_error,
                                             on_close = self.on_close)
    self._broker = broker
    self._topic = topic
    self._producer = Producer(conf)
    self._last_message = datetime.now()

    logging.info("connect: Connected to CryptoCompare streaming API")

  def on_open(self, ws):
    ''' This method is called when the connection to CryptoCompare is established; it's
        used to send the subscription message.

            Parameters:
                    ws (WebSocket): reference to the established websocket

            Returns:
                    nothing
    '''
    logging.debug("on_open: method has been called")

    ws.send(json.dumps(self._subs_message))
    
  def on_message(self, ws, data):
    ''' This method is called when the connection to CryptoCompare is established; it's
        used to send the subscription message.

            Parameters:
                    ws (WebSocket): reference to the established websocket
                    data (string): data coming from CryptoCompare; the string contains a
                                   JSON document.

            Returns:
                    nothing
    '''
    logging.debug("on_message: %s" % data)

    b_process_data = datetime.now() > (self._last_message + timedelta(seconds=self._throttle))
    if b_process_data:
      process_data(data, self._subs_message, self._producer, self._topic)
      self._last_message = datetime.now()
      
  def on_error(self, ws, error):
    ''' This method is called when there is any error on the connection with CryptoCompare,
        although no logic has been implemented at the moment.

            Parameters:
                    ws (WebSocket): reference to the established websocket
                    error (?): instance containing information about the error

            Returns:
                    nothing
    '''
    logging.error("on_error: method has been called with error '%s'" % error)

  def on_close(self, ws):
    ''' This method is called when the connection with CryptoCompare is closed,
        although no logic has been implemented at the moment.

            Parameters:
                    ws (WebSocket): reference to the established websocket

            Returns:
                    nothing
    '''
    logging.debug("on_close: method has been called")

  def run_forever(self):
    ''' This method starts the main loop of the websocket which waits for data to
        arrive, among other things.

            Parameters:
                    None

            Returns:
                    nothing
    '''
    logging.debug("run_forever: main loop started.")

    self._webSocket.run_forever()

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument("api_key", help="API key to query CryptoCompare's API")
  parser.add_argument("subs_file", help="Path to the JSON file containing the subscription message")
  parser.add_argument("-b", "--broker",
                      help="server:port of the Kafka broker where messages will be published")
  parser.add_argument("-t", "--topic",
                      help="topic where messages will be published")
  parser.add_argument("-th", "--throttle",
                      help="the number of seconds to wait before processing a message coming in")
  args = parser.parse_args()

  # 1. Get the subscription message from a file
  with open(args.subs_file, "r") as jsonFile:
    subs_message = json.load(jsonFile)
  # 2. Create the connection to the streaming API
  conn = CryptoCompareStreaming(args.api_key, subs_message, int(args.throttle))
  conn.connect(args.broker,args.topic)
  conn.run_forever()
