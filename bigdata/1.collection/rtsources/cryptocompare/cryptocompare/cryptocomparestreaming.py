import logging
import websocket

class CryptoCompareStreaming:

  APIURL = "wss://streamer.cryptocompare.com/v2?api_key=%s"
  _apiKey = ""
  _webSocket = None

  def __init__(self, apiKey):
    self._apiKey = apiKey
    logging.debug("__init__: New CryptoCompareStreamint instance with ApiKey '%s' has been created" % self._apiKey)

  def connect(self):
    logging.debug("connect: Connecting to CryptoCompare streaming API")
    self._webSocket = websocket.WebSocketApp(self.APIURL % self._apiKey,
                                             on_open = self.on_open,
                                             on_message = self.on_message,
                                             on_error = self.on_error,
                                             on_close = self.on_close)
    logging.info("connect: Connected to CryptoCompare streaming API")

  def on_open(self, ws):
    logging.debug("on_open method has been called")
    pass
    
  def on_message(self, ws, message):
    logging.debug("on_message method has been called")
    pass

  def on_error(self, ws, error):
    logging.debug("on_error method has been called")
    pass

  def on_close(self, ws):
    logging.debug("on_close method has been called")
    pass

  def run_forever(self):
    self._webSocket.run_forever()
