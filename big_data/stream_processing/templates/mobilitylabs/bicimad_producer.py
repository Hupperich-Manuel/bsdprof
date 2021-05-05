from mobilitylabs.bicimad import BiciMad
from confluent_kafka import Producer
import configparser
import argparse
import logging
import pprint
import socket
import sys
import os

# Auxiliary functions
#

# Action functions for the functions calling the REST API
#
def kafka_producer_decorator(broker, topic):
  ''' Decorator (function that takes another function) to create functions ready
      to publish messages to a specific topic and Kafka broker.

            Parameters:
                    broker (string): host:port of the Kafka broker
                    topic (string): topic name where the a message is going to be published

            Returns:
                    A function ready to publish messages to a specific topic and Kafka broker.
  '''

  def kafka_producer_action(content, action):
    ''' Function with the logic to publish messages into a Kafka topic. THIS IS THE FUNCTION
        WHERE YOU HAVE TO WORK ON.

            Parameters:
                    content (list): List of dictionaries (JSON) documents; if only one JSON
                                    document is returned from the web service, the list will
                                    only have 1 element.
                    action (string): The action specified by the user in the command line.

            Returns:
                    This function doesn't return anything.
    '''
    conf = {'bootstrap.servers': broker,
            'client.id': socket.gethostname()}
    csv_record= ""

    if isinstance(content, list):

      producer = Producer(conf) 
      # 1. JSON to CSV format
      for item in content:
        if action == "info_bike" or action == "info_bikes":
          csv_record = "%d|%s|%s|%d|%s" % \
                       (item['qrcode'],item['datePosition'],item['Tracker-ioMovement'],\
                        item['Speed'],item['state'])
        # 2. Display the CSV record
        print(csv_record)
        # 3. Publish the CSV record in the kafka topic
        producer.produce(topic, value=csv_record)
        producer.flush()
    else:
      print("No contents received. Nothing will be published into the topic.")
      logging.info("No contents received. Nothing will be published into the topic.")

  return kafka_producer_action

def pprint_action(content, action):
  ''' Function displaying the content on the standard output (screen). Useful for debugging
      purposes.

          Parameters:
                  content (list): List of dictionaries (JSON) documents; if only one JSON
                                  document is returned from the web service, the list will
                                  only have 1 element.
                  action (string): The action specified by the user in the command line.

          Returns:
                  This function doesn't return anything.
  '''

  print ("Results of action '%s':" % action)
  pp = pprint.PrettyPrinter(indent=2)
  pp.pprint(content)

# Decorators on functions calling the REST API
#
def info_bike_stations_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bike stations and executing the 'action_func' function on the 
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bike stations and executing the
                    'action_func' function on the results.
  '''

  def info_bike_stations(bicimad_service):
    ''' Function retrieving information of bike stations and executing the 'action_func' 
        function on the results.

              Parameters:
                      bicimad_service (BiciMad): instance leveraging the BiciMAD GO service.

              Returns:
                      Nothing
    '''

    info_bike_stations = bicimad_service.info_bike_stations()

    if info_bike_stations!=None:
      action_func(info_bike_stations, "info_bike_stations")
    else:
      print("The BiciMAD GO service didn't return any data.")

  return info_bike_stations

def info_bike_station_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of a particular bike station and executing the 'action_func' function
      on the results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of a particular bike stations and 
                    executing the 'action_func' function on the results.
  '''

  def info_bike_station(bicimad_service, bike_station_id):
    ''' Function retrieving information of a particular bike station and executing the 
        'action_func' function on the results.

              Parameters:
                      bicimad_service (BiciMad): instance leveraging the BiciMAD GO service.
                      bike_station_id (integer): identifier of the specific bike station.

              Returns:
                      Nothing
    '''

    info_bike_station = bicimad_service.info_bike_station(bike_station_id)

    if info_bike_station!=None:
      action_func(info_bike_station, "info_bike_station")
    else:
      print("The BiciMAD GO service didn't return any data.")

  return info_bike_station

def info_bikes_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bikes and executing the 'action_func' function on the results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bikes and executing the
                    'action_func' function on the results.
  '''

  def info_bikes(bicimad_service):
    ''' Function retrieving information of bikes and executing the 'action_func'
        function on the results.

              Parameters:
                      bicimad_service (BiciMad): instance leveraging the BiciMAD GO service.

              Returns:
                      Nothing
    '''

    info_bikes = bicimad_service.info_bikes()

    if info_bikes!=None:
      action_func(info_bikes, "info_bikes")
    else:
      print("The BiciMAD GO service didn't return any data.")

  return info_bikes

def info_bike_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of a particular bike and executing the 'action_func' function on the results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of a particular bike and
                    executing the 'action_func' function on the results.
  '''

  def info_bike(bicimad_service, bike_id):
    ''' Function retrieving information of a particular bike and executing the
        'action_func' function on the results.

              Parameters:
                      bicimad_service (BiciMad): instance leveraging the BiciMAD GO service.
                      bike_id (integer): identifier of the specific bike.

              Returns:
                      Nothing
    '''

    info_bike= bicimad_service.info_bike(bike_id)

    if info_bike!=None:
      action_func(info_bike, "info_bike")
    else:
      print("The BiciMAD GO service didn't return any data.")

  return info_bike

def build_kafka_producer(broker, topic):
  ''' This function creates a function ready to produce messages into Kafka.

            Parameters:
                    broker (string): host:port of the Kafka broker
                    topic (string): topic name where the a message is going to be published

            Returns:
                    A function ready to produce messages into a Kafka topic or None if no broker nor topic
                    are not provided.
  '''
  if broker!=None and topic!=None:
    logging.debug("Application executed as a Kafka producer")
    return kafka_producer_decorator(broker, topic)
  else:
    logging.debug("Application not executed as a Kafka producer")

  return None


if __name__ == "__main__":
  # Setting up debuging level and debug file with environment variables
  #
  debug_level = os.environ.get('MOBILITYLABS_DEBUGLEVEL',logging.WARN)
  debug_file = os.environ.get('MOBILITYLABS_DEBUGFILE')

  if debug_file==None:
    logging.basicConfig(level=debug_level)
  else:
    logging.basicConfig(filename=debug_file, filemode='w', level=debug_level)

  parser = argparse.ArgumentParser()
  parser.add_argument("action", choices=['info_bike_stations', 'info_bike_station', 'info_bikes', 'info_bike'],
                      help="what is going to be requested to the BiciMAD GO service")
  parser.add_argument("credentials_file", help="path to the file with info to access the service")
  parser.add_argument("-sid", "--bike_station_id",
                      help="bike station identifier for action 'info_bike_station'")
  parser.add_argument("-bid", "--bike_id",
                      help="bike identifier for action 'info_bike'")
  parser.add_argument("-b", "--broker",
                      help="server:port of the Kafka broker where messages will be published")
  parser.add_argument("-t", "--topic",
                      help="topic where messages will be published")
  args = parser.parse_args()

  # Read credentials to instantiate the class to use the service
  #
  credentials = configparser.ConfigParser()
  credentials.read(args.credentials_file)

  x_client_id = credentials['DEFAULT']['x_client_id']
  pass_key = credentials['DEFAULT']['pass_key']
  bicimad_service = BiciMad(x_client_id, pass_key)
  bicimad_service.log_in()

  # Action dispatching if credentials logged the client into the service
  #
  if (bicimad_service.is_logged_in()):

    action_function = build_kafka_producer(args.broker, args.topic)
    if action_function==None:
      action_function = pprint_action;

    if args.action == "info_bike_stations":
      logging.debug("x_client_id '%s' asking for bike stations information" % x_client_id)
      info_bike_stations = info_bike_stations_decorator(action_function)
      info_bike_stations(bicimad_service)
    elif args.action == "info_bike_station":
      if args.bike_station_id!=None:
        logging.debug("x_client_id '%s' asking for information for bike station '%s'" %
                      (x_client_id, args.bike_station_id))
        info_bike_station = info_bike_station_decorator(action_function)
        info_bike_station(bicimad_service, args.bike_station_id)
      else:
        logging.error("A bike station identifier has to be provided")
        sys.exit("A bike station identifier has to be provided for action 'info_bike_station'")
    elif args.action == "info_bikes":
      logging.debug("x_client_id '%s' asking for bikes information" % x_client_id)
      info_bikes = info_bikes_decorator(action_function)
      info_bikes(bicimad_service)
    elif args.action == "info_bike":
      if args.bike_id!=None:
        logging.debug("x_client_id '%s' asking for information for bike '%s'" %
                      (x_client_id, args.bike_id))
        info_bike = info_bike_decorator(action_function)
        info_bike(bicimad_service, args.bike_id)
      else:
        logging.error("A bike identifier has to be provided")
        sys.exit("A bike identifier has to be provided for action 'info_bike'")
    else:
      logging.error("Unsuccessful login with x_client_id '%s%'" % XClientId)
      sys.exit("Unsuccessful login with x_client_id '%s%'" % XClientId)

