from mobilitylabs.parkingemtmad import ParkingEMTMad
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
def get_feature(l_features, feature_name):
  ''' Function to get features from parkings.

            Parameters:
                    l_features (list): list of JSON documents with the features of a parking.
                    feature_name (string): name of the feature to retrieve; the 'name' property contains
                                           the name of the feature, and the 'content' property contains the
                                           value.

            Returns:
                    the value of the feature or None if the feature doesn't exist.
  '''

  for feature in l_features:
    if feature_name == feature['name']:
      return feature['content']

  return None

def get_occupation(occupation):
  ''' Function to get the occupation of a parking if exists

            Parameters:
                    ocupation (list): list with a JSON document containing the occupation of a parking. Those
                                      parkings with no occupation info will have None as value of this field.

            Returns:
                    a number with the free spaces or -1 if no occupation info is available.
  '''

  if occupation != None:
    return occupation[0]['free']

  return -1

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
        if action == "info_parking":
          total_spaces = get_feature(item['features'],'Total')
          occupation = get_occupation(item['occupation'])
          csv_record = "%d|%s|%s|%s|%s|%d" % \
                        (item['id'],item['nickName'],item['address'],\
                          item['administrativeArea'],total_spaces,occupation)
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

          Returns:
                  This function doesn't return anything.
  '''

  print ("Results of action '%s':" % action)
  pp = pprint.PrettyPrinter(indent=2)
  pp.pprint(content)

# Decorators on functions calling the REST API
#

def info_parkings_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of parkings and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of parkings and executing the
                    'action_func' function on the results.
  '''
  def info_parkings(parking_service):
    ''' Function retrieving information of parkings and executing the 'action_func'
        function on the results.

              Parameters:
                      parking_service (ParkingEMTMad): instance leveraging the Parking EMT service.

              Returns:
                      Nothing
    '''
    info_parkings = parking_service.info_parkings()

    if info_parkings!=None:
      action_func(info_parkings, "info_parkings")
    else:
      print("The ParkingEMTMad service didn't return any data.")

  return info_parkings

def info_parking_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of a specific parking and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of a parking and executing the
                    'action_func' function on the results.
  '''
       
  def info_parking(parking_service, parking_id):
    ''' Function retrieving information of a parking and executing the 'action_func'
        function on the results.

              Parameters:
                      parking_service (ParkingEMTMad): instance leveraging the Parking EMT service.
                      parking_id (integer): identifier of the parking.

              Returns:
                      Nothing
    '''

    info_parking = parking_service.info_parking(parking_id)

    if info_parking!=None:
      action_func(info_parking, "info_parking")
    else:
      print("The ParkingEMTMad service didn't return any data.")

  return info_parking

def availability_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      availability of parkings and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve availability of parkings and executing the
                    'action_func' function on the results.
  '''
  def availability(parking_service):
    ''' Function retrieving availability of parkings and executing the 'action_func'
        function on the results.

              Parameters:
                      parking_service (ParkingEMTMad): instance leveraging the Parking EMT service.

              Returns:
                      Nothing
    '''
    availability = parking_service.availability()

    if availability!=None:
      action_func(availability, "availability")
    else:
      print("The ParkingEMTMad service didn't return any data.")

  return availability

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
  parser.add_argument("action", choices=['info_parkings', 'info_parking', 'availability'],
                      help="what is going to be requested to the ParkingEMTMad service")
  parser.add_argument("credentials_file", help="path to the file with info to access the service")
  parser.add_argument("-id", "--parking_id", 
                      help="parking area identifier for action 'info_parking'")
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
  parking_service = ParkingEMTMad(x_client_id, pass_key)
  parking_service.log_in()

  # Action dispatching if credentials logged the client into the service
  #
  if (parking_service.is_logged_in()):

    action_function = build_kafka_producer(args.broker, args.topic)
    if action_function==None:
      action_function = pprint_action;

    if args.action == "info_parkings":
      logging.debug("x_client_id '%s' asking for parking areas information" % x_client_id)
      info_parkings = info_parkings_decorator(action_function)
      info_parkings(parking_service)
    elif args.action == "info_parking":
      if args.parking_id!=None:
        logging.debug("x_client_id '%s' asking for information for parking area '%s'" % 
                      (x_client_id, args.parking_id))
        info_parking = info_parking_decorator(action_function)
        info_parking(parking_service, args.parking_id)
      else:
        logging.error("A parking area identifier has to be provided")
        sys.exit("A parking area identifier has to be provided for action 'info_parking'")
    elif args.action == "availability":
      logging.debug("x_client_id '%s' asking for availability in parking areas" % x_client_id)
      availability = availability_decorator(action_function)
      availability(parking_service)
  else:
    logging.error("Unsuccessful login with x_client_id '%s%'" % x_client_id)
    sys.exit("Unsuccessful login with x_client_id '%s%'" % x_client_id)
