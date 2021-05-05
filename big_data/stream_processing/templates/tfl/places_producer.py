from transportforlondon.places import Places
from confluent_kafka import Producer
import argparse
import logging
import pprint
import socket
import sys
import os

# Auxiliary functions
#
def getAdditionalProperties(l_additional_props, l_keys_to_retrieve):
  ''' Auxiliar function that helps to get some keys from the list of additional properties,
      which is actually a list of JSON documents and it's not easy to query by key name.
      to publish messages to a specific topic and Kafka broker.

            Parameters:
                    l_additional_props (list): list of JSON documents provided by the service.
                    l_keys_to_retrieve (list): list of the JSON documents we are interested in,
                                               which is specified by provided the key of every
                                               JSON document.

            Returns:
                    a dictionary containing they key and the value of the JSON documents of
                    interest.
  '''

  d_kv = {}

  # 1. Go over all the additional properties.
  for additional_prop in l_additional_props:

    # 2. Get the key of the current additional property and check it with the list of interest.
    key = additional_prop['key']
    if key in l_keys_to_retrieve:
      # 3. Add the key and value to the dictionary if it's in the list.
      d_kv[key] = additional_prop['value']

  return d_kv

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

    producer = Producer(conf)

    if isinstance(content, list):
      for item in content:
        # 1. JSON to CSV format
        if action == "info_charge_connectors":
          d_props = getAdditionalProperties(item['additionalProperties'],\
                                            ['ConnectorType','ConnectorDescription','Power'])
          csv_record = "%s|%s|%s|%s|%s" % \
                       (item['id'],item['commonName'],d_props['ConnectorType'],\
                        d_props['ConnectorDescription'],d_props['Power'])
          # 2. Display the CSV record
          print(csv_record)
          # 3. Publish the CSV record in the kafka topic
          producer.produce(topic, value=csv_record)
          producer.flush()
    elif content!=None:
      pass
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
def info_charge_connectors_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bike stations and executing the 'action_func' function on the 
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bike stations and executing the
                    'action_func' function on the results.
  '''

  def info_charge_connectors(places_service):
    ''' Function retrieving information of charge connectors and executing the 'action_func' 
        function on the results.

              Parameters:
                      places_service: instance leveraging the Place API.

              Returns:
                      Nothing
    '''

    info_charge_connectors = places_service.info_charge_connectors()

    if info_charge_connectors!=None:
      action_func(info_charge_connectors, "info_charge_connectors")
    else:
      print("The Place API didn't return any data.")

  return info_charge_connectors

def info_charge_stations_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bike stations and executing the 'action_func' function on the 
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bike stations and executing the
                    'action_func' function on the results.
  '''

  def info_charge_stations(places_service):
    ''' Function retrieving the information of charge stations and executing the 'action_func' 
        function on the results.

              Parameters:
                      places_service: instance leveraging the Place API.

              Returns:
                      Nothing
    '''

    info_charge_stations = places_service.info_charge_stations()

    if info_charge_stations!=None:
      action_func(info_charge_stations, "info_charge_stations")
    else:
      print("The Place API didn't return any data.")

  return info_charge_stations

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
  parser.add_argument("action", choices=['info_charge_connectors', 'info_charge_stations'],
                      help="what is going to be requested to the Place API")
  parser.add_argument("-b", "--broker",
                      help="server:port of the Kafka broker where messages will be published")
  parser.add_argument("-t", "--topic",
                      help="topic where messages will be published")
  args = parser.parse_args()

  places_service = Places()
  action_function = build_kafka_producer(args.broker, args.topic)
  if action_function==None:
    action_function = pprint_action;

  if args.action == "info_charge_connectors":
    logging.debug("asking for info of all charge connectors")
    info_charge_connectors = info_charge_connectors_decorator(action_function)
    info_charge_connectors(places_service)
  elif args.action == "info_charge_stations":
    logging.debug("asking for info of all charge stations")
    info_charge_stations = info_charge_stations_decorator(action_function)
    info_charge_stations(places_service)
