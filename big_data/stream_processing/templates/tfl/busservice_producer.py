from transportforlondon.busservice import BusService
from confluent_kafka import Producer
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

    producer = Producer(conf)

    if isinstance(content, list):
      # Getting an array from the source
      for item in content:
        # 1. JSON to CSV format
        if action == "status_bus_lines" or action == "status_bus_line":
          status_severity = item['lineStatuses'][0]['statusSeverity'] 
          status_severity_desc = item['lineStatuses'][0]['statusSeverityDescription'] 
          service_type = item['serviceTypes'][0]['name'] 
          csv_record = "%s|%s|%s|%s|%s" % \
                       (item['name'],status_severity,status_severity_desc,\
                        service_type, item['modified'])
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
def info_bus_lines_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bike stations and executing the 'action_func' function on the 
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bike stations and executing the
                    'action_func' function on the results.
  '''

  def info_bus_lines(bus_service):
    ''' Function retrieving information of bus lines and executing the 'action_func' 
        function on the results.

              Parameters:
                      bus_service: instance leveraging the Line API.

              Returns:
                      Nothing
    '''

    info_bus_lines = bus_service.info_bus_lines()

    if info_bus_lines!=None:
      action_func(info_bus_lines, "info_bus_lines")
    else:
      print("The Line API didn't return any data.")

  return info_bus_lines

def status_bus_lines_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bike stations and executing the 'action_func' function on the 
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bike stations and executing the
                    'action_func' function on the results.
  '''

  def status_bus_lines(bus_service):
    ''' Function retrieving the status of bus lines and executing the 'action_func' 
        function on the results.

              Parameters:
                      bus_service: instance leveraging the Line API.

              Returns:
                      Nothing
    '''

    status_bus_lines = bus_service.status_bus_lines()

    if status_bus_lines!=None:
      action_func(status_bus_lines, "status_bus_lines")
    else:
      print("The Line API didn't return any data.")

  return status_bus_lines

def status_bus_line_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of a particular bike station and executing the 'action_func' function
      on the results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of a particular bike stations and 
                    executing the 'action_func' function on the results.
  '''

  def status_bus_line(bus_service, bus_line_id):
    ''' Function retrieving the status of a particular bus line and executing the 
        'action_func' function on the results.

              Parameters:
                      bus_service: instance leveraging the Line API.
                      bus_line_id (integer): identifier of the specific bus line.

              Returns:
                      Nothing
    '''

    status_bus_line = bus_service.status_bus_line(bus_line_id)

    if status_bus_line!=None:
      action_func(status_bus_line, "status_bus_line")
    else:
      print("The Line API didn't return any data.")

  return status_bus_line

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
  parser.add_argument("action", choices=['info_bus_lines', 'status_bus_lines', 'status_bus_line'],
                      help="what is going to be requested to the Line API")
  parser.add_argument("-bid", "--bus_line_id",
                      help="bus line identifier for action 'status_bus_line'")
  parser.add_argument("-b", "--broker",
                      help="server:port of the Kafka broker where messages will be published")
  parser.add_argument("-t", "--topic",
                      help="topic where messages will be published")
  args = parser.parse_args()

  bus_service = BusService()
  action_function = build_kafka_producer(args.broker, args.topic)
  if action_function==None:
    action_function = pprint_action;

  if args.action == "info_bus_lines":
    logging.debug("asking for all bus line")
    info_bus_lines = info_bus_lines_decorator(action_function)
    info_bus_lines(bus_service)
  elif args.action == "status_bus_lines":
    logging.debug("asking for status of all bus lines")
    status_bus_lines = status_bus_lines_decorator(action_function)
    status_bus_lines(bus_service)
  elif args.action == "status_bus_line":
    if args.bus_line_id!=None:
      logging.debug("asking for status of bus line '%s'" % args.bus_line_id)
      status_bus_line = status_bus_line_decorator(action_function)
      status_bus_line(bus_service, args.bus_line_id)
    else:
      logging.error("A bus line identifier has to be provided")
      sys.exit("A bus line identifier has to be provided for action 'status_bus_line'")
