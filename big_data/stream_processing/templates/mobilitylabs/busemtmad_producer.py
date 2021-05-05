from mobilitylabs.busemtmad import BusEMTMad
from confluent_kafka import Producer
from datetime import datetime
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

    producer = Producer(conf)
    # 1. JSON to CSV format
    if isinstance(content, list):
      for item in content:
        if action == "buses_arrivals":
          # Iterate over item['Arrive'] containing info of buses arrivingin real-time
          for bus_arrival in item['Arrive']:
            csv_record = "%s|%d|%s|%d|%d|%s|%d" % \
                         (bus_arrival['stop'],bus_arrival['bus'],bus_arrival['line'],\
                          bus_arrival['estimateArrive'],bus_arrival['DistanceBus'],\
                          bus_arrival['destination'],bus_arrival['deviation'])
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
def info_lines_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bus lines and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bus lines and executing the
                    'action_func' function on the results.
  '''

  def info_lines(busemtmad_service):
    ''' Function retrieving information of bus lines and executing the 'action_func'
        function on the results.

              Parameters:
                      busemtmad_service (BusEMTMad): instance leveraging the Bus EMT service.

              Returns:
                      Nothing
    '''

    today_string = datetime.today().strftime('%Y%m%d')
    info_lines = busemtmad_service.info_lines(today_string)

    if info_lines!=None:
      action_func(info_lines, "info_lines")
    else:
      print("The BusEMTMad service didn't return any data.")

  return info_lines

def info_line_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of a bus line and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of a bus line and executing the
                    'action_func' function on the results.
  '''

  def info_line(busemtmad_service, line_id):
    ''' Function retrieving information of a bus line and executing the 'action_func'
        function on the results.

              Parameters:
                      busemtmad_service (BusEMTMad): instance leveraging the Bus EMT service.
                      line_id (integer): identifier of the bus line.

              Returns:
                      Nothing
    '''

    today_string = datetime.today().strftime('%Y%m%d')
    info_line = busemtmad_service.info_line(line_id, today_string)

    if info_line!=None:
      action_func(info_line, "info_line")
    else:
      print("The BusEMTMad  service didn't return any data.")

  return info_line

def info_stops_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bus stops and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bus stops and executing the
                    'action_func' function on the results.
  '''

  def info_stops(busemtmad_service):
    ''' Function retrieving information of bus stops and executing the 'action_func'
        function on the results.

              Parameters:
                      busemtmad_service (BusEMTMad): instance leveraging the Bus EMT service.

              Returns:
                      Nothing
    '''

    info_stops = busemtmad_service.info_stops()

    if info_stops!=None:
      action_func(info_stops, "info_stops")
    else:
      print("The BusEMTMad service didn't return any data.")

  return info_stops

def info_stop_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of a bus stop and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of a bus stop and executing the
                    'action_func' function on the results.
  '''

  def info_stop(busemtmad_service, stop_id):
    ''' Function retrieving information of a bus stop and executing the 'action_func'
        function on the results.

              Parameters:
                      busemtmad_service (BusEMTMad): instance leveraging the Bus EMT service.
                      stop_id (integer): identifier of the bus stop.

              Returns:
                      Nothing
    '''

    info_stop = busemtmad_service.info_stop(stop_id)

    if info_stop!=None:
      action_func(info_stop, "info_stop")
    else:
      print("The BusEMTMad  service didn't return any data.")

  return info_stop

def line_stops_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of bus stops in a bus line and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of bus stops in a bus line and
                    executing the 'action_func' function on the results.
  '''

  def line_stops(busemtmad_service, line_id, direction):
    ''' Function retrieving information of bus stops in a bus line and executing the 
        'action_func' function on the results.

              Parameters:
                      busemtmad_service (BusEMTMad): instance leveraging the Bus EMT service.
                      line_id (integer): identifier of the bus line
                      direction (integer): direction of the bus line considered 
                                           (1: start->end, 2: end->start)

              Returns:
                      Nothing
    '''

    line_stops = busemtmad_service.line_stops(line_id, direction)

    if line_stops!=None:
      action_func(line_stops, "line_stops")
    else:
      print("The BusEMTMad  service didn't return any data.")

  return line_stops

def issues_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      issues identified in a bus stop and executing the 'action_func' function on the
      results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve issues identified in a bus stop and executing
                    the 'action_func' function on the results.
  '''

  def issues(busemtmad_service, stop_id):
    ''' Function retrieving issues identified in a bus stop and executing the
        'action_func' function on the results.

              Parameters:
                      busemtmad_service (BusEMTMad): instance leveraging the Bus EMT service.
                      stop_id (integer): identifier of the bus stop

              Returns:
                      Nothing
    '''

    issues = busemtmad_service.issues(stop_id)

    if issues!=None:
      action_func(issues, "issues")
    else:
      print("The BusEMTMad  service didn't return any data.")

  return issues

def buses_arrivals_decorator(action_func):
  ''' Decorator (function that takes another function) to create a function retrieving
      information of buses arrivals in a bus stop and, optionally, for a specific bus line
      and executing the 'action_func' function on the results.

            Parameters:
                    action_func (function): the function that will be applied on the results.

            Returns:
                    A function ready to retrieve information of buses arrivals in a bus stop 
                    and, optionally, for a specific bus line the 'action_func' function on 
                    the results.
  '''

  def buses_arrivals(busemtmad_service, stop_id, line_id):
    ''' Function retrieving information of buses arrivals in a bus stop and, optionally, for a 
        specific bus line and executing the 'action_func' function on the results.

              Parameters:
                      busemtmad_service (BusEMTMad): instance leveraging the Bus EMT service.
                      stop_id (integer): identifier of the bus stop
                      line_id (integer): identifier of the bus line (optional)

              Returns:
                      Nothing
    '''

    if line_id!=None:
      buses_arrivals = busemtmad_service.buses_arrivals(stop_id, line_id)
    else:
      buses_arrivals = busemtmad_service.buses_arrivals(stop_id)

    if buses_arrivals!=None:
      action_func(buses_arrivals, "buses_arrivals")
    else:
      print("The BusEMTMad  service didn't return any data.")

  return buses_arrivals

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
  parser.add_argument("action", choices=['info_lines', 'info_line', 'info_stops', 'info_stop', 'line_stops', 
                                         'issues', 'buses_arrivals'],
                      help="what is going to be requested to the BusEMTMad service")
  parser.add_argument("credentials_file", help="path to the file with info to access the service")
  parser.add_argument("-lid", "--line_id",
                      help="bus line identifier for actions 'info_line', 'line_stops' and 'issues'; this argument is optional for action 'buses_arrivals'")
  parser.add_argument("-sid", "--stop_id",
                      help="stop identifier for action 'info_stop' and 'buses_arrivals'")
  parser.add_argument("-dir", "--direction", choices=['1','2'],
                      help="direction to be considered to analyze line info for action 'line_stops'; 1 for start to end, 2 for end to start")
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
  busemtmad_service = BusEMTMad(x_client_id, pass_key)
  busemtmad_service.log_in()

  # Action dispatching if credentials logged the client into the service
  #
  if (busemtmad_service.is_logged_in()):

    action_function = build_kafka_producer(args.broker, args.topic)
    if action_function==None:
      action_function = pprint_action;

    if args.action == "info_lines":
      logging.debug("x_client_id '%s' asking for lines information" % x_client_id)
      info_lines = info_lines_decorator(action_function)
      info_lines(busemtmad_service)
    elif args.action == "info_line":
      if args.line_id!=None:
        logging.debug("x_client_id '%s' asking for information for bus line '%s'" %
                      (x_client_id, args.line_id))
        info_line = info_line_decorator(action_function)
        info_line(busemtmad_service, args.line_id)
      else:
        logging.error("A bus line identifier has to be provided")
        sys.exit("A bus line identifier has to be provided for action 'info_line'")
    elif args.action == "info_stops":
      logging.debug("x_client_id '%s' asking for bus stops information" % x_client_id)
      info_stops = info_stops_decorator(action_function)
      info_stops(busemtmad_service)
    elif args.action == "info_stop":
      if args.stop_id!=None:
        logging.debug("x_client_id '%s' asking for information for bus stop '%s'" %
                      (x_client_id, args.stop_id))
        info_stop = info_stop_decorator(action_function)
        info_stop(busemtmad_service, args.stop_id)
      else:
        logging.error("A bus stop identifier has to be provided")
        sys.exit("A bus stop identifier has to be provided for action 'info_stop'")
    elif args.action == "line_stops":
      if args.line_id!=None and args.direction!=None:
        logging.debug("x_client_id '%s' asking for stops of the line '%s' with direction '%s'" %
                      (x_client_id, args.line_id, args.direction))
        line_stops = line_stops_decorator(action_function)
        line_stops(busemtmad_service, args.line_id, args.direction)
      else:
        logging.error("A bus line identifier and direction have to be provided")
        sys.exit("A bus line identifier and direction have to be provided for action 'line_stops'")
    elif args.action == "issues":
      if args.stop_id!=None:
        logging.debug("x_client_id '%s' asking for issues for bus stop '%s'" %
                      (x_client_id, args.stop_id))
        issues = issues_decorator(action_function)
        issues(busemtmad_service, args.stop_id)
      else:
        logging.error("A bus stop identifier has to be provided")
        sys.exit("A bus stop has to be provided for action 'issues'")
    elif args.action == "buses_arrivals":
      if args.stop_id!=None:
        logging.debug("x_client_id '%s' asking for buses arrivals for bus stop '%s' and bus line '%s'" %
                      (x_client_id, args.stop_id, args.line_id))
        buses_arrivals = buses_arrivals_decorator(action_function)
        buses_arrivals(busemtmad_service, args.stop_id, args.line_id)
      else:
        logging.error("A bus stop identifier has to be provided")
        sys.exit("A bus stop has to be provided for action 'busesarrivals' at least")
  else:
    logging.error("Unsuccessful login with x_client_id '%s%'" % x_client_id)
    sys.exit("Unsuccessful login with x_client_id '%s%'" % x_client_id)
