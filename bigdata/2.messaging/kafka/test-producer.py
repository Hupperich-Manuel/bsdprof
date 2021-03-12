from confluent_kafka import Producer


p = Producer({'bootstrap.servers': 'localhost'})

# Trigger any available delivery report callbacks from previous produce() calls
p.poll(0)
p.produce('cryptocompare', value='test message')

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
