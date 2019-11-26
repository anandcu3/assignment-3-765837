import pika
import sys
import json
import time

topic = sys.argv[1]
credentials = pika.PlainCredentials('user', 'QwwyqaQj1C4i')
parameters = pika.ConnectionParameters('35.247.117.124',5672,'/',credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=topic)
def callback(ch, method, properties, body):
    print("Received"+body.decode(), file=open("logs/rabbit_server.log","a"))
    data = json.loads(body.decode())
    parameters = pika.ConnectionParameters('35.247.117.124',5672,'/',credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_publish(exchange='', routing_key="streaming_data", body=json.dumps(data))
    start = time.time()
    end = time.time()

channel.basic_consume(queue=topic, on_message_callback=callback, auto_ack=True)
print(' [*] Waiting for messages on topic', topic, 'To exit press CTRL+C')
print(' [*] Waiting for messages on topic', topic, 'To exit press CTRL+C', file=open("logs/rabbit_server.log","a"))
channel.start_consuming()
