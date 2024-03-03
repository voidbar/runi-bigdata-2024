import os
import time
import pika
from loguru import logger

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
rabbitmq_port = os.getenv('RABBITMQ_PORT', 5672)
rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
rabbitmq_credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
rabbitmq_connection_params = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=rabbitmq_credentials)

def get_rabbitmq_connection():
    return pika.BlockingConnection(rabbitmq_connection_params)

def do_work(body):
    logger.info(f"Received work: {body}")
    # Simulate doing some work by sleeping
    time.sleep(5)  # Sleep for 5 seconds to simulate work
    logger.info(f"Finished work: {body}")

def work_finished():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='work_finished_queue', durable=True)
    finish_message = 'Work finished'
    channel.basic_publish(exchange='',
                          routing_key='work_finished_queue',
                          body=finish_message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
    logger.info("Sent work completion message")
    connection.close()

def on_message(channel, method, properties, body):
    do_work(body)
    work_finished()
    channel.basic_ack(delivery_tag=method.delivery_tag)

def start_consuming():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='work_queue', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='work_queue', on_message_callback=on_message)

    logger.info('Worker is waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    start_consuming()