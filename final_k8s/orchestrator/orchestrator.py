import time
import os
import sys 
import pika
from loguru import logger


rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
rabbitmq_port = os.getenv('RABBITMQ_PORT', 5672)
rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
rabbitmq_credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
rabbitmq_connection_params = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=rabbitmq_credentials)

def is_rabbitmq_ready():
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue='bigdata', durable=True)
        connection.close()
        
        return True
    except Exception as e:
        print(f"RabbitMQ connection test failed: {e}")
        return False

def get_rabbitmq_connection():
    return pika.BlockingConnection(rabbitmq_connection_params)

def initialize_cassandra_db():
    pass

def request_work():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='work_queue', durable=True)
    for i in range(10):
        start_index = i * 1000
        end_index = start_index + 1000
        msg = f'{start_index},{end_index}'
        channel.basic_publish(exchange='',
                              routing_key='work_queue',
                              body=msg,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        logger.info(f"Sent {msg}")
    connection.close()

def wait_for_work_finished():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='work_finished_queue', durable=True)
    
    count = 0
    def callback(ch, method, properties, body):
        nonlocal count
        logger.info(f"Received work completion message: {body}")
        count += 1
        if count >= 10:  # Assuming there are 10 workers
            ch.stop_consuming()

    channel.basic_consume(queue='work_finished_queue', on_message_callback=callback, auto_ack=True)
    logger.info('Waiting for work to finish...')
    channel.start_consuming()

def request_aggregation():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='aggregation_queue', durable=True)
    aggregation_message = 'Start aggregation'
    channel.basic_publish(exchange='',
                          routing_key='aggregation_queue',
                          body=aggregation_message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
    logger.info("Sent aggregation request")
    connection.close()

def main():
    if not is_rabbitmq_ready():
        sys.exit(1)
    
    initialize_cassandra_db()
    request_work()
    wait_for_work_finished()
    request_aggregation()

if __name__ == '__main__':
    main()