import time
import os
import sys 
import pika
from loguru import logger
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement


cassandra_host = os.getenv('DATABASE_HOST', 'localhost')

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
rabbitmq_port = os.getenv('RABBITMQ_PORT', 5672)
rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
rabbitmq_credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
rabbitmq_connection_params = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=rabbitmq_credentials)

WORK_CHUNKS = 6

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
    cluster = Cluster([cassandra_host])
    session = cluster.connect()

    session.execute("DROP KEYSPACE IF EXISTS twitter_insights")

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS twitter_insights WITH replication = {
            'class': 'SimpleStrategy', 'replication_factor': '3'
        }
    """)

    session.set_keyspace('twitter_insights')

    session.execute("""
        CREATE TABLE IF NOT EXISTS hashtag_stats (
            date_time timestamp,
            year int,
            tag_name text,
            number_of_likes int,
            number_of_shares int,
            country text,
            latitude float,
            longitude float,
            PRIMARY KEY (tag_name, year, date_time)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS author_stats (
            date_time timestamp,
            author_name text,
            number_of_likes int,
            number_of_shares int,
            country text,
            latitude float,
            longitude float,
            PRIMARY KEY (author_name, date_time)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS emoji_stats (
            date_time timestamp,
            emoji text,
            author text,
            number_of_likes int,
            number_of_shares int,
            country text,
            latitude float,
            longitude float,
            PRIMARY KEY ((emoji, author), date_time)
        );
    """)
    


def request_work():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='work_queue', durable=True)
    for i in range(WORK_CHUNKS):
        start_index = i * 10000
        end_index = start_index + 10000
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
        if count >= WORK_CHUNKS:
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
    logger.info("Starting orchestrator")

    logger.info("Checking if RabbitMQ is ready")
    if not is_rabbitmq_ready():
        logger.error("RabbitMQ is not ready. Exiting.")
        sys.exit(1)
    logger.info("Initializing Cassandra database")
    initialize_cassandra_db()
    logger.info("Requesting work from ingestion workers...")
    request_work()
    logger.info("Waiting for work to finish...")
    wait_for_work_finished()
    logger.info("Requesting aggregation...")
    request_aggregation()

    logger.info("Orchestrator finished")

if __name__ == '__main__':
    main()