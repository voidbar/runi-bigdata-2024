import os
import time
import pika
import pandas as pd
from loguru import logger
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
import re


cassandra_host = os.getenv('DATABASE_HOST', 'localhost')

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
rabbitmq_port = os.getenv('RABBITMQ_PORT', 5672)
rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
rabbitmq_credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
rabbitmq_connection_params = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=rabbitmq_credentials)


def get_rabbitmq_connection():
    return pika.BlockingConnection(rabbitmq_connection_params)

def get_cassandra_session():
    cluster = Cluster([cassandra_host])
    session = cluster.connect()
    session.set_keyspace('twitter_insights')

    return session

def do_work():
    session = get_cassandra_session()
    rows = session.execute("""
    SELECT tag_name, SUM(number_of_likes) AS total_likes, SUM(number_of_shares) AS total_shares
    FROM hashtag_stats
    GROUP BY tag_name
    LIMIT 10
    ALLOW FILTERING;
    """)

    output = str(pd.DataFrame(list(rows)))
    with open('/data/output.txt', 'w') as f:
        f.write(output)

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
    logger.info(f"Received message: {body}")
    do_work()
    work_finished()
    channel.basic_ack(delivery_tag=method.delivery_tag)

def start_consuming():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='aggregation_queue', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='aggregation_queue', on_message_callback=on_message)

    logger.info('Aggregation is waiting for messages')
    channel.start_consuming()


if __name__ == '__main__':
    start_consuming()