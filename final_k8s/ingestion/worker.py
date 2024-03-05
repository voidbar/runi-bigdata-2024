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

TWEETS_CSV = 'tweets.csv'

def get_rabbitmq_connection():
    return pika.BlockingConnection(rabbitmq_connection_params)

def get_cassandra_session():
    cluster = Cluster([cassandra_host])
    session = cluster.connect()
    session.set_keyspace('twitter_insights')

    return session

def extract_hashtags(tweet):
    # Define the regex pattern for hashtags
    hashtag_pattern = r'#\w+'

    # Find all hashtags in the tweet
    hashtags = re.findall(hashtag_pattern, tweet)

    return hashtags

def extract_emojis(tweet):
    # Define the regex pattern for emojis
    emoji_pattern = r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF]'

    # Find all emojis in the tweet
    emojis = re.findall(emoji_pattern, tweet, flags=re.UNICODE)

    return emojis

def ingest_tweets(tweets):
    # Setting the batch size so that we can control the number of rows inserted per batch
    batch_size = 1024*25 # 25KB

    session = get_cassandra_session()
    # Create individual insert statements
    insert_authors_popularity = session.prepare(
        f"INSERT INTO author_stats (date_time, author_name, number_of_likes, number_of_shares) "
        f"VALUES (?, ?, ?, ?)"
    )
    insert_hashtag_stats = session.prepare(
        f"INSERT INTO hashtag_stats (date_time, year, tag_name, number_of_likes, number_of_shares) "
        f"VALUES (?, ?, ?, ?, ?)"
    )
    insert_emoji_stats = session.prepare(
        f"INSERT INTO emoji_stats (date_time, emoji, author, number_of_likes, number_of_shares) "
        f"VALUES (?, ?, ?, ?, ?)"
    )

    # Ingestion of tweets
    batch = BatchStatement()
    for index, row in tweets.iterrows():
        # Extract data from the CSV row
        date_time = pd.to_datetime(row['date_time'])
        author_name = row['author']
        number_of_likes = int(row['number_of_likes'])
        number_of_shares = int(row['number_of_shares'])
        content = row['content']
        # Note: Current dataset includes empty geo fields, ignoring them for this calculation.
        # country = row['country']
        # latitude = float(row['latitude'])
        # longitude = float(row['longitude'])

        # Insert data into author_stats table
        batch.add(
            insert_authors_popularity,
            (date_time, author_name, number_of_likes, number_of_shares)
        )

        # Insert data into hashtag_stats table
        hashtags = extract_hashtags(content)
        for hashtag in hashtags:
            batch.add(
                insert_hashtag_stats,
                (date_time, date_time.year, hashtag, number_of_likes, number_of_shares)
            )

        # Insert data into emoji_stats table
        emojis = extract_emojis(content)
        for emoji in emojis:
            batch.add(
                insert_emoji_stats,
                (date_time, emoji, author_name, number_of_likes, number_of_shares)
            )

        if index % batch_size == 0:
            session.execute(batch)
            batch.clear()

    session.execute(batch)


def do_work(body):
    logger.info(f"Received message: {body}")

    tweets = pd.read_csv(TWEETS_CSV)
    start_idx, end_idx = body.split(b',')
    if end_idx == 'end' or int(end_idx) > len(tweets):
        tweets = tweets[int(start_idx):]
    else:
        tweets = tweets[int(start_idx):int(end_idx)]
    
    logger.info(f"Processing {len(tweets)} tweets")
    ingest_tweets(tweets)
    logger.info(f"Finished processing {len(tweets)} tweets")

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
    do_work(body)
    work_finished()
    channel.basic_ack(delivery_tag=method.delivery_tag)

def start_consuming():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='work_queue', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='work_queue', on_message_callback=on_message)

    logger.info('Worker is waiting for messages')
    channel.start_consuming()
    logger.info('Worker is finished?')
    input('Press Enter to exit')


if __name__ == '__main__':
    start_consuming()