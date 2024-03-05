import os
import time
import pika
import pandas as pd
from loguru import logger
from cassandra.cluster import Cluster
import threading
import http.server
import socketserver
from matplotlib import pyplot as plt
import seaborn as sns

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

def aggregate_popular_hashtags(session):
    rows = session.execute("""
    SELECT tag_name, number_of_likes
    FROM hashtag_stats;
    """)
    df = pd.DataFrame(list(rows))
    result = df.groupby('tag_name').agg(
    total_likes=pd.NamedAgg(column='number_of_likes', aggfunc='sum'),
    ).sort_values(by='total_likes', ascending=False).head(10)

    # Create a figure and a set of subplots
    plt.figure(figsize=(10, 6))

    # Bar plot for total_likes
    barplot = sns.barplot(x='total_likes', y='tag_name', data=result.reset_index(), palette='viridis')

    # Add chart labels and title
    plt.xlabel('Total Likes')
    plt.ylabel('Hashtag')
    plt.title('Top 10 Most Popular Hashtags by Total Likes')

    for index, row in result.iterrows():
        barplot.text(row.total_likes, index, round(row.total_likes), color='black', ha="left")

    plt.savefig('/data/popular_hashtags.png')

def aggregate_popular_author(session):
    """
    Aggregates the total number of likes and shares for each author, filters the data for years 2013 to 2017,
    and plots a bar chart showing the most popular author by year based on total likes.

    Parameters:
    - session: The session object used to execute the SQL query.

    Returns:
    None
    """
    rows = session.execute("""
    SELECT author_name, date_time, number_of_likes, number_of_shares
    FROM author_stats
    """)
    df = pd.DataFrame(list(rows))

    # Ensure 'date_time' is a datetime type
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Extract the year from 'date_time'
    df['year'] = df['date_time'].dt.year

    # Filter the DataFrame for years 2013 to 2017
    filtered_df = df[(df['year'] >= 2013) & (df['year'] <= 2017)]

    #Group by 'year' and 'author_name', then sum 'number_of_likes' and 'number_of_shares'
    aggregated_df = filtered_df.groupby(['year', 'author_name']).agg(
        total_likes=pd.NamedAgg(column='number_of_likes', aggfunc='sum'),
        total_shares=pd.NamedAgg(column='number_of_shares', aggfunc='sum')
    ).reset_index()

    # For each year, find the author with the highest total likes
    most_popular_author_by_year = aggregated_df.loc[aggregated_df.groupby('year')['total_likes'].idxmax()]

    barplot = sns.barplot(
        x='year',
        y='total_likes',
        hue='author_name',
        data=most_popular_author_by_year,
        dodge=False,
        palette='viridis'
    )

    # Add title and labels
    plt.title('Most Popular Author by Year (2013-2017) Based on Total Likes', fontsize=16)
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Total Likes', fontsize=14)

    # Place the legend outside the plot
    plt.legend(title='Author Name', title_fontsize='13', fontsize='12', bbox_to_anchor=(1, 1), loc='upper left')

    # Show values on top of the bars
    for bar in barplot.patches:
        # The text annotation for each bar
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), 
                round(bar.get_height()),
                ha='center', va='bottom', color='black')

    # Rotate x-axis labels
    plt.xticks(rotation=45)

    plt.savefig('/data/popular_authors.png')

def aggregate_correlation_likes_shares(session):
    rows = session.execute("""
        SELECT tag_name, number_of_likes, number_of_shares
        FROM hashtag_stats
        WHERE tag_name = '#TheUncommonThread'
    """)
    df = pd.DataFrame(list(rows))

    # Our data contains multiple entries for the same tag, we will sum up the number of like and shares
    # Aggregate likes and shares by summing them up
    aggregated_data = df.groupby('tag_name').sum().reset_index()

    if not aggregated_data.empty:
        correlation = aggregated_data['number_of_likes'].iloc[0] / aggregated_data['number_of_shares'].iloc[0]
        txt = f"The correlation coefficient between number_of_likes and number_of_shares for #TheUncommonThread is: {correlation}"
    else:
        txt = "No data available to calculate correlation."

    # Show the plot
    with open('/data/correlation_likes_shares.txt', 'w') as f:
        f.write(txt)

def aggregate_correlation_likes_shares_globally(session):
    rows = session.execute("""
    SELECT SUM(number_of_likes) AS total_likes, SUM(number_of_shares) AS total_shares FROM hashtag_stats""")
    df = pd.DataFrame(list(rows))
    correlation = df['total_likes'][0] / df['total_shares'][0]

    txt = f"The correlation coefficient between number_of_likes and number_of_shares globally is: {correlation}"
    with open('/data/correlation_likes_shares_globally.txt', 'w') as f:
        f.write(txt)

def aggregate_emoji_histogram(session):
    rows = session.execute("""
    SELECT emoji
    FROM emoji_stats
    """)
    df = pd.DataFrame(list(rows))
    # Count the appearances of each emoji
    emoji_counts = df['emoji'].value_counts()

    # Convert the Series back into a DataFrame for easier handling
    emoji_counts_df = emoji_counts.reset_index()
    emoji_counts_df.columns = ['Emoji', 'Count']

    # Select the top 10 emojis
    top_emojis_df = emoji_counts_df.head(10)

    # please note that we decided to show the emojis in a table for visualization since emojis are not supported at matplot
    print("Top 10 emojis based on their appearance count:")
    txt = str(top_emojis_df)
    with open('/data/emoji_histogram.txt', 'w') as f:
        f.write(txt)

def aggregate_emoji_author(session):
    rows = session.execute("""
    SELECT emoji, author, SUM(number_of_likes) + SUM(number_of_shares) AS total_interactions
        FROM emoji_stats
        WHERE author = 'katyperry'
        GROUP BY emoji, author
        ALLOW FILTERING;
    """)
    df = pd.DataFrame(list(rows))

    # Sort the DataFrame based on the 'total_interactions' column in descending order
    df_sorted = df.sort_values(by='total_interactions', ascending=False)

    # Select the top 10 rows
    top_10_df = df_sorted.head(10)

    txt = "Top 10 emojis used by Katy Perry based on total_interactions are:\n"

    # Print the top 10 rows
    # please note that we decided to show the emojis in a table for visualization since emojis are not supported at matplot
    txt += str(top_10_df)

    with open('/data/emoji_author.txt', 'w') as f:
        f.write(txt)

def aggregate_daily_active_users(session):
    rows = session.execute("""
    SELECT author_name, date_time, COUNT(*) AS count
    FROM author_stats
    GROUP BY author_name, date_time
    """)

    df = pd.DataFrame(list(rows))

    # Convert 'date_time' to datetime object
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Extract date from datetime
    df['date'] = df['date_time'].dt.date

    # Count the unique users for each day
    daily_unique_users = df.groupby('date')['author_name'].nunique().reset_index()
    daily_unique_users.columns = ['date', 'unique_users']

    # Group together the days with the same number of unique users
    grouped_unique_users = daily_unique_users.groupby('unique_users')['date'].count().reset_index()
    grouped_unique_users.columns = ['unique_users', 'days_count']

    # Plot a pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(grouped_unique_users['days_count'], labels=grouped_unique_users['unique_users'], autopct='%1.1f%%', startangle=140)
    plt.title('Distribution of Days by Unique User Count')
    plt.axis('equal')

    plt.savefig('/data/daily_active_users.png')

def do_work():
    session = get_cassandra_session()

    aggregate_popular_hashtags(session)
    aggregate_popular_author(session)
    aggregate_correlation_likes_shares(session)
    aggregate_correlation_likes_shares_globally(session)
    aggregate_emoji_histogram(session)
    aggregate_emoji_author(session)
    aggregate_daily_active_users(session)


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

def start_http_server():
    PORT = 9090
    directory = "/data"
    Handler = http.server.SimpleHTTPRequestHandler
    if not os.path.exists(directory):
        os.makedirs(directory)
    os.chdir(directory)
    
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        logger.info(f"Serving /data directory at http://localhost:{PORT}")
        httpd.serve_forever()

if __name__ == '__main__':
    http_server_thread = threading.Thread(target=start_http_server, daemon=True)
    http_server_thread.start()
    start_consuming()
