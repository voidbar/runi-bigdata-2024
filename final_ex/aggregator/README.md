# Aggregator

The aggregator queries the database and extract insights from the data. It is the final step of the pipeline.
Below are the main insights that the aggregator extracts from the data.

Applying the keyspace:⁠ `use twitter_insights`

## Query 1: Most Popular Hashtags by Year

To find the most popular hashtags by year, we can extract the data from the DB and then use pandas to aggregate the number of likes for each hashtag, grouped by year, and sort them in descending order by likes.

```sql
SELECT tag_name, number_of_likes, number_of_shares
FROM hashtag_stats
```

## Query 2: Most Popular Author by Year

Similar to hashtags, we we can extract the data from the DB and then use pandas to aggregate the number of likes and shares for each author, filtering by year, to find the most popular author by likes, show how many shares he got.

```SQL
SELECT author_name, date_time, number_of_likes, number_of_shares
FROM author_stats
```

## Query 3: Correlation between Hashtag and Likes/Shares

This query extracts the number of likes and shares for a specific hashtag, we will then calculate the correlation between them in .

```SQL
SELECT tag_name, number_of_likes, number_of_shares
FROM hashtag_stats
WHERE tag_name = 'your_hashtag'
```

## Query 4: Correlation between Likes and Shares Globally

To understand the relationship between likes and shares across all hashtags, we calculate the correlation of all the likes and shares globally.

```SQL
SELECT number_of_likes, number_of_shares
FROM hashtag_stats;
```

## Query 5: Emoji Histogram

To understand emoji usage, this query get all the emojis, we then will use pandas to sort and sum them in order to create a histogram of most used emojis.

```SQL
SELECT emoji
FROM emoji_stats
```

## Query 6: Most Used Emojis For a Given Author

For a specific author, this query finds the number of usages of emojis by aggregating likes and shares, providing insights into the author's emoji preferences, we then use pandas to get the top ten used Emojis.

```SQL
SELECT emoji, author, SUM(number_of_likes) + SUM(number_of_shares) AS total_interactions
FROM emoji_stats
WHERE author = 'your_author'
GROUP BY emoji, author
ALLOW FILTERING;
```

## Query 7: Daily Active Users

To find the number of unique active users per day, which can help understand daily user engagement and activity levels.
The query will fetch all relevant data, we will use pandas to show the distribution of days based on the number of unique users observed in them.

```SQL
SELECT author_name, date_time, COUNT(*) AS count
FROM author_stats
GROUP BY author_name, date_time
```