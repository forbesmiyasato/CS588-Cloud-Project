import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

start_time = time.perf_counter()

hashtags = r.smembers('all_hashtags')
hashtag_and_count = []

for hashtag  in hashtags:
    hashtag = hashtag.decode("utf-8")
    tweet_count = r.zcount(f'hashtag_tweets:{hashtag}', '-inf', '+inf')
    hashtag_and_count.append((hashtag, tweet_count))

hashtag_and_count.sort(key = lambda x: x[1], reverse=True)
top_100_hashtag_and_count = hashtag_and_count[:100]

total_time = time.perf_counter() - start_time
print(top_100_hashtag_and_count)
print(f"Query executed in {total_time:0.2f} seconds")
