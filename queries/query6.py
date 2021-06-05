import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

start_time = time.perf_counter()

users = r.hgetall('all_users')
for username, id in users.items():
    username = username.decode("utf-8")
    id = id.decode("utf-8")
    all_tweet_keys = r.zrangebyscore(f'user_tweets:{id}', '-inf', '+inf')
    all_count = len(all_tweet_keys)
    simple_count = 0
    reply_count = 0
    retweet_count = 0
    quoted_count = 0
    for key in all_tweet_keys:
        key = key.decode("utf-8")
        type = r.hget(f'tweet:{key}', 'type').decode("utf-8")
        if type == 'simple':
            simple_count += 1
        elif type == 'reply':
            reply_count += 1
        elif type == 'retweet':
            retweet_count += 1
        elif type == 'quoted':
            quoted_count += 1
    print(f'For {username}')
    print(f'Simple Tweets: {(simple_count / all_count)*100}%')
    print(f'Reply Tweets: {(reply_count / all_count)*100}%')
    print(f'Retweets: {(retweet_count / all_count)*100}%')
    print(f'Quoted Tweets: {(quoted_count / all_count)*100}%')

total_time = time.perf_counter() - start_time
print(f"Query executed in {total_time:0.2f} seconds")
