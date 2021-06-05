import json
import redis
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--file', required=True, type=str)
args = parser.parse_args()
file = args.file

r = redis.Redis(host='localhost', port=6379, db=0)

with open(file) as f:
    tweets = json.loads(f.read())

for tweet in tweets:
    if 'id' not in tweet:
        continue
    tweet_id = tweet['id']
    created_time = tweet['created_at']
    text = tweet['text']
    user_key = tweet['user']['id']
    tweet_type = 'Simple'
    if tweet['in_reply_to_status_id']:
        tweet_type = 'reply'
    elif 'retweeted_status' in tweet:
        if tweet['retweeted_status']['is_quote_status']:
            tweet_type = 'retweet with quote'
        else:
            tweet_type = 'retweet'
    reply_to_user_key = 'None'
    if tweet['in_reply_to_user_id']:
        reply_to_user_key = tweet['in_reply_to_user_id']
    reply_to_tweet_id = 'None'
    if tweet['in_reply_to_status_id']:
        reply_to_tweet_id = tweet['in_reply_to_status_id']
    created_dt = datetime.strptime(created_time,'%a %b %d %H:%M:%S +0000 %Y') 
    r.hset('all_tweets', tweet_id, tweet_id)
    r.hmset(f'tweet:{tweet_id}', {'tweet_id': tweet_id, 'type': tweet_type,
                            'created_time': created_time, 'text': text,
                            'user_key': user_key, 'reply_to_user_key': reply_to_user_key})
    r.zadd(f'reply_tweets:{reply_to_tweet_id}', {tweet_id: created_dt.timestamp()}, nx=True)

