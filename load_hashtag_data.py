import json
import redis
import argparse
import time
from datetime import datetime
from utils import get_logger

parser = argparse.ArgumentParser()
parser.add_argument('--file', required=True, type=str)
args = parser.parse_args()
file = args.file

LOGGER = get_logger('load hashtag data')

r = redis.Redis(host='localhost', port=6379, db=0)

LOGGER.info('Starting to load hashtag data')

start_time = time.perf_counter()

with open(file) as f:
    tweets = json.loads(f.read())

for tweet in tweets:
    if ('id' not in tweet or 'entities' not in tweet 
        or 'hashtags' not in tweet['entities']):
        continue
    hashtags = []
    for hashtag in tweet['entities']['hashtags']:
        hashtags.append(hashtag['text'])
    if hashtags:
        r.sadd('all_hashtags', *hashtags)
    created_time = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
    for hashtag in hashtags:
        r.zadd(f'hashtag_tweets:{hashtag}', {tweet['id']: created_time.timestamp()},
               nx=True)

total_time = time.perf_counter() - start_time
LOGGER.info(f"The program took {total_time:0.2f} seconds to run")
