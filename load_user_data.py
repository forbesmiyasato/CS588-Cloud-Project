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

LOGGER = get_logger('load user data')

r = redis.Redis(host='localhost', port=6379, db=0)

LOGGER.info('Starting to load user data')

start_time = time.perf_counter()

with open(file) as f:
    tweets = json.loads(f.read())

print(len(tweets))
for tweet in tweets:
    if 'user' not in tweet:
        continue
    id = tweet['user']['id']
    username = tweet['user']['name']
    screen_name = tweet['user']['screen_name']
    r.hset('all_users', username, id)
    r.hmset(f'user:{id}', {'username': username, 'screen_name': screen_name})
    created_time = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
    r.zadd(f'user_tweets:{id}', {tweet['id']: created_time.timestamp()}, nx=True)

total_time = time.perf_counter() - start_time
LOGGER.info(f"The program took {total_time:0.2f} seconds to run")
