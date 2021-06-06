import json
import redis
import argparse
import time
from utils import get_logger

parser = argparse.ArgumentParser()
parser.add_argument('--file', required=True, type=str)
args = parser.parse_args()
file = args.file

LOGGER = get_logger('load country data')

r = redis.Redis(host='localhost', port=6379, db=0)

LOGGER.info('Starting to load country data')

start_time = time.perf_counter()

with open(file) as f:
    tweets = json.loads(f.read())

country_dictionary = {}
for tweet in tweets:
    if 'place' not in tweet:
        continue

    if tweet['place'] == None:
        continue

    if 'country' not in tweet['place']:
        continue

    if tweet['place']['country'] == None:
        continue

    country = tweet['place']['country']

    if country in country_dictionary:
        country_dictionary[country] += 1
    else:
        country_dictionary[country] = 1

for key, value in country_dictionary.items():
    r.zadd("Country",{key: value}, incr=True)

total_time = time.perf_counter() - start_time
LOGGER.info(f"The program took {total_time:0.2f} seconds to run")
