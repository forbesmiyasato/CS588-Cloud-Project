import json
import redis
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--file', required=True, type=str)
args = parser.parse_args()
file = args.file

r = redis.Redis(host='localhost', port=6379, db=0)

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
    r.zadd("country-test",{key: value}, incr=True)
