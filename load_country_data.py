import json
import redis
import time

parser = argparse.ArgumentParser()
parser.add_argument('--file', required=True, type=str)
args = parser.parse_args()
file = args.file

start_time = time.perf_conter()
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
    r.zadd("Country",{key: value}, incr=True)

total_time = time.perf_counter() - start_time
print(f"The program took {total_time:0.2f} seconds to run")
