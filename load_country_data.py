import json
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

with open('../Eurovision_files/Eurovision3.json') as f:
    tweets = json.loads("[" +
        f.read().replace("}\n\n{", "},\n{") +
    "]")


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
    r.zadd("country-test",{key: value})
