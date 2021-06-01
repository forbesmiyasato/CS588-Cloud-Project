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

print(len(tweets))
for tweet in tweets:
    if 'user' not in tweet:
        continue
    id = tweet['user']['id']
    username = tweet['user']['name']
    screen_name = tweet['user']['screen_name']
    r.hset('all_users', username, id)
    r.hmset(f'user:{id}', {'username': username, 'screen_name': screen_name})
