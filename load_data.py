import json
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

with open('Eurovision3.json') as f:
    tweets = json.loads("[" + 
        f.read().replace("}\n\n{", "},\n{") + 
    "]")

for tweet in tweets:
    if 'user' not in tweet:
        continue
    id = tweet['user']['id']
    username = tweet['user']['name']
    screen_name = tweet['user']['screen_name']
    r.hset('all_users', username, id)
    r.hmset(f'user:{id}', {'username': username, 'screen_name': screen_name})
