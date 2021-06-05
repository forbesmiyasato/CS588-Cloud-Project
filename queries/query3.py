import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

start_time = time.perf_counter()

all_users = r.hgetall('all_users')
max_count = float('-inf')
max_user = None
for username, id in all_users.items():
    username = username.decode("utf-8")
    id = id.decode("utf-8")
    count = r.zcount(f'user_tweets:{id}', '-inf', '+inf')
    if count > max_count:
        max_count = count
        max_user = username

total_time = time.perf_counter() - start_time
print(f"{max_user} posted the most tweets: {max_count} tweets")
print(f"Query executed in {total_time:0.2f} seconds")
