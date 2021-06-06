import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

start_time = time.perf_counter()
countries = r.zrevrange("Country", 0, 0, withscores=True)
print(countries)
total_time = time.perf_counter() - start_time
print(f"Query executed in {total_time:0.2f} seconds")

