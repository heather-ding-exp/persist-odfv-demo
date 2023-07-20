# Prints all entries stored in Redis online store
import redis

# Connection to Redis instance
r = redis.Redis(host='localhost', port=6379)
keys = r.keys()
for k in keys:
    print(k)

