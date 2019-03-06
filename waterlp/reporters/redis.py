import redis

local_redis = redis.Redis(host='localhost', port=6379, db=0)