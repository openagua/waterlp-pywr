import os
import redis

local_redis = redis.Redis(host=os.environ.get('REDIS_HOST', 'localhost'), port=6379, db=0)
