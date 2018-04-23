import redis

config = {
    'host' : 'localhost',
    'port' : 6379,
    'db' : 0
}

redis_object = redis.StrictRedis(**config)