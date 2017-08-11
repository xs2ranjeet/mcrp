import redis
import os

redisconfig = {
    #'host': 'xsim-redis.dvkmky.0001.apse1.cache.amazonaws.com',
    'host': 'localhost',    
    'port': 6379,
    'db': 0,
    'password':None,
    'socket_timeout':None
}
rq = redis.StrictRedis(**redisconfig)
