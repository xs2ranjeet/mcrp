import zmq
import logging
import redis
import time
import util
from settings import rq
from RedisListener import Listener
from collections import OrderedDict

HEARTBEAT_INTERVAL = 1.0   # Seconds
HEARTBEAT_LIVENESS = 3     # 3..5 is reasonable
PING_INTERVAL = 30.0   # Seconds
class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker
    def next(self):
        address, worker = self.queue.popitem(False)
        print(address)
        print(worker)
        return address
def send_message(address, event, data, backend):
    if data == None:
        msg = [address, event]
    else:
        msg = [address, event, data]
    backend.send_multipart(msg)
def isValidaAddress(address):
    if len(address) < 5 or len(address) > 10:
        return False
    #if address.find("+") or address.find("000"):
        #return False
    rq.sadd('user'+util.getCurrentTimeStampKey(), address)
    return True
    
def ProcessClientRequest(frames, workers, backend):
    if not frames:
        return
    logging.info(frames)
    address = frames[0]
    workers.ready(Worker(address))
    #print(frames[1])
    #print(address)
    if isValidaAddress(address) ==  False:
        send_message(address, b'6', b'1', backend)
        return
    rq.set('last_client_ping',util.getCurrentTimeStamp())
    if frames[1]==b'1':
        send_message(address, b'2', None, backend)
    elif frames[1]==b'3':
        send_message(address, b'4', None, backend)
    elif frames[1]==b'54':
        rq.hset('REG_DATA', address, frames[2].decode())
        send_message(address, b'154', None, backend)
    
def StartServer():
    logging.info("Server Start")
    context = zmq.Context(1)
    backend = context.socket(zmq.ROUTER)  # ROUTER
    backend.bind("tcp://*:5000")  # For workers
    workers = WorkerQueue()
    #r = redis.Redis()
    client = Listener(rq, ['cmd_robot'],backend, workers)
    client.start()    
    
    poll_workers = zmq.Poller()
    poll_workers.register(backend, zmq.POLLIN)
    ping_at = time.time() + PING_INTERVAL
    while True:
        poller = poll_workers
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
    
        # Handle worker activity on backend
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            frames = backend.recv_multipart()
            ProcessClientRequest(frames, workers, backend)
        if time.time() >= ping_at:
            ping_at = time.time() + PING_INTERVAL
            rq.set('last_zmq_ping',util.getCurrentTimeStamp())