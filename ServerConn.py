import zmq
import logging
import redis
import time
from settings import rq
from RedisListener import Listener
from collections import OrderedDict

HEARTBEAT_INTERVAL = 1.0   # Seconds
HEARTBEAT_LIVENESS = 3     # 3..5 is reasonable
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
def ProcessClientRequest(frames, workers, backend):
    if not frames:
        return
    logging.info(frames)
    address = frames[0]
    workers.ready(Worker(address))
    print(frames[1])
    #print(address)
    if frames[1]==b'1':
        msg = [address,b'2']
        backend.send_multipart(msg)
    elif frames[1]==b'3':
        msg = [address,b'4']
        backend.send_multipart(msg)
    elif frames[1]==b'54':
        r.hset('REG_DATA', address, frames[2].decode())
    
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
    
    while True:
        poller = poll_workers
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
    
        # Handle worker activity on backend
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            frames = backend.recv_multipart()
            ProcessClientRequest(frames, workers, backend)
