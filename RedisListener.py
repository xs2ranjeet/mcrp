import redis
import settings
import threading
import logging
import json

class Listener(threading.Thread):
    def __init__(self, r, channels, backend, workers):
        threading.Thread.__init__(self)
        self.redis = r
        self.backend = backend
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(channels)
        self.workers = workers
    def work1(self, item):
        print(item['channel'], ":", item['data'])
        for worker in self.workers.queue:
            msg = [worker, item['data'],b'optional data']
            logging.info('Send {0} {1}'.format(item['data'], worker))
            self.backend.send_multipart(msg)      
    def work(self, item):
        print(item['channel'], ":", item['data'])
        if type(item['data']) == int:
            return
        data = json.loads(item['data'].decode('utf-8'))
        if data.get('cid'):
            cids = data['cid']
        else:
            cids = []
        if data.get('gid'):
            gid = data['gid']
        else:
            gid = []
        if data.get('cid'):
            data.pop('cid')
        if data.get('gid'):
            data.pop('gid')
        for cid in cids:
            msg = [bytes(cid, 'UTF-8'), b'7', bytes(json.dumps(data), 'UTF-8')]
            logging.info('Send To: {0} {1}'.format(cid, data))
            self.backend.send_multipart(msg)  
        if len(gid) > 0:
            cidlist = self.redis.get(gid)
            if len(cidlist) > 0:
                for cid in cidlist:
                    msg = [bytes(cid, 'UTF-8'), b'7',bytes(json.dumps(data), 'UTF-8')]
                    logging.info('Send To: {0} {1}'.format(cid, data))
                    self.backend.send_multipart(msg)                 
            
    def run(self):
        for item in self.pubsub.listen():
            if item['data'] == "KILL":
                self.pubsub.unsubscribe()
                print (self, "unsubscribed and finished")
                break
            else:
                self.work(item)