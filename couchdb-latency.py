#!/usr/bin/python3

# Test to measure latency between posting a doc to CouchDB and getting it back
# through the changes feed.

from threading import Thread
from queue import Queue
import time
from http.client import ResponseNotReady

from usercouch.misc import TempCouch
from microfiber import Database, random_id


def changes_thread(name, env, queue):
    try:
        db = Database(name, env)
        
        # First, get the starting state of the DB:
        result = db.get('_changes')
        last_seq = result['last_seq']
        queue.put(result)

        # And now monitor it for changes
        while True:
            try:
                result = db.get('_changes', feed='longpoll', since=last_seq)
                last_seq = result['last_seq']
                queue.put(result)
            except ResponseNotReady:
                pass
    except Exception as e:
        queue.put(e)


class SmartQueue(Queue):
    """
    Queue with custom get() that raises exception instances from the queue.
    """

    def get(self, block=True, timeout=None):
        item = super().get(block, timeout)
        if isinstance(item, Exception):
            raise item
        return item


def _start_thread(target, *args):
    thread = Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread


tmpcouch = TempCouch()
env = tmpcouch.bootstrap()
db = Database('novacut', env)
db.put(None)

# Start the thread that listens for changes:
q = SmartQueue()
thread = _start_thread(changes_thread, 'novacut', env, q)

print('\nRound trip times:')
docs = []
q.get()  # Get the initial state
def run_test(count):
    for i in range(count):
        time.sleep(2)  # 1s is threshold for batching when delayed_commits=True
        _id = random_id()
        doc = {'_id': _id, 'index': i}
        start = time.time()
        db.post(doc)
        r = q.get()
        diff = time.time() - start
        assert r['results'][0]['id'] == _id
        print('  {:.4f}'.format(diff))
        yield diff


count = 10
times = tuple(run_test(count))
total = sum(times)
print('Max latency: {:.4f}'.format(max(times)))
print('Min latency: {:.4f}'.format(min(times)))
print('Average latency: {:.4f}'.format(total / count))
