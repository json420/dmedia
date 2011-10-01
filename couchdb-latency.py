#!/usr/bin/python3

# Test to measure latency between posting a doc to CouchDB and getting it back
# through the changes feed.

from threading import Thread
from queue import Queue
import time
from http.client import ResponseNotReady

from dmedia.tests.couch import TempCouch
from microfiber import Database, random_id


def changes_thread(name, env, queue):
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

print(q.get())  # Get the initial state
def run_test(count):
    for i in range(count):
        time.sleep(1)
        _id = random_id()
        doc = {'_id': _id, 'index': i}
        start = time.time()
        db.post(doc)
        r = q.get()
        end = time.time()
        print(r)
        assert r['results'][0]['id'] == _id
        yield end - start


count = 20
times = tuple(run_test(count))
print('\nRound trip times:')
for t in times:
    print('  {:.3f}'.format(t))
total = sum(times)
print('Max latency: {:.3f}'.format(max(times)))
print('Min latency: {:.3f}'.format(min(times)))
print('Average latency: {:.3f}'.format(total / count))
