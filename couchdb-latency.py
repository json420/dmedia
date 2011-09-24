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
    start = db.get('_changes', include_docs=True)
    last_seq = start['last_seq']
    queue.put(start)

    # And now monitor it for changes
    while True:
        try:
            change = db.get('_changes', include_docs=True,
                feed='longpoll',
                since=last_seq,
            )
            last_seq = change['last_seq']
            queue.put(change)
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


def run_test(count):
    for i in range(count):
        start = time.time()
        doc = {'_id': random_id()}
        db.post(doc)
        q.get()
        yield time.time() - start


count = 200
times = tuple(run_test(count))
total = sum(times)
print('Total time: {}'.format(total))
print('Average time: {}'.format(total / count))


