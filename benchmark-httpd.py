#!/usr/bin/python3

import multiprocessing
import optparse
import time
import os

from microfiber import CouchBase, dumps

from dmedia.peering import TempPKI
from dmedia.httpd import run_server, echo_app


request_body = os.urandom(1776)


def start_process(target, *args, **kwargs):
    process = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
    process.daemon = True
    process.start()
    return process


def client_process(env, count):
    client = CouchBase(env)
    for i in range(count):
        client.put(request_body)


parser = optparse.OptionParser()
parser.add_option('--client-pki',
    help='use client-side PKI also',
    action='store_true',
    default=False,
)
(options, args) = parser.parse_args()


pki = TempPKI(options.client_pki)
q = multiprocessing.Queue()
start_process(run_server, q, echo_app,
    ssl_config=pki.get_server_config(),
)
env = q.get()
env['ssl'] = pki.get_client_config()

p_count = 22
count = 500
print('')
print('Benchmarking with {} requests per connection, client_pki={!r}'.format(
        count, options.client_pki)
)
for p in range(1, p_count + 1):
    workers = []
    time.sleep(0.5)  # Let things "settle" a moment
    start = time.time()
    for i in range(p):
        w = start_process(client_process, env, count)
        workers.append(w)
    for w in workers:
        w.join()
    elapsed = time.time() - start
    print('    Concurrency: {}; Requests/Second: {:d}'.format(
            p, int((count * p) / elapsed)
        )
    )
