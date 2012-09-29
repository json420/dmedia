#!/usr/bin/python3

from usercouch.misc import TempPKI
import multiprocessing
import json
from microfiber import Server, dumps, build_ssl_context
import time
from dmedia.httpd import HTTPD, build_server_ssl_context, echo_app


def start_process(target, *args, **kwargs):
    process = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
    process.daemon = True
    process.start()
    return process


def server(queue, config):
    ctx = build_server_ssl_context(config)
    httpd = HTTPD(echo_app, '::1', ctx, True)
    env = {'port': httpd.port, 'url': httpd.url}
    queue.put(env)
    httpd.serve_forever()


def start_httpd(config):
    q = multiprocessing.Queue()
    httpd = start_process(server, q, config)
    env = q.get()
    return (httpd, env)


def loop(env, count):
    s = Server(env)
    for i in range(count):
        s.get()


pki = TempPKI(client_pki=True)
(httpd, env) = start_httpd(pki.get_server_config())
env['ssl'] = pki.get_client_config()
#print(dumps(env))

s = Server(env)
s.get()
#print(dumps(s.get(), pretty=True))

print('\nBenchmarking...')
p_count = 20
count = 200
for p in range(1, p_count + 1):
    time.sleep(1)
    workers = []
    start = time.time()
    for i in range(p):
        w = start_process(loop, env, count)
        workers.append(w)
    for w in workers:
        w.join()
    elapsed = time.time() - start
    print('    Concurrency: {}; Requests Per Second: {:d}'.format(
            p, int((count * p) / elapsed)
        )
    )
