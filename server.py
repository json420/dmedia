from usercouch.misc import TempPKI
import multiprocessing
import json
from microfiber import Server, dumps, build_ssl_context
import ssl
import select
import time
from dmedia.httpd import Server as HTTPServer, start_thread
import logging


format = [
    '%(levelname)s',
    '%(processName)s',
    '%(threadName)s',
    '%(message)s',
]
logging.basicConfig(
    level=logging.DEBUG,
    format='\t'.join(format),
)


def build_ssl_server_context(config):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    ctx.load_cert_chain(config['cert_file'],
        keyfile=config.get('key_file')
    )
    if 'ca_file' in config or 'ca_path' in config:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(
            cafile=config.get('ca_file'),
            capath=config.get('ca_path'),
        )
    return ctx


def get_value(value):
    if isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def application(environ, start_response):
    status = '200 OK'
    obj = dict(
        (key, get_value(value))
        for (key, value) in environ.items()
    )
    output = json.dumps(obj).encode('utf-8')

    response_headers = [
        ('Content-type', 'application/json'),
        ('Content-Length', str(len(output))),
    ]
    start_response(status, response_headers)

    return [output]



def start_process(target, *args, **kwargs):
    process = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
    process.daemon = True
    process.start()
    return process


def server(queue, config):
    ctx = build_ssl_server_context(config)
    httpd = HTTPServer(application, '::1', ctx, True)
    env = {'port': httpd.port, 'url': httpd.url}
    queue.put(env)
    httpd.serve_forever()


def start_httpd(config):
    q = multiprocessing.Queue()
    httpd = start_process(server, q, config)
    env = q.get()
    return (httpd, env)


def loop(s, count):
    for i in range(count):
        s.get()


pki = TempPKI(client_pki=True)
(httpd, env) = start_httpd(pki.get_server_config())
env['ssl'] = pki.get_client_config()
print(env)

s = Server(env)
print(dumps(s.get(), pretty=True))
#time.sleep(35)
#print(dumps(s.get(), pretty=True))

p_count = 10
count = 100
for p in range(1, p_count + 1):
    threads = []
    start = time.time()
    for i in range(p):
        thread = start_thread(loop, s, count)
        threads.append(thread)
    for thread in threads:
        thread.join()
    elapsed = time.time() - start
    print(p, ((count * p) / elapsed))
    #time.sleep(1)

#time.sleep(5)


