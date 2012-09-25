from wsgiref.simple_server import WSGIServer, WSGIRequestHandler
import socket
from usercouch.misc import TempPKI
import multiprocessing
import json
from microfiber import Server, dumps, build_ssl_context
import ssl
import select


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


class WSGIServer6(WSGIServer):
    address_family = socket.AF_INET6
    ssl_context = None

    def get_request(self):
        (conn, address) = self.socket.accept()
        if self.ssl_context is not None:
            conn = self.ssl_context.wrap_socket(conn, server_side=True)
            while True:
                try:
                    conn.do_handshake()
                    break
                except ssl.SSLError as err:
                    if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                        select.select([conn], [], [])
                    elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                        select.select([], [conn], [])
                    else:
                        raise
        return (conn, address)

#    def serve_forever(self, poll_interval=0.5):
#        while True:
#            try:
#                (conn, address) = self.get_request()
#            except socket.error:
#                continue
#            if self.verify_request(conn, address):
#                try:
#                    self.process_request(conn, address)
#                except:
#                    self.handle_error(conn, address)
#                    self.shutdown_request(conn)


def start_process(target, *args, **kwargs):
    process = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
    process.daemon = True
    process.start()
    return process


def server(queue, config):
    httpd = WSGIServer6(('::1', 0), WSGIRequestHandler)
    ctx = build_ssl_context(config)
    httpd.ssl_context = ctx
    #httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    httpd.set_app(application)
    port = httpd.socket.getsockname()[1]
    queue.put(port)
    httpd.serve_forever()


def start_httpd(config):
    q = multiprocessing.Queue()
    httpd = start_process(server, q, config)
    port = q.get()
    return (httpd, port)


pki = TempPKI(client_pki=True)
(httpd, port) = start_httpd(pki.get_server_config())
env = {
    'url': 'https://[::1]:{}/'.format(port),
    'ssl': pki.get_client_config(),
}

s = Server(env)
print(dumps(s.get(), pretty=True))


