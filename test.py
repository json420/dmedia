from usercouch.misc import TempCouch
from microfiber import Server, dumps, _start_thread, random_id
from dmedia.httpd import HTTPD, echo_app
import time

couch1 = TempCouch()
env1 = couch1.bootstrap('open')
s1 = Server(env1)
s1.put(None, 'one')

couch2 = TempCouch()
env2 = couch2.bootstrap('open')
s2 = Server(env2)
s2.put(None, 'two')



def pusher(env):
    time.sleep(1)
    s1.push('one', 'two', env, continuous=True)
    while True:
        doc = {'_id': random_id()}
        s1.post(doc, 'one')
        time.sleep(3)


def get_headers(environ):
    for (key, value) in environ.items():
        if key in ('CONTENT_LENGHT', 'CONTENT_TYPE'):
            yield (key.replace('_', '-').lower(), value)
        elif key.startswith('HTTP_'):
            yield (key[5:].replace('_', '-').lower(), value)


def app(environ, start_response):
    print('\nREQUEST:')
    print('  {REQUEST_METHOD} {PATH_INFO}'.format(**environ))
    headers = tuple(get_headers(environ))
    for (name, value) in headers:
        print('  {}: {}'.format(name, value))
    headers = dict(headers)
    headers['host'] = s2.ctx.t.netloc

    if environ['wsgi.input']._avail:
        body = environ['wsgi.input'].read()
    else:
        body = None

    response = s2.raw_request(environ['REQUEST_METHOD'], environ['PATH_INFO'], body, headers)

    print('RESPONSE:')
    status = '{} {}'.format(response.status, response.reason)
    headers = response.getheaders()
    print('  {}'.format(status))
    for (name, value) in headers:
        print('  {}: {}'.format(name, value))
    start_response(status, headers)
    body = response.read()
    if body:
        return [body]
    return []


httpd = HTTPD(app)
env = {'url': httpd.url}
_start_thread(pusher, env)
httpd.serve_forever()

#time.sleep(10)


