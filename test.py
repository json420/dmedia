from usercouch.misc import TempCouch
from microfiber import Server, dumps, _start_thread, random_id
from dmedia.peering import TempPKI
from dmedia.server import RootApp
from dmedia.httpd import make_server
import time
from copy import deepcopy
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

pki = TempPKI(True)
config = {
    'username': 'admin',
    'replicator': pki.get_client_config(),
}

couch1 = TempCouch()
env1 = couch1.bootstrap('basic', deepcopy(config))
s1 = Server(env1)
s1.put(None, 'one')

couch2 = TempCouch()
env2 = couch2.bootstrap('basic', deepcopy(config))
s2 = Server(env2)
s2.put(None, 'two')



def pusher(env):
    time.sleep(2)
    s1.push('one', 'two', env, continuous=True)
    timeout = 1
    while True:
        docs = [{'_id': random_id(), 'i': i} for i in range(100)]
        for doc in docs:
            doc['_rev'] = s1.post(doc, 'one')['rev']
        time.sleep(timeout)
        timeout *= 1.25
        for doc in docs:
            assert s2.get('two', doc['_id']) == doc



app = RootApp(env2)

httpd = make_server(app, ssl_config=pki.get_server_config())
env = {'url': httpd.url + 'couch/'}
_start_thread(pusher, env)
httpd.serve_forever()

#time.sleep(10)


