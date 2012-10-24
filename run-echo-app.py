#!/usr/bin/python3

import multiprocessing
import optparse
import os
from hashlib import md5

from microfiber import CouchBase, dumps

from dmedia.identity import TempPKI
from dmedia.httpd import run_server, echo_app


def start_process(target, *args, **kwargs):
    process = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
    process.daemon = True
    process.start()
    return process


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
client = CouchBase(env)
body = os.urandom(1776)
digest = md5(body).hexdigest()
result = client.post(body)
assert result['echo.content_md5'] == digest
assert result['CONTENT_LENGTH'] == '1776'
print(dumps(result, pretty=True))

