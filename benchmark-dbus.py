#!/usr/bin/python3

import os
from os import path
import time
from base64 import b32encode
from subprocess import Popen
import timeit


def random_bus():
    random = b32encode(os.urandom(10)).decode('utf-8').lower()
    return 'tmp{}.Dmedia'.format(random)


bus = random_bus()
service = path.join(path.dirname(path.abspath(__file__)), 'dummy-service')
assert path.isfile(service)
p = Popen([service, '--bus', bus])
time.sleep(0.25)

N = 10 * 1000

setup = """
import dbus

_id = 'FWV6OJYI36C5NN5DC4GS2IGWZXFCZCGJGHK35YV62LKAG7D2Z4LO4Z2S'
session = dbus.SessionBus()
Dmedia = session.get_object({!r}, '/')
""".format(bus)


def benchmark(statement):
    t = timeit.Timer(statement, setup)
    elapsed = t.timeit(N)
    rate = int(N / elapsed)
    print('{:>10,}: {}'.format(rate, statement)) 


try:
    benchmark('Dmedia.Resolve1()')
    benchmark('Dmedia.Resolve2(_id)')
    benchmark('Dmedia.Resolve3(_id)')
    benchmark('Dmedia.Resolve4(_id)')
finally:
    p.terminate()
    p.wait()
