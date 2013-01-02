#!/usr/bin/python3

import os
from os import path
import time
from base64 import b32encode
from subprocess import Popen
import timeit

import dbus


def random_bus():
    random = b32encode(os.urandom(10)).decode('utf-8').lower()
    return 'tmp{}.Dmedia'.format(random)


bus = random_bus()
service = path.join(path.dirname(path.abspath(__file__)), 'dummy-service')
assert path.isfile(service)
p = Popen([service, '--bus', bus])
time.sleep(1)
session = dbus.SessionBus()
Dmedia = session.get_object(bus, '/')

N = 5 * 1000

setup = """
import dbus

session = dbus.SessionBus()
Dmedia = session.get_object({!r}, '/')

ids = [
    'FWV6OJYI36C5NN5DC4GS2IGWZXFCZCGJGHK35YV62LKAG7D2Z4LO4Z2S',
    'OB756PX5V32JMKJAFKIAJ4AFSFPA2WLNIK32ELNO4FJLJPEEEN6DCAAJ',
    'QSOHXCDH64IQBOG2NM67XEC6MLZKKPGBTISWWRPMCFCJ2EKMA2SMLY46',
    'BQ5UTB33ML2VDTCTLVXK6N4VSMGGKKKDYKG24B6DOAFJB6NRSGMB5BNO',
    'ER3LDDZ2LHMTDLOPE5XA5GEEZ6OE45VFIFLY42GEMV4TSZ2B7GJJXAIX',
    'R6RN5KL7UBNJWR5SK5YPUKIGAOWWFMYYOVESU5DPT34X5MEK75PXXYIX',
]
_id = ids[0]

""".format(bus)


def benchmark(statement):
    t = timeit.Timer(statement, setup)
    elapsed = t.timeit(N)
    rate = int(N / elapsed)
    print('{:>10,}: {}'.format(rate, statement)) 


try:
    benchmark('Dmedia.Empty()')
    benchmark('Dmedia.Echo(_id)')
    benchmark('Dmedia.Resolve(_id)')
    benchmark('Dmedia.ResolveMany(ids)')
finally:
    Dmedia.Kill()
    p.wait()
