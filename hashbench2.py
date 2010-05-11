#!/usr/bin/env python2

import sys
import os
from os import path
import optparse
import time
import hashlib
import platform

CHUNK = 2 ** 20  # Read in chunks of 1 MiB
assert CHUNK == 1024 * 1024


parser = optparse.OptionParser(
	usage='Usage: %prog FILE',
)
parser.add_option('--runs',
    help='Runs per algorithm; default=4',
    metavar='N',
    default=4,
    type='int',
)

(options, args) = parser.parse_args()
if len(args) != 1:
    parser.print_usage()
    sys.exit('ERROR: must provide FILE to hash')
src = path.abspath(args[0])
if not path.isfile(src):
    parser.print_usage()
    sys.exit('ERROR: not a file: %r' % src)
size = path.getsize(src)


# Build list of hashes:
hashes = [
    hashlib.md5,
    hashlib.sha1,
    hashlib.sha224,
    hashlib.sha256,
    hashlib.sha384,
    hashlib.sha512,
]


def hash_file(filename, hashfunc):
    """
    Compute the content-hash of the file *filename*.
    """
    fp = open(filename, 'rb')
    h = hashfunc()
    while True:
        try:
            chunk = fp.read(CHUNK)
        except KeyboardInterrupt:
            print ''
            sys.exit()
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()


def benchmark(hashfunc):
    start = time.time()
    for i in range(options.runs):
        hash_file(src, hashfunc)
    return (time.time() - start) / options.runs

print '-' * 80
print 'File size: %d bytes' % path.getsize(src)
print 'Buffer size: %d KiB' % (CHUNK / 1024)
print 'Runs per algorithm: %d' % options.runs
print 'Python: %s, %s, %s' % (
    platform.python_version(), platform.machine(), platform.system()
)


# Do an md5sum once to get the file into the page cache:
hash_file(src, hashlib.md5)

report = []
for hashfunc in hashes:
    avg = benchmark(hashfunc)
    bytes_per_second = (size / avg)
    report.append(
        dict(
            name=hashfunc.__name__,
            avg=avg,
            bytes_per_second=bytes_per_second,
            mbps=(bytes_per_second / 10 ** 6),
        )
    )

output = [['Hash', 'Avg.', 'MB/s']]
for d in report:
    output.append(
        [
            '%(name)s' % d,
            '%(avg).3f' % d,
            '%(mbps).1f' % d,
        ]
    )

widths = [
    max(len(r[i]) for r in output) for i in range(3)
]
output.insert(1, ['=' * w for w in widths])

print ''
for row in output:
    print '  '.join(row[i].ljust(widths[i]) for i in range(3))
print '-' * 80
