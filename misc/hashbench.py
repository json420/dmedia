#!/usr/bin/env python3

import os
import optparse
import time
import zlib
import hashlib
import platform


timefunc = (time.monotonic if hasattr(time, 'monotonic') else time.time)

parser = optparse.OptionParser()
parser.add_option('--leaf',
    help='Leaf size in MiB; default=8',
    metavar='MiB',
    default=8,
    type='int',
)
parser.add_option('--count',
    help='Number of leaves to hash; default=64',
    metavar='N',
    default=64,
    type='int',
)

(options, args) = parser.parse_args()
MiB = 1024 * 1024
leaf_size = options.leaf * MiB
size = leaf_size * options.count
leaf = b'a' * leaf_size


# Build list of hashes:
hashes = [
    zlib.adler32,
    zlib.crc32,
    hashlib.md5,
    hashlib.sha1,
    #hashlib.sha224,
    hashlib.sha256,
    #hashlib.sha384,
    hashlib.sha512,
]
try:
    import skein
    hashes.extend([
        skein.skein256,
        skein.skein512,
        skein.skein1024,
    ])
except ImportError:
    print('Could not import `skein`.')
    print('Download pyskein at http://packages.python.org/pyskein/')
    print('')
try:
    import sha3
    hashes.extend([
        sha3.sha3_224,
        sha3.sha3_256,
        sha3.sha3_384,
        sha3.sha3_512,
    ])
except ImportError:
    print('Could not import `sha3`.')
    print('Download pysha3 at http://pypi.python.org/pypi/pysha3/')
    print('')


def benchmark_crc(crcfunc):
    start = timefunc()
    for i in range(options.count):
        crcfunc(leaf)
    return timefunc() - start


def benchmark(hashfunc):
    start = timefunc()
    for i in range(options.count):
        hashfunc(leaf).digest()
    return timefunc() - start


print('-' * 80)
print('Leaf size: {} MiB'.format(options.leaf))
print('Leaf count: {}'.format(options.count))
print('Total size: {} MiB'.format(size // MiB))
print('Python: {}, {}, {}'.format(
    platform.python_version(), platform.machine(), platform.system())
)


report = []
for hashfunc in hashes:
    if hashfunc in (zlib.adler32, zlib.crc32):
        avg = benchmark_crc(hashfunc)
    else:
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

output = [['Hash', 'Time', 'MB/s']]
for d in report:
    output.append(
        [
            '%(name)s' % d,
            '%(avg).2f' % d,
            '%(mbps).1f' % d,
        ]
    )

widths = [
    max(len(r[i]) for r in output) for i in range(3)
]
output.insert(1, ['=' * w for w in widths])

print('')
for row in output:
    print('  '.join(row[i].ljust(widths[i]) for i in range(3)))
print('-' * 80)
