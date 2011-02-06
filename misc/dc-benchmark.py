#!/usr/bin/env python

import tempfile
import cProfile

(fileno, filename) = tempfile.mkstemp()

def run():
    from desktopcouch.records.server import  CouchDatabase
    dc = CouchDatabase('dmedia', create=True)


cProfile.run('run()', filename)


import pstats
p = pstats.Stats(filename)
p.sort_stats('cumulative').print_stats(20)
