#!/usr/bin/env python

"""
Create API documentation from source code using epydoc.
"""

from __future__ import print_function

import sys
import os
from os import path
import shutil
from subprocess import call

import dmedia as package


tree = path.dirname(path.abspath(__file__))
os.chdir(tree)

apidoc = path.join(tree, 'apidoc')
init = path.join(tree, package.__name__, '__init__.py')
name = '%s %s API documentation' % (package.__name__, package.__version__)


if not path.exists(init):
    print('Cannot find %r' % init)
    print('Error: %r does not appear to be project tree' % tree)
    sys.exit(1)
if path.isdir(apidoc):
    print('Removing old %r' % apidoc)
    shutil.rmtree(apidoc)
if path.lexists(apidoc):
    print('Error: %r is not a directory' % apidoc)
    sys.exit(1)

cmd = ['epydoc', '-v', '--html', '--no-frames',
    '--docformat', 'restructuredtext',
    '--name', name,
    '--exclude', '%s.tests' % package.__name__,
    '--output', apidoc,
    package.__name__
]

sys.exit(call(cmd))
