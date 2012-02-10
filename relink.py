#!/usr/bin/python3

import sys
from os import path

from microfiber import dmedia_env
from dmedia.metastore import MetaStore
from dmedia.core import init_filestore

parentdir = path.abspath(sys.argv[1])

ms = MetaStore(dmedia_env())
(fs, doc) = init_filestore(parentdir)
ms.relink(fs)
ms.scan(fs)

