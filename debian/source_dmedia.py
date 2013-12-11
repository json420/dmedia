"""
Apport package hook for dmedia (requires Apport 2.5 or newer).

(c) 2012 Novacut Inc
Author: Jason Gerard DeRose <jderose@novacut.com>
"""

import os
from os import path

from apport.hookutils import attach_file_if_exists

# Note that Apport will automatically attach ~/.cache/upstart/dmedia.log
LOGS = (
    'dmedia-gtk.log',
    'dmedia-peer-gtk.log',
)

def add_info(report):
    report['CrashDB'] = "{'impl': 'launchpad', 'project': 'dmedia'}"
    cache = path.join(os.environ['HOME'], '.cache', 'dmedia')
    for name in LOGS:
        attach_file_if_exists(report, path.join(cache, name), name)

