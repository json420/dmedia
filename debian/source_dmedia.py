'''apport package hook for dmedia.

(c) 2012 Novacut Inc
Author: Jason Gerard DeRose <jderose@novacut.com>
'''

import os
from os import path

from apport.hookutils import attach_file_if_exists

LOGS = (
    'dmedia-service.log',
    'dmedia-service.log.previous',
    'dmedia-gtk.log',
    'dmedia-peer-gtk.log',
)

def add_info(report):
    report['CrashDB'] = 'dmedia'
    cache = path.join(os.environ['HOME'], '.cache', 'dmedia')
    for name in LOGS:
        attach_file_if_exists(report, path.join(cache, name), name)

