'''apport package hook for dmedia.

(c) 2012 Novacut Inc
Author: Jason Gerard DeRose <jderose@novacut.com>
'''

import os
from os import path

from apport.hookutils import attach_file_if_exists

LOGS = (
    ('ServiceLog', 'dmedia-service.log'),
    ('GtkLog', 'dmedia-gtk.log'),
)

def add_info(report):
    report['CrashDB'] = 'dmedia'
    cache = path.join(os.environ['HOME'], '.cache', 'dmedia')
    for (key, name) in LOGS:
        log = path.join(cache, name)
        attach_file_if_exists(report, log, key)

