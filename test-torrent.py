#!/usr/bin/env python

"""
Tests downloading torrent from:

http://novacut.s3.amazonaws.com/37GDNHANX7RCBMBGTYLSIK7TMTUQSKDS.mov?torrent
"""

from __future__ import print_function

import sys
import os
from os import path
import time
from base64 import b32decode, b64decode
from dmedia.downloader import TorrentDownloader
from dmedia.filestore import FileStore


# Known size, top hash, and leaf hashes for test video:
size = 44188757

chash = '37GDNHANX7RCBMBGTYLSIK7TMTUQSKDS'

leaves_b32 = [
    'TDCPJHYQVEVCTMLIKEQITVMJKSUIETHD',
    'UAG5HQCLH6PGA4RAYXDEFRNCSZDTMLXU',
    '2DGBOSUSSDG5OKASXNQTJG3MANGL4H2U',
    'D4TMBNAQOOFMIWB2ATT2GR7Y262EATVM',
    'SEYVEQPAVCROXPYZWIXN4YZRHOZV2MWV',
    'B2I7VCLIVV4LBSRTGSRQNNXDDPWCKNLA',
]

leaves = [b32decode(l) for l in leaves_b32]


# The .torrent file, base64 encoded:
tdata = """
ZDg6YW5ub3VuY2U0MjpodHRwOi8vdHJhY2tlci5hbWF6b25hd3MuY29tOjY5NjkvYW5ub3VuY2Ux
Mzphbm5vdW5jZS1saXN0bGw0MjpodHRwOi8vdHJhY2tlci5hbWF6b25hd3MuY29tOjY5NjkvYW5u
b3VuY2VlZTQ6aW5mb2Q2Omxlbmd0aGk0NDE4ODc1N2U0Om5hbWUzNjozN0dETkhBTlg3UkNCTUJH
VFlMU0lLN1RNVFVRU0tEUy5tb3YxMjpwaWVjZSBsZW5ndGhpMjYyMTQ0ZTY6cGllY2VzMzM4MDqr
KimqIY/Y6lI6kGX+koXSVvVYgXilfN8qnUAk31PJttEN8RXgagd3dLD41PXZjf5KXt84gp1i8Cgh
pfPOKRi3wBOGwNoq2JCjiHiW4wEobgaePx3alOno8Xl/phRuRoUoK/FgPJ6fm5JRulnrh5lonsmJ
iShyE+B9N5Kax2lFN5t6B1W+6y51ROzBDep9E8MaBobFyINvHkRsDY0mNgpTpwdtqAiZTOzsj9L4
YE966dNMTBVXKWC8i5pSEuRN72Bf7wXF9DoOvBaNk1vMpzKzd+GGDRpKYoZcROuyUbEzYMuI+HgE
5CvZGZZH01yVfelfotaMK1ogSShZAserPHEGLEfNwBMn51wSrLGrYiGN770n4O3tLSpmaZ1ZGaE5
Ot2nZ+ZmSJmcZWF5Uu6e07QONhziX3SYn58rvqON7fJ/mz2GiO+9ARMfv/pc6ElbaVIAEHJy2Yej
5uYIRtaaeraRG/KFgzvuxidwtePSyZzoM+1j/BaTDig+76N1hrei6R8/oUkU+rtn2MjYwaJtH+y5
/iLp418vHyjqTUsFNGGBh24nJT7LcXhakfkkjmDmCKiZ6SDVbKW47EfpR8lCnwnHC4KXz7l/yddv
m2Oc6fuUAX11DEffcSclEtPx0/VEtih1uYgoovq6ef4Eg09PwRmEQ1mfy99yCk62WSEefeYWDHq0
WO7ih+3mPTaimIMp6lClIf3DRszxe2bta3C/D+J4E+NtAsSSzkKn1MK5w+SFxj3ooQmrl1jaYv+I
QzBeVWb5KRDpzwuIr7LwaeV4/ClqGi1p4c+juohEFoLsrttB0xN9dRZcOtGVwMXqDrz4ukAtla7l
y+A93K/X60idwWteUlDDiZ8nK//1wmnaS5i1dDhEU+s+3M6JPB/w0pDjqoRFcuc99om0+7GS3E0V
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
"""


# Create a FileStore in ~/.dmedia_test/
home = path.abspath(os.environ['HOME'])
base = path.join(home, '.dmedia_test')
fs = FileStore(base)

t = TorrentDownloader(b64decode(tdata), fs, chash, 'mov')
t.run()

assert path.getsize(fs.path(chash, 'mov')) == size
