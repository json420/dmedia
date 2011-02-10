from __future__ import print_function

import sys
import os
from os import path
import threading
from base64 import b32decode
from dmedia.filestore import FileStore, HashList
from BitTorrent.download import download

# URL of test torrent:
url = 'http://novacut.s3.amazonaws.com/novacut_test_video.tgz?bittorrent'

# Size of novacut_test_video.tgz (in bytes):
size = 448881430

# Known top hash and leaf hashes for novacut_test_video.tgz:
chash = 'RPJVUTSXO7VSSVO52SKOWDYQLBJQIAJR'

leaves_b32 = [
    'D2VQLE34C43A2FZVNHUBWK563YV6VE3G',
    'FNPGZPJV24JAQHKPSQE5TEJYRTJ6YVV2',
    'BB7SUEQKQ6IWJ37W5XQTPNBDXLP7K3I4',
    'L7RMVOGEY4T4LOANEVDF76V5MYQD4EDZ',
    '3HJIPXHLFUZOK7RWZTJMISYBDCGL4XJ2',
    'DUMQFT4WLLQVAMOJJ7ZEKA5XY44WSGLW',
    'DLRJWLGY3LNSZM6SELIWBGMLFARNFKAO',
    'JKRYIV54JXSCKIABWJKOALWGVJDS2VAS',
    'XNUEGNTJKGG53PSQFXZF4AORR43WSIUY',
    '7357UI2XNY7IV3QYJBVEWI4BS2MDCCS4',
    '5MXZHY45T7VT73Y2O5SQ3STV5WFUUMLC',
    'S5JEOS2AEPHJ62TG2O53MG3QLOBF37SR',
    'JN2JMACM4JMMBDTWYEH55MXQKBA4LAHO',
    'F356ET4GFVEWMBGXPY4RAXJMHW4DK24H',
    'O5FF6C7AKZN5EWURGHTYVV7OOW2ZDSUD',
    'IZPOPYEHBRKT2UG3CD2ARPU7RG47SBAF',
    '7NVISX75YR7ZIXRHKOXRR3FBCFAX7LJX',
    'J3KC25YM5EPTDYDAJRQR3BBXABRGN7ND',
    '5BHL3KQXGLRL4ADLOL6GG7M3XV5MP3LB',
    'NX6QXUHI3DW5YLNIUIBVSD37RY47C2OW',
    'IHGYFQIDXAY7PZMG5N4LPBWW3ID6CAGN',
    'PMVJJBPCLI7AUR3JH2YGD566O4556ERQ',
    '6LNHOZOHOOI4TRFZF5MAFXIYB7YMJMY4',
    'I5Z5TRBUDC766IXHU344IIOYK3XPQZKB',
    'IIMKZOB2HORV5NCKH2VA62HL3TKTGFNY',
    'WPV3CIURJSNQUU32S5LXGFXCU7VUDDSG',
    'GDYD6WC5B4TVUGGTTPHIOZMRQ7O5BOCI',
    '6TIJUAVQ2NBVNJ4PUHVI6HJNJDY4LPLQ',
    'VYKXDR273C4OMF3ZFGIWBYJKBKN53PWM',
    'MPO2CEDG2BQEJAGVMG6EVPLXOYNMCE2Z',
    'KM2DPVSYLEP7CWGONGTUWCG6723ZEI2S',
    'XFZHYYAVYGD52ZHUUC3EJLJFVJPXPP3N',
    '76TEHRTVM4EZN64V2ZJWACTYHAFKZGYK',
    'TVUDE3QMND6LBMODPXYPJO5HU3WWFPKV',
    'U6WUGLYHX3QOVDHKTE77BF3UDK7M5QNN',
    'KCBMZMQOLHHMJ4ZCENWCGYQD5NALJBDM',
    'CRUE2N6JOLBUSCPEVP2HSKOWYLDL3BIO',
    '5SSD2NXC7T6TMFGUKYMN5T6GIYYDSYD5',
    'UFF7AROVWWPBKA4X7OQRTTV33JYAOWWK',
    'BBMMX2JNTABP7526VCRTQEWSISAD5BF2',
    'EO56LHHPWCO6HESXM2265NXIM7IHWJXQ',
    'ZEPQLT33EI2GT6ZHU6K3SPDIG5JDYRTY',
    'JIFAJSGJDQCD6U6Q3WILME6PMQ6A572H',
    'T7Y2ACNTAWRAGGJUH2EL3767A5VFVUDL',
    'G24P5AK6S4XGVUFH5VAWFOEUGHK3OXL2',
    '2DDWMB5OMRZ6C66EF65ICCZLP7OKL76D',
    'BJ2D2JPVAYQDBXJXFAHDIM6NAGCDFR7Y',
    '2ZLWKSPWYIGSXGOKPCCLCFMKTQJ5KQHB',
    'AHULNH6XZD5HEAL2G773JDQGXA4ZRBNF',
    'IMFTSDBA4GXGIX5XP6ZBXSHMFGOMSPAB',
    'BSX4DJRY5WJQ5G6HSPD4DMAKIJMDU7Q5',
    '6NEGE6DDSJAVCU7XJEZY7VGVAQZOQHHZ',
    'OW2RYNVNQF5EXNT7NQ67MNXGSIDDRMZZ',
    'DXJURGCSUQHGJMN3VGCKVXZV6AIUZWFC',
]

leaves = [b32decode(l) for l in leaves_b32]


# Create a FileStore in ~/.dmedia_test/
home = path.abspath(os.environ['HOME'])
base = path.join(home, '.dmedia_test')
store = FileStore(base)

# Get tmp path we will write file to as we download:
tmp = store.temp(chash, 'tgz', create=True)
print('Will write file to:\n  %r' % tmp)


# All the callbacks we need:
def choose_file(default, size, saveas, dirpath):
    print('choose_file(%r, %r, %r, %r)' % (default, size, saveas, dirpath))

def progress(d):
    print('progress:')
    for key in sorted(d):
        print('  %s = %r' % (key, d[key]))


def finished():
    print('finished!')


def error(msg):
    print('error: %r' % msg)


def newpath(self, pathname):
    print('newpath: %r' % pathname)


params = [
    '--url', url,
    '--saveas', tmp,
]

print('Downloading:\n  %r' % url)
download(
    params,
    choose_file,
    progress,
    finished,
    error,
    threading.Event(),
    80,
    newpath
)
