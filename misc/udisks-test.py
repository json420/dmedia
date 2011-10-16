#!/usr/bin/python3

import os
from os import path
from subprocess import check_output
import json

from gi.repository import Gio


class DBus:
    def __init__(self, conn):
        self.conn = conn

    def get(self, bus, objpath, iface=None):
        if iface is None:
            iface = bus
        return Gio.DBusProxy.new_sync(
            self.conn, 0, None, bus, objpath, iface, None
        )

    def get_async(self, callback, bus, objpath, iface=None):
        if iface is None:
            iface = bus
        Gio.DBusProxy.new(
            self.conn, 0, None, bus, objpath, iface, None, callback, None
        )


session = DBus(Gio.bus_get_sync(Gio.BusType.SESSION, None))
system = DBus(Gio.bus_get_sync(Gio.BusType.SYSTEM, None))


BUS = 'org.freedesktop.UDisks'
PROPS = 'org.freedesktop.DBus.Properties'
ECRYPTFS = '/home/.ecryptfs/'


udisks = system.get(BUS, '/org/freedesktop/UDisks')


class DeviceNotFound(Exception):
    def __init__(self, basedir):
        self.basedir = basedir
        super().__init__(basedir)


def get_device_props(objpath):
    device = system.get(BUS, objpath, PROPS)
    return device.GetAll('(s)', 'org.freedesktop.UDisks.Device')


def by_major_minor(basedir):
    st_dev = os.stat(basedir).st_dev
    major = os.major(st_dev)
    minor = os.minor(st_dev)
    try:
        return udisks.FindDeviceByMajorMinor('(xx)', major, minor)
    except Exception:
        raise DeviceNotFound(basedir)


def get_partition(basedir):
    try:
        return by_major_minor(basedir)
    except DeviceNotFound as e:
        pass
    private = path.join(basedir, '.Private')
    if path.islink(private) and os.readlink(private).startswith(ECRYPTFS):
        return by_major_minor(private)
    raise e


def get_info(basedir):
    objpath = get_partition(basedir)
    part = get_device_props(objpath)
    assert part['DeviceIsPartition'] is True
    drive = get_device_props(part['PartitionSlave'])
    assert drive['DeviceIsDrive'] is True
    return {
        'partition': {
            'uuid': part['IdUuid'],
            'bytes': part['DeviceSize'],
            'filesystem': part['IdType'],
            'filesystem_version': part['IdVersion'],
            'label': part['IdLabel'],
            'number': part['PartitionNumber'],
        },
        'drive': {
            'serial': drive['DriveSerial'],
            'bytes': drive['DeviceSize'],
            'block_bytes': drive['DeviceBlockSize'],
            'vendor': drive['DriveVendor'],
            'model': drive['DriveModel'],
            'revision': drive['DriveRevision'],
            'rotational': drive['DriveIsRotational'],
            'partition_scheme': drive['PartitionTableScheme'],
            'internal': drive['DeviceIsSystemInternal'],
        },
    }


info = {'devices': {}, 'paths': {}}

print('\nEnumerateDevices:')
for objpath in udisks.EnumerateDevices():
    print(objpath)
    props = get_device_props(objpath)
    del props['DriveAtaSmartBlob']
    info['devices'][objpath] = props

print('\nTesting paths:')
user = path.abspath(os.environ['HOME'])
dirs = ['/', '/tmp','/home', user]
private = path.join(user, '.Private')
if path.islink(private):
    dirs.append(private)
for name in os.listdir('/media'):
    dirs.append(path.join('/media', name))
for d in dirs:
    print(d)
    try: 
        o = get_info(d)
    except DeviceNotFound:
        o = None
    info['paths'][d] = o


print('\nGetting info from `df`:')
lines = check_output(['/bin/df', '-T', '--si']).decode('utf-8').splitlines()
info['df'] = lines
for line in lines:
    print(line)


dst = path.join(path.dirname(path.abspath(__file__)), 'udisks-report.json')
dst_fp = open(dst, 'w')
json.dump(info, dst_fp, sort_keys=True, indent=4)
print('\nUDisks info dumped to:')
print(dst)

