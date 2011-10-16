from gi.repository import GObject

from dmedia.service import dbus


def report(objpath):
    keys = [
        'DeviceIsMounted',
        'DeviceIsPartition',
        'DeviceIsSystemInternal',
        'DeviceMountPaths',
    ]
    props = dbus.get_device_props(objpath)
    for key in sorted(keys):
        print('  {}: {!r}'.format(key, props[key]))


def on_DeviceAdded(udisks, objpath):
    print('DeviceAdded', objpath)
    report(objpath)


def on_DeviceChanged(udisks, objpath):
    print('DeviceChanged', objpath)
    report(objpath)


def on_DeviceRemoved(udisks, objpath):
    print('DeviceRemoved', objpath)


udisks = dbus.UDisks()
udisks.connect('DeviceAdded', on_DeviceAdded)
udisks.connect('DeviceChanged', on_DeviceChanged)
udisks.connect('DeviceRemoved', on_DeviceRemoved)
udisks.monitor()


mainloop = GObject.MainLoop()
mainloop.run()
