from gi.repository import GObject

from dmedia.service import dbus


def report(objpath):
    keys = [
        'DeviceIsMounted',
        'DeviceIsPartition',
        #'DeviceIsSystemInternal',
        'DeviceMountPaths',
        'DriveConnectionSpeed',
        #'DriveConnectionInterface',
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

    
def callback(udisks, *args):
    signal = args[-1]
    print(signal, args[0], args[1])


udisks = dbus.UDisks()

signals = ['card-inserted', 'store-added', 'card-removed', 'store-removed']
for name in signals:
    udisks.connect(name, callback, name)

udisks.monitor()


mainloop = GObject.MainLoop()
mainloop.run()
