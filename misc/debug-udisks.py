from dmedia.udisks import Device
from sys import argv


for dev_iface in Device.enumerate_devices():
    d=Device(path='/')
    d._d = dev_iface
    f = str(d["DeviceFile"])
    mounts = []
    if d["DeviceIsMounted"]:
        mounts =  map(str, d["DeviceMountPaths"])
    print("{} {!r}".format(f, mounts))


for path in argv[1:]:
    d=Device(path=path)
    print("{!r} -> {!r}".format(path, str(d["DeviceFile"])))    


