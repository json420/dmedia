from dmedia.service.dbus import Device

part = Device('/org/freedesktop/UDisks/devices/sde1')
drive = Device(part['PartitionSlave'])

fstype = part['IdType']
label = part['IdLabel']

print(fstype, label)

part.FilesystemUnmount([])
part.FilesystemCreate(fstype, ['label={}'.format(label)])
#drive.DriveDetach()  # Nope, will powerdown card *reader* which needs to be unpluged to work again
