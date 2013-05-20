"""
Gio docs:

    http://developer.gnome.org/gio/stable/GVolumeMonitor.html
"""

from gi.repository import GLib, Gio, GObject
import json

GLib.threads_init()
mainloop = GLib.MainLoop()


def on_mount_added(monitor, mount):
    print('mount-added', mount)


def on_mount_changed(monitor, mount):
    print('mount-changed', mount)


def on_mount_pre_unmount(monitor, mount):
    print('mount-pre-unmount', mount)


def on_mount_removed(monitor, mount):
    print('mount-removed', mount)


# Use VolumeMonitor.get(), not VolumeMonitor() or VolumeMonitor.new():
monitor = Gio.VolumeMonitor.get()
monitor.connect('mount-added', on_mount_added)
monitor.connect('mount-changed', on_mount_changed)
monitor.connect('mount-pre-unmount', on_mount_pre_unmount)
monitor.connect('mount-removed', on_mount_removed)

for mount in monitor.get_mounts():
    root = mount.get_root()
    print(mount.get_name(), root.get_path())
    

mainloop.run()




    



