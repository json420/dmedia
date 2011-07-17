# Authors:
#   David Green <david4dev@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Simple wrapper over the udisks dbus API to provide information about
devices.
"""

import dbus
import os

class Device(object):
    """
    Instances of this class represent udisks devices, providing a simple
    way to get device information.

    For a Device instance `device` you can access information like so:
        device[property]
    eg.
        device = Device(path="/media/EOS_DIGITAL/image.jpg")
        size = device["DeviceSize"]
    """

    @classmethod
    def enumerate_devices(cls):
        """
        Iterate through udisks devices.
        Yield dbus properties interfaces (dbus.PROPERTIES_IFACE).
        """
        bus = dbus.SystemBus()
        ud_manager_obj = bus.get_object("org.freedesktop.UDisks", "/org/freedesktop/UDisks")
        ud_manager = dbus.Interface(ud_manager_obj, 'org.freedesktop.UDisks')
        for dev in ud_manager.EnumerateDevices():
            device_obj = bus.get_object("org.freedesktop.UDisks", dev)
            yield dbus.Interface(device_obj, dbus.PROPERTIES_IFACE)

    @classmethod
    def get_by_dev(cls, dev):
        """
        Return the udisks dbus properties interface for a device which
        has the UNIX special device file `dev`. eg. /dev/sdb1.
        """
        for d in cls.enumerate_devices():
            if d.Get("org.freedesktop.UDisks.Device", "DeviceFile") == dev:
                return d

    @classmethod
    def get_by_path(cls, path):
        """
        Return the udisks dbus properties interface for a device on
        which resides the file at `path`.
        """
        path = os.path.abspath(path)
        st_dev = os.stat(path).st_dev
        major = os.major(st_dev)
        minor = os.minor(st_dev)
        for d in cls.enumerate_devices():
            if int(
                d.Get("org.freedesktop.UDisks.Device", "DeviceMajor")
            ) == major and int(
                d.Get("org.freedesktop.UDisks.Device", "DeviceMinor")
            ) == minor:
                return d

    def __init__(self, dev=None, path=None):
        """
        Either `dev` or `path` should be specified and they mustn't both
        be specified. When specified, `dev` and `path` should be strings
        (either str or unicode).

        If `dev` is specified, the device will be looked up by /dev/*
        device. For example, `dev` could be "/dev/sdb1".

        If `path` is specified, the device will be looked up for the
        device that the file or folder represented by `path` is on.
        """
        if (dev == None and path == None) or (dev != None and path != None):
            raise(Exception("You need to specify exactly one of `dev` or `path`."))
        if (dev == None):
            if type(path) == str or type(path) == unicode:
                self._d = self.__class__.get_by_path(path)
            else:
                raise(TypeError("`path` must be a string."))
        else:
            if type(dev) == str or type(dev) == unicode:
                self._d = self.__class__.get_by_dev(dev)
            else:
                raise(TypeError("`dev` must be a string."))

    def exists(self):
        """
        Return True if the udisks lookup was successful, False otherwise.
        """
        return self._d and True

    def __getitem__(self, prop):
        return self._d.Get("org.freedesktop.UDisks.Device", prop)
