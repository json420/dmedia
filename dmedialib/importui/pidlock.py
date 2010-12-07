#!/usr/bin/env python

# Authors:
#   David Green <david4dev@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
import os


class PidLock(object):
    """
    A class for creating a 'pid lock', which can be used to ensure only
    one instance of an application is running at any one time.
    """
    def __init__(self, appname):
        """
        Create a pid lock object for application with the name specified by
        the appname string.
        """
        self.appname = appname
        self.pid_file = os.path.join('/tmp', self.appname + '.pid')


    def get(self):
        """
        Checks to see if this application is already running.
        Returns True if the application isn't running, meaning it is
        safe to start an instance of it.
        Returns False otherwise, meaning that this application instance
        shouldn't run.
        """
        if os.path.exists(self.pid_file): #check if pid file exists

            pid_file_handler = open(self.pid_file, "r")
            pid = pid_file_handler.readline() #read PID
            try:
                pid = int(pid)
            except:
                return True #invalid pid, start indicator
            pid_file_handler.close()

            try:
                os.kill(pid, 0) #are you running @pid?
            except OSError: #if not
                return True
                #if pid isn't running, indicator-dmedia isn't running, so
                #it is safe to start the indicator

            proc_dir = os.path.join('/proc', str(pid))
            if os.path.exists(proc_dir):
                proc_command = os.path.join(proc_dir, 'cmdline')
                proc_command_handler = open(proc_command, "r")
                cmd = proc_command_handler.readline()
                proc_command_handler.close()
                if cmd.find(self.appname) != -1: #is the pid actually from indicator-dmedia?
                    return False #There is already an indicator running, start the indicator

            else:
                return True #it is safe to start the indicator

        else:
            return True #if pid_file doesn't exist, start the indicator


    def create(self):
        """
        Creates a pid lock file for this application.
        This blocks other instances from running
        (if they check the pid lock file using pid_lock_object.get())
        """
        #Create the pid file if it doesn't exist
        #and write the current pid to the file
        pid_file_handler = open(self.pid_file, "w")
        pid_file_handler.write("%s" % str(os.getpid()))
        pid_file_handler.close()


    def release(self):
        """
        Releases the current pid lock, allowing other instances of this
        application to run.
        """
        #Write an empty string to the pid file
        #This could be potentially used to show the program quit
        #successfully
        pid_file_handler = open(self.pid_file, "w")
        pid_file_handler.write('')
        pid_file_handler.close()


    def main(self, func):
        """
        Runs the main function (provided by the func argument) of this
        application but does not let other instances of this application
        do the same.
        """
        if self.get():
            self.create()
            func()
            self.release()
