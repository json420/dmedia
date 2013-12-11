#!/bin/sh

# Based on script of the same name in the Ubuntu 13.10 `hud` package.
# We should be able to drop this as soon as we drop 13.10 support.
#
#                       Original comment:
# This is a quick hack to make it so that DBus activation works as the
# DBus daemon holds onto the PID until the name gets registered.  So we
# need the PID to exist until then.  10 seconds should be more that enough
# time for the service to register the name.
#
# This can go away if we get DBus Activation for Upstart

if [ "x$UPSTART_SESSION" != "x" ]; then
	start dmedia
	sleep 10
else
	/usr/lib/dmedia/dmedia-service
fi
