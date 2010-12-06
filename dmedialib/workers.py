# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
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

"""
Multi-process workers.
"""

from multiprocessing import current_process
from .constants import TYPE_ERROR

_workers = {}


def register(worker):
    if not (isinstance(worker, type) and issubclass(worker, Worker)):
        raise TypeError(
            'worker: must be subclass of %r; got %r' % (Worker, worker)
        )
    name = worker.__name__
    if name in _workers:
        raise ValueError(
            'cannot register %r, worker with name %r already registered' % (
                worker, name
            )
        )
    _workers[name] = worker


def exception_name(exception):
    """
    Return name of ``Exception`` subclass or instance *exception*.

    Works with ``Exception`` instances:

    >>> exception_name(ValueError('bad value!'))
    'ValueError'

    And with ``Exception`` subclasses:

    >>> exception_name(ValueError)
    'ValueError'

    """
    if isinstance(exception, Exception):
        return exception.__class__.__name__
    return exception.__name__


def dispatch(name, q, args):
    try:
        klass = _workers[name]
        inst = klass(q, args)
        inst.run()
    except Exception as e:
        q.put(dict(
            signal='Error',
            args=(exception_name(e), str(e)),
            worker=name,
            worker_args=args,
            pid=current_process().pid,
        ))
    finally:
        q.put(dict(
            signal='_terminate',
            worker=name,
            args=args,
            pid=current_process().pid,
        ))


class Worker(object):
    def __init__(self, q, args):
        self.q = q
        self.args = args
        self.pid = current_process().pid
        self.name = self.__class__.__name__

    def emit(self, signal, *args):
        """
        Put *signal* into message queue, optionally with *args*.

        To aid debugging and logging, the worker class name and worker process
        ID are included in the message.

        The message is a ``dict`` with the following keys:

            *worker* - the worker class name
            *pid* - the process ID
            *signal* - the signal name
            *args* - signal arguments
        """
        self.q.put(dict(
            worker=self.name,
            pid=self.pid,
            signal=signal,
            args=args,
        ))

    def run(self, adapter=None):
        raise NotImplementedError(
            '%s.run()' % self.name
        )
