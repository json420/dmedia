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

import multiprocessing
from multiprocessing import current_process
from threading import Thread, Lock
from Queue import Empty
import logging
from .constants import TYPE_ERROR

log = logging.getLogger()

_workers = {}


def isregistered(worker):
    if not (isinstance(worker, type) and issubclass(worker, Worker)):
        raise TypeError(
            'worker: must be subclass of %r; got %r' % (Worker, worker)
        )
    name = worker.__name__
    return (name in _workers)


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


def dispatch(q, worker, key, args):
    try:
        klass = _workers[worker]
        inst = klass(q, key, args)
        inst.run()
    except Exception as e:
        q.put(dict(
            signal='error',
            args=(key, exception_name(e), str(e)),
            worker=worker,
            pid=current_process().pid,
        ))
    finally:
        q.put(dict(
            signal='terminate',
            args=(key,),
            worker=worker,
            pid=current_process().pid,
        ))


class Worker(object):
    def __init__(self, q, key, args):
        self.q = q
        self.key = key
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
            args=(self.key,) + args,
        ))

    def run(self):
        self.execute(*self.args)

    def execute(self, *args):
        raise NotImplementedError(
            '%s.execute()' % self.name
        )


class Manager(object):
    def __init__(self, callback=None):
        if not (callback is None or callable(callback)):
            raise TypeError(
                'callback must be callable; got %r' % callback
            )
        self._callback = callback
        self._running = False
        self._workers = {}
        self._q = multiprocessing.Queue()
        self._lock = Lock()
        self._thread = None

    def _signal_thread(self):
        while self._running:
            try:
                self._process_message(self._q.get(timeout=1))
            except Empty:
                pass

    def _process_message(self, msg):
        log.info('%(signal)s %(args)r', msg)
        with self._lock:
            signal = msg['signal']
            args = msg['args']
            handler = getattr(self, 'on_' + signal)
            handler(*args)

    def on_terminate(self, key):
        p = self._workers.pop(key)
        p.join()

    def on_error(self, key, exception, message):
        pass

    def start(self):
        with self._lock:
            if self._running:
                return False
            self._running = True
            self._thread = Thread(target=self._signal_thread)
            self._thread.daemon = True
            self._thread.start()
            return True

    def kill(self):
        if not self._running:
            return False
        self._running = False
        self._thread.join()  # Cleanly shutdown _signal_thread
        with self._lock:
            for p in self._workers.values():
                p.terminate()
                p.join()
            self._workers.clear()
            return True

    def do(self, worker, key, *args):
        """
        Start a process identified by *key*, using worker class *name*.
        """
        if key in self._workers:
            return False
        p = multiprocessing.Process(
            target=dispatch,
            args=(self._q, worker, key, args),
        )
        p.daemon = True
        self._workers[key] = p
        p.start()
        return True

    def kill_job(self, key):
        with self._lock:
            if key not in self._workers:
                return False
            p = self._workers.pop(key)
            p.terminate()
            p.join()
            return True

    def emit(self, signal, *args):
        """
        Emit a signal to higher-level code.
        """
        if self._callback is None:
            return
        self._callback(signal, args)
