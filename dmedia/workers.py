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
from .abstractcouch import get_server, get_db


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


def dispatch(worker, env, q, key, args):
    """
    Dispatch a worker in this proccess.

    :param worker: name of worker class, eg ``'ImportWorker'``
    :param env: a ``dict`` containing run-time information like the CouchDB URL
    :param q: a ``multiprocessing.Queue`` or similar
    :param key: a key to uniquely identify this worker among active workers
        controlled by the `Manager` that launched this worker
    :param args: arguments to be passed to `Worker.run()`
    """
    pid = current_process().pid
    log.debug('** dispatch in process %d: worker=%r, key=%r, args=%r',
        pid, worker, key, args
    )
    try:
        klass = _workers[worker]
        inst = klass(env, q, key, args)
        inst.run()
    except Exception as e:
        log.exception('exception in procces %d, worker=%r', pid, worker)
        q.put(dict(
            signal='error',
            args=(key, exception_name(e), str(e)),
            worker=worker,
            pid=pid,
        ))
    finally:
        q.put(dict(
            signal='terminate',
            args=(key,),
            worker=worker,
            pid=pid,
        ))


class Worker(object):
    def __init__(self, env, q, key, args):
        self.env = env
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


class CouchWorker(Worker):
    def __init__(self, env, q, key, args):
        super(CouchWorker, self).__init__(env, q, key, args)
        self.server = get_server(env)
        self.db = get_db(env, self.server)


class Manager(object):
    def __init__(self, env, callback=None):
        if not (callback is None or callable(callback)):
            raise TypeError(
                'callback must be callable; got %r' % callback
            )
        self.env = env
        self._callback = callback
        self._q = multiprocessing.Queue()
        self._lock = Lock()
        self._workers = {}
        self._running = False
        self._thread = None
        self.name = self.__class__.__name__

    def _start_signal_thread(self):
        assert self._running is False
        assert len(self._workers) > 0
        if self._thread is not None:
            self._thread.join()
        self._running = True
        self._thread = Thread(target=self._signal_thread)
        self._thread.daemon = True
        self._thread.start()

    def _kill_signal_thread(self):
        assert self._running is True
        assert self._thread.is_alive()
        assert len(self._workers) == 0
        self._running = False

    def _signal_thread(self):
        while self._running:
            try:
                self._process_message(self._q.get(timeout=0.5))
            except Empty:
                pass

    def _process_message(self, msg):
        log.info('[from %(worker)s %(pid)d] %(signal)s %(args)r', msg)
        with self._lock:
            signal = msg['signal']
            args = msg['args']
            handler = getattr(self, 'on_' + signal, None)
            if callable(handler):
                handler(*args)
            else:
                self.emit(signal, *args)

    def first_worker_starting(self):
        pass

    def last_worker_finished(self):
        pass

    def on_terminate(self, key):
        p = self._workers.pop(key)
        p.join()
        if len(self._workers) == 0:
            self._kill_signal_thread()
            self.last_worker_finished()

    def on_error(self, key, exception, message):
        log.error('%s %s: %s: %s', self.name, key, exception, message)

    def kill(self):
        if not self._running:
            return False
        log.info('Killing %s', self.name)
        self._running = False
        self._thread.join()  # Cleanly shutdown _signal_thread
        with self._lock:
            for p in self._workers.values():
                p.terminate()
                p.join()
            self._workers.clear()
            return True

    def get_worker_env(self, worker, key, args):
        return dict(self.env)

    def start_job(self, worker, key, *args):
        """
        Start a process identified by *key*, using worker class *name*.

        :param worker: name of worker class, eg ``'ImportWorker'``
        :param key: a key to uniquely identify new `Worker` among active workers
            controlled by this `Manager`
        :param args: arguments to be passed to `Worker.run()`
        """
        with self._lock:
            if key in self._workers:
                return False
            if len(self._workers) == 0:
                self.first_worker_starting()
            env = self.get_worker_env(worker, key, args)
            p = multiprocessing.Process(
                target=dispatch,
                args=(worker, env, self._q, key, args),
            )
            p.daemon = True
            self._workers[key] = p
            p.start()
            if len(self._workers) == 1:
                self._start_signal_thread()
            return True

    def kill_job(self, key):
        with self._lock:
            if key not in self._workers:
                return False
            p = self._workers.pop(key)
            p.terminate()
            p.join()
            return True

    def list_jobs(self):
        return sorted(self._workers)

    def emit(self, signal, *args):
        """
        Emit a signal to higher-level code.
        """
        if self._callback is None:
            return
        self._callback(signal, args)


class CouchManager(Manager):
    def __init__(self, env, callback=None):
        super(CouchManager, self).__init__(env, callback)
        self.server = get_server(env)
        self.db = get_db(env, self.server)
