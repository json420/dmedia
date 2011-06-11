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

This module implements the `Manager` and `Worker` classes that add some
conveniences on top of the Python multiprocessing module.

Any heavy lifting dmedia does (importing, downloading, uploading, verifying,
etc) is done in a subprocess created with multiprocessing.Process().  This
allows us to fully utilize multicore processors, plus makes dmedia more robust
as things can go horribly wrong in a Worker without crashing the main process.
It also keeps the memory footprint of the main process smaller and more stable
over time, which is important as dmedia is a long running process (whether as a
DBus service or a pure server).

To start a new `Worker`, a `Manager` launches a new process and passes that
process a description of the job, plus a queue that the `Worker` uses to send
signals to the `Manager`.  The communication is one-way: once the `Worker` is
started, the `Manager` has no way to signal the `Worker`, the only thing it can
do is kill the `Worker`.

Workers need to be atomic, must be able to be killed at any time without leaving
the job in an undefined state.  Some workers (like `DownloadWorker`) will
themselves intelligently resume a job where they left off (resume the download).
Other workers (like `ImportWorker`) will simply start over from the beginning.
But either way, the status of the job itself must be atomic: it's finished, or
it's not, with no gray area.

A `Worker` sends signals to the `Manager` over the queue.  These signals are
largely used to provide UI status updates (stuff like a progress bar for a
specific file being downloaded).  Typically these signals will be emitted over
DBus, which is how apps built on dmedia will show progress, status, etc to their
users.  But as the core dmedia bits also need to run on headless servers, the
`Manager` is kept away from the details, instead is created with an optional
callback that is called to pass a signal to "higher level code", whatever that
happens to be.
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

    This function will create an instance of the appropriate `Worker` subclass
    and call its run() method.

    If this function catches an exception, it will be logged and then forwarded
    to the `Manager` through the queue via the "error" signal.

    Unless something goes spectacularly wrong, this function will always send
    a "terminate" signal to the `Manager`, even if the worker crashes or a
    `Worker` subclass named *worker* isn't registered.

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
    """
    Just a workin' class class.

    To create a worker, just subclass like this and override the
    `Worker.execute()` method:

    >>> class MyWorker(Worker):
    ...     def execute(self, junk):
    ...         self.emit('my_signal', 'doing stuff with junk')
    ...

    You must be explicitly registered your worker with `register()` for
    `dispatch()` to be able to create instances of your worker in a sub process.
    It's best to first check if your worker is registered, like this:

    >>> if not isregistered(MyWorker):
    ...     register(MyWorker)
    ...

    Ideally, the corresponding `Manager` subclass should insure all its workers
    are registered, typically in its __init__() method, like this:

    >>> class MyManager(Manager):
    ...     def __init__(self, env, callback=None):
    ...         super(MyManager, self).__init__(env, callback)
    ...         for klass in [MyWorker]:
    ...             if not isregistered(klass):
    ...                 register(klass)
    ...
    >>> manager = MyManager({})

    :param env: a ``dict`` containing run-time information like the CouchDB URL
    :param q: a ``multiprocessing.Queue`` or similar
    :param key: a key to uniquely identify this worker among active workers
        controlled by the `Manager` that launched this worker
    :param args: arguments to be passed to `Worker.run()`
    """
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
        """
        Called before starting the first worker.

        This method can be overridden by subclasses.  It is called when the
        manager goes from a state of having no active workers to a state of
        having at least one active worker, just prior to starting the new
        worker.

        For example, `ImportManager` overrides this method to fire the
        "BatchStarted" signal.

        Also see `Manager.last_worker_finished()`.
        """

    def last_worker_finished(self):
        """
        Called after last worker has finished.

        This method can be overridden by subclasses.  It is called when the
        manager goes from a state of having at least one active worker to a
        state of having no active workers, just after the worker's "terminate"
        signal has been handled and the worker removed from the ``_workers``
        dictionary.

        For example, `ImportManager` overrides this method to fire the
        "BatchFinished" signal.

        Also see `Manager.first_worker_starting()`.
        """

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

        If the job identified by *key* is already running, ``False`` is
        returned, without any further action.

        Otherwise the job is dispatched to a new worker process, and ``True`` is
        returned.

        Note that this method is asynchronous and will return immediately.

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
