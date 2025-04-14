# This software was developed by employees of the National Institute of
# Standards and Technology (NIST), an agency of the Federal Government.
# Pursuant to title 17 United States Code Section 105, works of NIST employees
# are not subject to copyright protection in the United States and are
# considered to be in the public domain. Permission to freely use, copy,
# modify, and distribute this software and its documentation without fee is
# hereby granted, provided that this notice and disclaimer of warranty appears
# in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' WITHOUT ANY WARRANTY OF ANY KIND, EITHER
# EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY
# THAT THE SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND FREEDOM FROM
# INFRINGEMENT, AND ANY WARRANTY THAT THE DOCUMENTATION WILL CONFORM TO THE
# SOFTWARE, OR ANY WARRANTY THAT THE SOFTWARE WILL BE ERROR FREE. IN NO EVENT
# SHALL NIST BE LIABLE FOR ANY DAMAGES, INCLUDING, BUT NOT LIMITED TO, DIRECT,
# INDIRECT, SPECIAL OR CONSEQUENTIAL DAMAGES, ARISING OUT OF, RESULTING FROM,
# OR IN ANY WAY CONNECTED WITH THIS SOFTWARE, WHETHER OR NOT BASED UPON
# WARRANTY, CONTRACT, TORT, OR OTHERWISE, WHETHER OR NOT INJURY WAS SUSTAINED
# BY PERSONS OR PROPERTY OR OTHERWISE, AND WHETHER OR NOT LOSS WAS SUSTAINED
# FROM, OR AROSE OUT OF THE RESULTS OF, OR USE OF, THE SOFTWARE OR SERVICES
# PROVIDED HEREUNDER. Distributions of NIST software should also include
# copyright and licensing statements of any third-party software that are
# legally bundled with the code in compliance with the conditions of those
# licenses.

from collections import OrderedDict
from contextlib import contextmanager
import inspect
import pandas as pd
import os
from queue import Queue, Empty
from sortedcontainers import SortedDict
import sys
from threading import Thread, ThreadError, Event
from functools import wraps
import psutil

import time
import traceback
from textwrap import dedent

import traitlets
import logging
import copy
from traitlets import All, Undefined, TraitType

__all__ = ['concurrently', 'sequentially', 'Call', 'ConcurrentException',
           'ConfigStore', 'ConcurrentRunner', 'FilenameDict', 'hash_caller',
           'kill_by_name', 'check_master',
           'retry', 'show_messages', 'sleep', 'stopwatch', 'Testbed', 'ThreadSandbox',
           'ThreadEndedByMaster', 'until_timeout','ConnectionError', 'DeviceNotReady', 'DeviceFatalError', 'DeviceException',
           'DeviceConnectionLost', 'Undefined', 'All', 'DeviceStateError',
           'Int', 'Float', 'Unicode', 'Complex', 'Bytes', 'CaselessBytesEnum',
           'Bool', 'List', 'Dict', 'TCPAddress',
           'CaselessStrEnum', 'Device', 'list_devices', 'logger', 'CommandNotImplementedError']

class ConcurrentException(Exception):
    pass


class MasterThreadException(ThreadError):
    pass


class ThreadEndedByMaster(ThreadError):
    pass


stop_request_event = Event()


def sleep(seconds, tick=1.):
    ''' Drop-in replacement for time.sleep that raises ConcurrentException
        if another thread requests that all threads stop.
    '''
    t0 = time.time()
    global stop_request_event
    remaining = 0

    while True:
        # Raise ConcurrentException if the stop_request_event is set
        if stop_request_event.wait(min(remaining, tick)):
            raise ThreadEndedByMaster

        remaining = seconds - (time.time() - t0)

        # Return normally if the sleep finishes as requested
        if remaining <= 0:
            return


def check_master():
    ''' Raise ThreadEndedByMaster if the master thread as requested this
        thread to end.
    '''
    sleep(0.)


def retry(exception_or_exceptions, tries=4, delay=0,
          backoff=0, exception_func=lambda: None):
    """ This decorator causes the function call to repeat, suppressing specified exception(s), until a
    maximum number of retries has been attempted.
    - If the function raises the exception the specified number of times, the underlying exception is raised.
    - Otherwise, return the result of the function call.

    :example:
    The following retries the telnet connection 5 times on ConnectionRefusedError::

        import telnetlib

        # Retry a telnet connection 5 times if the telnet library raises ConnectionRefusedError
        @retry(ConnectionRefusedError, tries=5)
        def connect(host, port):
            t = telnetlib.Telnet()
            t.open(host,port,5)
            return t


    Inspired by https://github.com/saltycrane/retry-decorator which is released
    under the BSD license.

    :param exception_or_exceptions: Exception (sub)class (or tuple of exception classes) to watch for
    :param tries: number of times to try before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: float
    :param backoff: backoff to multiply to the delay for each retry
    :type backoff: float
    :param exception_func: function to call on exception before the next retry
    :type exception_func: callable
    """

    def decorator(f):
        @wraps(f)
        def do_retry(*args, **kwargs):
            active_delay = delay
            for retry in range(tries):
                try:
                    ret = f(*args, **kwargs)
                except exception_or_exceptions as e:
                    ex = e
                    logger.warning(str(e))
                    logger.warning(
                        f'{f.__name__} retry (call attempt {retry + 1}/{tries})')
                    exception_func()
                    sleep(active_delay)
                    active_delay = active_delay * backoff
                else:
                    break
            else:
                raise ex

            return ret

        return do_retry

    return decorator


def until_timeout(exception_or_exceptions, timeout, delay=0,
                  backoff=0, exception_func=lambda: None):
    """ This decorator causes the function call to repeat, suppressing specified exception(s), until the
    specified timeout period has expired.
    - If the timeout expires, the underlying exception is raised.
    - Otherwise, return the result of the function call.

    Inspired by https://github.com/saltycrane/retry-decorator which is released
    under the BSD license.


    :example:
    The following retries the telnet connection for 5 seconds on ConnectionRefusedError::

        import telnetlib

        @until_timeout(ConnectionRefusedError, 5)
        def connect(host, port):
            t = telnetlib.Telnet()
            t.open(host,port,5)
            return t

    :param exception_or_exceptions: Exception (sub)class (or tuple of exception classes) to watch for
    :param timeout: time in seconds to continue calling the decorated function while suppressing exception_or_exceptions
    :type timeout: float
    :param delay: initial delay between retries in seconds
    :type delay: float
    :param backoff: backoff to multiply to the delay for each retry
    :type backoff: float
    :param exception_func: function to call on exception before the next retry
    :type exception_func: callable
    """

    def decorator(f):
        @wraps(f)
        def do_retry(*args, **kwargs):
            active_delay = delay
            t0 = time.time()
            while time.time() - t0 < timeout:
                progress = time.time() - t0
                try:
                    ret = f(*args, **kwargs)
                except exception_or_exceptions as e:
                    ex = e
                    logger.warning(str(e))
                    logger.warning(
                        f'{f.__name__} retry ({progress}s/{timeout}s elapsed)')
                    exception_func()
                    sleep(active_delay)
                    active_delay = active_delay * backoff
                else:
                    break
            else:
                raise ex

            return ret

        return do_retry

    return decorator


@contextmanager
def limit_exception_traceback(limit):
    ''' Limit the tracebacks printed for uncaught
        exceptions to the specified depth. Works for
        regular CPython and IPython interpreters.
    '''

    def limit_hook(type, value, tb):
        traceback.print_exception(type, value, tb, limit=limit)

    if 'ipykernel' in repr(sys.excepthook):
        from ipykernel import kernelapp
        import IPython

        app = kernelapp.IPKernelApp.instance()
        ipyhook = app.shell.excepthook

        is_ipy = (sys.excepthook == ipyhook)
    else:
        is_ipy = False

    if is_ipy:
        def showtb(self, *args, **kws):
            limit_hook(*sys.exc_info())

        oldhook = ipyhook
        IPython.core.interactiveshell.InteractiveShell.showtraceback = showtb
    else:
        oldhook, sys.excepthook = sys.excepthook, limit_hook

    yield

    if is_ipy:
        app.showtraceback = oldhook
    else:
        sys.excepthook = oldhook


def show_messages(minimum_level):
    ''' Configure screen debug message output for any messages as least as important as indicated by `level`.

    :param minimum_level: One of 'debug', 'warning', 'error', or None. If None, there will be no output.
    :return: None
    '''

    import logging
    import coloredlogs

    err_map = {'debug': logging.DEBUG,
               'warning': logging.WARNING,
               'error': logging.ERROR,
               'info': logging.INFO,
               None: None}

    if minimum_level.lower() not in err_map:
        raise ValueError(
            f'message level must be one of {list(err_map.keys())}')
    level = err_map[minimum_level.lower()]

    logger.setLevel(logging.DEBUG)

    # Clear out any stale handlers
    if hasattr(logger, '_screen_handler'):
        logger.removeHandler(logger._screen_handler)

    if level is not None:
        logger._screen_handler = logging.StreamHandler()
        logger._screen_handler.setLevel(level)
        # - %(pathname)s:%(lineno)d'
        log_fmt = '%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s'
        #    coloredlogs.install(level='DEBUG', logger=logger)
        logger._screen_handler.setFormatter(
            coloredlogs.ColoredFormatter(log_fmt))
        logger.addHandler(logger._screen_handler)


def kill_by_name(*names):
    ''' Kill one or more running processes by the name(s) of matching binaries.

        :param names: list of names of processes to kill
        :type names: str

        :example:
        >>> # Kill any binaries called 'notepad.exe' or 'notepad2.exe'
        >>> kill_by_name('notepad.exe', 'notepad2.exe')

        :Notes:
        Looks for a case-insensitive match against the Process.name() in the
        psutil library. Though psutil is cross-platform, the naming convention
        returned by name() is platform-dependent. In windows, for example, name()
        usually ends in '.exe'.
    '''
    for pid in pspids():
        try:
            proc = psProcess(pid)
            for target in names:
                if proc.name().lower() == target.lower():
                    logger.info(f'killing process {proc.name()}')
                    proc.kill()
        except psNoSuchProcess:
            continue


def hash_caller(call_depth=1):
    ''' Use introspection to return an SHA224 hex digest of the caller, which
        is almost certainly unique to the combination of the caller source code
        and the arguments passed it.
    '''
    import inspect
    import hashlib
    import pickle

    thisframe = inspect.currentframe()
    frame = inspect.getouterframes(thisframe)[call_depth]
    arginfo = inspect.getargvalues(frame.frame)

    # get the function object for a simple function
    if frame.function in frame.frame.f_globals:
        func = frame.frame.f_globals[frame.function]
        argnames = arginfo.args

    # get the function object for a method in a class
    elif len(arginfo.args) > 0:  # arginfo.args[0] == 'self':
        name = arginfo.args[0]
        if name not in frame.frame.f_locals:
            raise ValueError('failed to find function object by introspection')
        func = getattr(frame.frame.f_locals[name], frame.function)
        argnames = arginfo.args[1:]

    # there weren't any arguments
    else:
        argnames = []

    args = [arginfo.locals[k] for k in argnames]

    s = inspect.getsource(func) + str(pickle.dumps(args))
    return hashlib.sha224(s.encode('ascii')).hexdigest()


class Call(object):
    ''' Wrap a function to apply arguments for threaded calls to `concurrently`.
        This can be passed in directly by a user in order to provide arguments;
        otherwise, it will automatically be wrapped inside `concurrently` to
        keep track of some call metadata during execution.
    '''

    def __init__(self, func, *args, **kws):
        if not callable(func):
            raise ValueError(
                '`func` argument is not callable; did you mistakenly add the () and call it?')
        self.func = func
        self.name = self.func.__name__
        self.args = args
        self.kws = kws
        self.queue = None

        # This is a means for the main thread to raise an exception
        # if this is running in a separate thread

    def __call__(self):
        try:
            self.result = self.func(*self.args, **self.kws)
        except BaseException:
            self.result = None
            self.traceback = sys.exc_info()
        else:
            self.traceback = None

        self.queue.put(self)

    def set_queue(self, queue):
        self.queue = queue

    @staticmethod
    def setup(func_in):
        ''' Setup threading (concurrent execution only), including
            checks for whether a Device instance indicates it supports
            concurrent execution or not.
        '''
        func = func_in.func if isinstance(func_in, Call) else func_in
        if hasattr(func, '__self__') \
                and isinstance(func.__self__, core.Device):
            if not func.__self__.settings.concurrency_support:
                raise ConcurrentException(
                    f'{func.__self__} does not support concurrency')
            elif hasattr(func.__self__, '__pre_thread__'):
                func.__self__.__pre_thread__()
        return func_in

    @staticmethod
    def cleanup(func_in):
        ''' Cleanup threading (concurrent execution only)
        '''
        # Implement the below at some stage in the future?
        func = func_in.func if isinstance(func_in, Call) else func_in
        if inspect.ismethod(func) \
                and hasattr(func.__self__, '__post_thread__'):
            func.__self__.__post_thread__()
        return func_in


@contextmanager
def stopwatch(desc=''):
    ''' Time a block of code using a with statement like this:

    >>> with stopwatch('sleep statement'):
    >>>     time.sleep(2)
    sleep statement time elapsed 1.999s.

    :param desc: text for display that describes the event being timed
    :type desc: str
    :return: context manager
    '''
    from platform import platform
    import time

    if platform().lower().startswith('windows'):
        timefunc = time.perf_counter
    else:
        timefunc = time.time

    t0 = timefunc()

    try:
        yield
    finally:
        T = timefunc() - t0
        logger.info(f'{desc} time elapsed {T:0.3f}s'.lstrip())


def concurrently_call(*funcs, **kws):
    def traceback_skip(exc_tuple, count):
        ''' Skip the first `count` traceback entries in
            an exception.
        '''
        tb = exc_tuple[2]
        for i in range(count):
            if tb.tb_next is not None:
                tb = tb.tb_next
        return exc_tuple[:2] + (tb,)

    stop_request_event.clear()

    results = {}
    for f in funcs:
        if isinstance(f, dict):
            results.update(f)
            funcs.remove(f)
        elif not callable(f):
            msg = 'only dictionary and callable arguments are allowed, but got ' + \
                  repr(f)
            raise ValueError(msg)

    catch = kws.get('catch', False)
    if callable(catch):
        kws['catch'] = catch
        catch = False

    flatten = kws.get('flatten', True)
    if callable(flatten):
        kws['flatten'] = flatten
        flatten = True

    nones = kws.get('nones', False)
    if callable(nones):
        kws['nones'] = nones
        nones = False

    traceback_delay = kws.get('traceback_delay', True)
    if callable(nones):
        kws['traceback_delay'] = traceback_delay
        traceback_delay = False

    calls = [Call.setup(f) if isinstance(f, Call) else Call(f) for f in funcs]

    # Force unique names
    names = [c.name for c in calls]
    for i, c in enumerate(calls):
        count = names[:i].count(c.name)
        if count > 0:
            c0 = calls[names[:i].index(c.name)]
            if not c0.name.endswith('_0'):
                c0.name += '_0'
            c.name += '_' + str(count)
    del names

    # Set up mappings between wrappers, threads, and the function to call
    # OrderedDict([(func,Call(func, *args.get(func,[]))) for func in funcs])
    wrappers = OrderedDict(list(zip([c.name for c in calls], calls)))
    threads = OrderedDict([(name, Thread(target=w))
                           for name, w in list(wrappers.items())])

    # Start threads with calls to each function
    finished = Queue()
    for name, thread in list(threads.items()):
        wrappers[name].set_queue(finished)
        thread.start()

    # As each thread ends, collect the return value and any exceptions
    #    exception_count = 0
    tracebacks = []
    master_exception = None

    t0 = time.perf_counter()

    while len(threads) > 0:
        try:
            called = finished.get(timeout=0.25)
        except Empty:
            if time.perf_counter() - t0 > 60 * 15:
                names = ','.join(list(threads.keys()))
                logger.debug(f'{names} threads are still running')
                t0 = time.perf_counter()
            continue
        except BaseException as e:
            master_exception = e
            stop_request_event.set()
            called = None

        if called is None:
            continue

        # Below only happens when called is not none
        if master_exception is not None:
            names = ', '.join(list(threads.keys()))
            logger.error(
                f'raising {master_exception.__class__} in main thread after child threads {names} return')

        # if there was an exception that wasn't us ending the thread,
        # show messages
        if called.traceback is not None:
            tb = traceback_skip(called.traceback, 1)

            if called.traceback[0] is not ThreadEndedByMaster:
                #                exception_count += 1
                tracebacks.append(tb)
                last_exception = called.traceback[1]

            if not traceback_delay:
                try:
                    traceback.print_exception(*tb)
                except BaseException:
                    sys.stderr.write('\nthread error (fixme to print message)')
                    sys.stderr.write('\n')
        else:
            if flatten and isinstance(called.result, dict):
                results.update(called.result)
            elif nones or called.result is not None:
                results[called.name] = called.result

        # Remove this thread from the dictionary of running threads
        del threads[called.name]

    # Raise exceptions as necessary
    if master_exception is not None:
        for h in logger.handlers:
            h.flush()

        for tb in tracebacks:
            try:
                traceback.print_exception(*tb)
            except BaseException:
                sys.stderr.write('\nthread error (fixme to print message)')
                sys.stderr.write('\n')

        raise master_exception

    elif len(tracebacks) > 0 and not catch:
        for h in logger.handlers:
            h.flush()
        if len(tracebacks) == 1:
            raise last_exception
        else:
            for tb in tracebacks:
                try:
                    traceback.print_exception(*tb)
                except BaseException:
                    sys.stderr.write('\nthread error (fixme to print message)')
                    sys.stderr.write('\n')

            with limit_exception_traceback(5):
                raise ConcurrentException(
                    f'{len(tracebacks)} call(s) raised exceptions')

    return results


@contextmanager
def concurrently_enter(*contexts, **kws):
    t0 = time.time()
    exits = []

    def enter(c):
        def ex(*args):
            try:
                Call.cleanup(c.__exit__)
            finally:
                c.__exit__(*args)

        # Exit Device instances last, to give other
        # devices a chance to access them during their
        # __exit__
        if isinstance(c, core.Device):
            exits.insert(0, ex)
        else:
            exits.append(ex)

        ent = c.__enter__
        ret = ent()

        return ret

    try:
        for c in contexts:
            Call.setup(c.__enter__)
        for c in kws.values():
            Call.setup(c.__enter__)
        calls = [Call(enter, c) for c in contexts]
        for name, c in kws.items():
            call = Call(enter, c)
            call.name = name
            calls.append(call)
        ret = concurrently_call(*calls)
        logger.info(f'Connected all in {time.time() - t0:0.2f}s')
        if ret is None:
            yield []
        else:
            yield ret.values()
    except BaseException:
        exc = sys.exc_info()
    else:
        exc = (None, None, None)

    t0 = time.time()
    while exits:
        exit = exits.pop()
        try:
            exit(*exc)
        except BaseException:
            exc = sys.exc_info()

    logger.info(f'Disonnected all in {time.time() - t0:0.2f}s')

    if exc != (None, None, None):
        # sys.exc_info() may have been
        # changed by one of the exit methods
        # so provide explicit exception info
        for h in logger.handlers:
            h.flush()
        raise exc[1]


def sequentially(*funcs, **kws):
    r''' Call each function or method listed in `*funcs` sequentially.
         The goal is to emulate the behavior of the `concurrently` function,
         with some of the same support for updating result dictionaries.

        Multiple references to the same function in `funcs` only result in one
        call. The `catch` and `flatten` arguments may be callables, in which
        case they are executed (and their values are treated as defaults).

        :param objs:  each argument may be a callable (function or class that
        defines a __call__ method), or context manager (such as a Device instance)
        :param catch:  if `False` (the default), a `ConcurrentException` is
        raised if any of `funcs` raise an exception; otherwise, any remaining
        successful calls are returned as normal :param flatten:  if not callable
        and evalues as True, updates the returned dictionary with the
        dictionary (instead of a nested dictionary) :param nones: if not
        callable and evalues as True, includes entries for calls that return
        None (default is False) :return: the values returned by each function
        :rtype: dictionary of keyed by function.

        Here are some examples:

        :Example: Call each function `myfunc1` and `myfunc2`, each with no arguments:

        >>> import labbench as lb
        >>> def do_something_1 ():
        >>>     time.sleep(0.5)
        >>>     return 1
        >>> def do_something_2 ():
        >>>     time.sleep(1)
        >>>     return 2
        >>> rets = lb.sequentially(myfunc1, myfunc2)
        >>> rets[do_something_1]
        1

        :Example: To pass arguments, use the Call wrapper

        >>> def do_something_3 (a,b,c):
        >>>     time.sleep(2)
        >>>     return a,b,c
        >>> rets = lb.sequentially(myfunc1, Call(myfunc3,a,b,c=c))
        >>> rets[do_something_3]
        a, b, c

        Because :func sequentially: does not use threading, it does not check
        whether a Device method supports concurrency before it runs.
    '''

    funcs = list(funcs)
    results = {}
    for f in funcs:
        if isinstance(f, dict):
            results.update(f)
            funcs.remove(f)
        elif not callable(f):
            msg = 'only dictionary and callable arguments are allowed, but got ' + \
                  repr(f)
            raise ValueError(msg)

    if len(set(funcs)) != len(funcs):
        raise Exception(
            'input arguments include duplicates, but each must be unique')

    catch = kws.get('catch', False)
    if callable(catch):
        kws['catch'] = catch
        catch = False

    flatten = kws.get('flatten', True)
    if callable(flatten):
        kws['flatten'] = flatten
        flatten = True

    nones = kws.get('nones', False)
    if callable(nones):
        kws['nones'] = nones
        nones = False

    traceback_delay = kws.get('traceback_delay', True)
    if callable(nones):
        kws['traceback_delay'] = traceback_delay
        traceback_delay = False

    calls = [f if isinstance(f, Call) else Call(f) for f in funcs]

    # Force unique names
    names = [c.name for c in calls]
    for i, c in enumerate(calls):
        count = names[:i].count(c.name)
        if count > 0:
            c0 = calls[names[:i].index(c.name)]
            if not c0.name.endswith('_0'):
                c0.name += '_0'
            c.name += '_' + str(count)
    del names

    # Set up mappings between wrappers, threads, and the function to call
    # OrderedDict([(func,Call(func, *args.get(func,[]))) for func in funcs])
    wrappers = OrderedDict(list(zip([c.name for c in calls], calls)))
    threads = OrderedDict([(name, w)
                           for name, w in list(wrappers.items())])

    # Call one at a time with calls to each function
    finished = Queue()
    for name, thread in list(threads.items()):
        wrappers[name].set_queue(finished)
        thread()

    # As each thread ends, collect the return value and any exceptions
    tracebacks = []
    for i in range(len(threads)):
        called = finished.get()

        # if there was an exception
        if called.traceback is not None:
            if not traceback_delay:
                sys.stderr.write('\n')
                traceback.print_exception(*called.traceback)
                sys.stderr.write('\n')
            tracebacks.append(called.traceback)

        else:
            if flatten and isinstance(called.result, dict):
                results.update(called.result)
            elif nones or called.result is not None:
                results[called.name] = called.result

    # Raise exceptions as necessary
    if len(tracebacks) > 0 and not catch:
        raise ConcurrentException(
            f'{len(tracebacks)} call(s) raised exceptions')

    if traceback_delay:
        for tb in tracebacks:
            sys.stderr.write('\n')
            traceback.print_exception(*tb)
            sys.stderr.write('\n')

    if results is None:
        return {}
    else:
        return results


def concurrently(*objs, **kws):
    r''' If `*objs` are callable (like functions), call each of
         `*objs` in concurrent threads. If `*objs` are context
         managers (such as Device instances to be connected),
         enter each context in concurrent threads.

        Multiple references to the same function in `funcs` only result in one call. The `catch` and `flatten`
        arguments may be callables, in which case they are executed (and each flag value is treated as defaults).

        :param objs:  each argument may be a callable (function or class that defines a __call__ method), or context manager (such as a Device instance)
        :param catch:  if `False` (the default), a `ConcurrentException` is raised if any of `funcs` raise an exception; otherwise, any remaining successful calls are returned as normal
        :param flatten:  if not callable and evalues as True, updates the returned dictionary with the dictionary (instead of a nested dictionary)
        :param nones: if not callable and evalues as True, includes entries for calls that return None (default is False)
        :param traceback_delay: if `False`, immediately show traceback information on a thread exception; if `True` (the default), wait until all threads finish
        :return: the values returned by each function
        :rtype: dictionary of keyed by function

        Here are some examples:

        :Example: Call each function `myfunc1` and `myfunc2`, each with no arguments:

        >>> def do_something_1 ():
        >>>     time.sleep(0.5)
        >>>     return 1
        >>> def do_something_2 ():
        >>>     time.sleep(1)
        >>>     return 2
        >>> rets = concurrent(myfunc1, myfunc2)
        >>> rets[do_something_1]
        1

        :Example: To pass arguments, use the Call wrapper

        >>> def do_something_3 (a,b,c):
        >>>     time.sleep(2)
        >>>     return a,b,c
        >>> rets = concurrent(myfunc1, Call(myfunc3,a,b,c=c))
        >>> rets[do_something_3]
        a, b, c

        **Caveats**

        - Because the calls are in different threads, not different processes,
          this should be used for IO-bound functions (not CPU-intensive functions).
        - Be careful about thread safety.

        When the callable object is a Device method, :func concurrency: checks
        the Device object state.concurrency_support for compatibility
        before execution. If this check returns `False`, this method
        raises a ConcurrentException.

    '''
    objs = list(objs)

    for f in objs:
        if isinstance(f, dict):
            objs.remove(f)
        if not isinstance(f, dict) and not callable(
                f) and not hasattr(f, '__enter__'):
            msg = 'only dict, callable or context manager arguments are allowed, but got {}' + \
                  repr(f)
            raise ValueError(msg)

    for k, f in kws.items():
        if k in ('catch', 'flatten', 'nones', 'traceback_delay'):
            continue
        if isinstance(f, dict):
            del kws[k]
        if not isinstance(f, dict) and not callable(
                f) and not hasattr(f, '__enter__'):
            msg = 'only dict, callable or context manager arguments are allowed, but got {}' + \
                  repr(f)
            raise ValueError(msg)

    if len(set(objs)) != len(objs):
        raise Exception(
            'input arguments include duplicates, but each must be unique')

    # If funcs are context managers, concurrently enter
    # their contexts instead of calling them
    for f in objs + list(kws.values()):
        if not hasattr(f, '__enter__'):
            return concurrently_call(*objs, **kws)
    else:
        return concurrently_enter(*objs, **kws)


OP_CALL = 'op'
OP_GET = 'get'
OP_SET = 'set'
OP_QUIT = None


class ThreadDelegate(object):
    _sandbox = None
    _obj = None
    _dir = None
    _repr = None

    def __init__(self, sandbox, obj, dir_, repr_):
        self._sandbox = sandbox
        self._obj = obj
        self._dir = dir_
        self._repr = repr_

    def __call__(self, *args, **kws):
        return message(self._sandbox, OP_CALL, self._obj, None, args, kws)

    def __getattribute__(self, name):
        if name in delegate_keys:
            return object.__getattribute__(self, name)
        else:
            return message(self._sandbox, OP_GET, self._obj, name, None, None)

    def __dir__(self):
        return self._dir

    def __repr__(self):
        return f'ThreadDelegate({self._repr})'

    def __setattr__(self, name, value):
        if name in delegate_keys:
            return object.__setattr__(self, name, value)
        else:
            return message(self._sandbox, OP_SET, self._obj, name, value, None)


delegate_keys = set(ThreadDelegate.__dict__.keys()
                    ).difference(object.__dict__.keys())


def message(sandbox, *msg):
    req, rsp = sandbox._requestq, Queue(1)

    # Await and handle request. Exception should be raised in this
    # (main) thread
    req.put(msg + (rsp,), True)
    ret, exc = rsp.get(True)
    if exc is not None:
        raise exc

    return ret


class ThreadSandbox(object):
    __repr_root__ = 'uninitialized ThreadSandbox'
    __dir_root__ = []
    __thread = None
    _requestq = None

    def __init__(self, factory, should_sandbox_func=None):
        # Start the thread and block until it's ready
        self._requestq = Queue(1)
        ready = Queue(1)
        self.__thread = Thread(target=self.__worker, args=(
            factory, ready, should_sandbox_func))
        self.__thread.start()
        exc = ready.get(True)
        if exc is not None:
            raise exc

    def __worker(self, factory, ready, sandbox_check_func):
        ''' This is the only thread allowed to access the protected object.
        '''

        try:
            root = factory()

            def default_sandbox_check_func(obj):
                try:
                    return inspect.getmodule(obj).__name__.startswith(
                        inspect.getmodule(root).__name__)
                except AttributeError:
                    return False

            if sandbox_check_func is None:
                sandbox_check_func = default_sandbox_check_func

            self.__repr_root__ = repr(root)
            self.__dir_root__ = sorted(
                list(set(dir(root) + list(sandbox_keys))))
            exc = None
        except Exception as e:
            exc = e
        finally:
            ready.put(exc, True)
        if exc:
            return

        # Do some sort of setup here
        while True:
            ret = None
            exc = None

            op, obj, name, args, kws, rsp = self._requestq.get(True)

            # End if that's good
            if op is OP_QUIT:
                break
            if obj is None:
                obj = root

            # Do the op
            try:
                if op is OP_GET:
                    ret = getattr(obj, name)
                elif op is OP_CALL:
                    ret = obj(*args, **kws)
                elif op is OP_SET:
                    ret = setattr(obj, name, args)

                # Make it a delegate if it needs to be protected
                if sandbox_check_func(ret):
                    ret = ThreadDelegate(self, ret,
                                         dir_=dir(ret),
                                         repr_=repr(ret))

            # Catch all exceptions
            except Exception as e:
                exc = e
                exc = e

            rsp.put((ret, exc), True)

        logger.write('ThreadSandbox worker thread finished')

    def __getattr__(self, name):
        if name in sandbox_keys:
            return object.__getattribute__(self, name)
        else:
            return message(self, OP_GET, None, name, None, None)

    def __setattr__(self, name, value):
        if name in sandbox_keys:
            return object.__setattr__(self, name, value)
        else:
            return message(self, OP_SET, None, name, value, None)

    def _stop(self):
        message(self, OP_QUIT, None, None, None, None, None)

    def _kill(self):
        if isinstance(self.__thread, Thread):
            self.__thread.join(0)
        else:
            raise Exception("no thread running to kill")

    def __del__(self):
        try:
            del_ = message(self, OP_GET, None, '__del__', None, None)
        except AttributeError:
            pass
        else:
            del_()
        finally:
            try:
                self._kill()
            except BaseException:
                pass

    def __repr__(self):
        return f'ThreadSandbox({self.__repr_root__})'

    def __dir__(self):
        return self.__dir_root__


sandbox_keys = set(ThreadSandbox.__dict__.keys()
                   ).difference(object.__dict__.keys())


class ConfigStore:
    ''' Define dictionaries of configuration settings
        in subclasses of this object. Each dictionary should
        be an attribute of the subclass. The all() class method
        returns a flattened dictionary consisting of all values
        of these dictionary attributes, keyed according to
        '{attr_name}_{attr_key}', where {attr_name} is the
        name of the dictionary attribute and {attr_key} is the
        nested dictionary key.
    '''

    @classmethod
    def all(cls):
        ret = {}
        for k, v in cls.__dict__.items():
            if isinstance(v, dict) and not k.startswith('_'):
                ret.update([(k + '_' + k2, v2) for k2, v2 in v.items()])
        return ret

    @classmethod
    def frame(cls):
        df = pd.DataFrame([cls.all()]).T
        df.columns.name = 'Value'
        df.index.name = 'Parameter'
        return df


class FilenameDict(SortedDict):
    ''' Sometimes instrument configuration file can be defined according
        to a combination of several test parameters.

        This class provides a way of mapping these parameters to and from a
        filename string.

        They keys are sorted alphabetically, just as in the underlying
        SortedDict.
    '''

    def __init__(self, *args, **kws):
        if len(args) == 1 and isinstance(args[0], str):
            d = self.from_filename(args[0])
            super(FilenameDict, self).__init__()
            self.update(d)
        elif len(args) >= 1 and isinstance(args[0], (pd.Series, pd.DataFrame)):
            d = self.from_index(*args, **kws)
            super(FilenameDict, self).__init__()
            self.update(d)
        else:
            super(FilenameDict, self).__init__(*args, **kws)

    def __str__(self):
        ''' Convert the dictionary to a filename. It is not guaranteed
            to fit within file name length limit of any filesystem.
        '''
        return ','.join([f'{k}={v}' for k, v in self.items()])

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(str(self))})'

    @classmethod
    def from_filename(cls, filename):
        ''' Convert from a FilenameDict filename string to a FilenameDict
            object.
        '''
        filename = os.path.splitext(os.path.basename(filename))[0]
        fields = filename.split(',')
        fields = [f.split('=') for f in fields]
        return cls(fields)

    @classmethod
    def from_index(cls, df, value=None):
        ''' Make a FilenameDict where the keys are taken from df.index
            and the values are constant values provided.
        '''
        keys = df.index.tolist()
        values = len(keys) * [value]
        return cls(zip(keys, values))


class ConcurrentRunner:
    ''' Concurrently runs all staticmethods or classmethods
        defined in the subclass.
    '''

    def __new__(cls):
        def log_wrapper(func, func_name):
            def wrap():
                ret = func()
                logger.info(
                    f'concurrent run {cls.__name__}.{func_name} done')
                return ret

            return wrap

        methods = {}

        # Only attributes that are not in this base class
        attrs = set(dir(cls)).difference(dir(ConcurrentRunner))

        for k in sorted(attrs):
            if not k.startswith('_'):
                v = getattr(cls, k)
                clsmeth = inspect.ismethod(v) \
                          and v.__self__ is cls
                staticmeth = callable(v) and not hasattr(v, '__self__')
                if clsmeth or staticmeth:
                    methods[k] = log_wrapper(v, k)
                elif callable(v):
                    logger.info(f'skipping {cls.__name__}.{k}')

        logger.info(
            f"concurrent run {cls.__name__} {list(methods.keys())}")
        return concurrently(*methods.values(), flatten=True)


class Testbed(object):
    ''' Base class for testbeds that contain multiple Device instances
        or other objects like database managers that implement context
        management.

        Use a `with` block with the testbed instance to connect everything
        at once like so::

            with Testbed() as testbed:
                # use the testbed here
                pass

        or optionally connect only a subset of devices like this::

            testbed = Testbed()
            with testbed.dev1, testbed.dev2:
                # use the testbed.dev1 and testbed.dev2 here
                pass

        Make your own subclass of Testbed with a custom `make`
        method to define the Device or database manager instances, and
        a custom `startup` method to implement custom code to set up the
        testbed after each Device is connected.
    '''

    def __init__(self, config=None, concurrent=True):
        self.config = config
        attrs_start = dir(self)
        self.make()

        # Find the objects
        new_attrs = set(dir(self)).difference(attrs_start)
        self._contexts = {}
        for a in new_attrs:
            o = getattr(self, a)
            if hasattr(o, '__enter__'):
                self._contexts[a] = o

        if concurrent:
            self.__cm = concurrently(**self._contexts)
        else:
            self.__cm = sequentially(**self._contexts)

    def __enter__(self):
        self.__cm.__enter__()
        self.startup()
        return self

    def __exit__(self, *args):
        try:
            self.cleanup()
        except BaseException as e:
            ex = e
        else:
            ex = None
        finally:
            ret = self.__cm.__exit__(*args)
            if ex is not None:
                raise ex
            return ret

    def make(self):
        ''' Implement this method in a subclass of Testbed. It should
            the drivers as attributes of the Testbed instance, for example::

                self.dev1 = MyDevice()

            This is called automatically when when the testbed class
            is instantiated.
        '''
        pass

    def startup(self):
        ''' This is called automatically after connect if the testbed is
            connected using the `with` statement block.

            Implement any custom code here in Testbed subclasses to
            implement startup of the testbed given connected Device
            instances.
        '''
        pass

    def cleanup(self):
        ''' This is called automatically immediately before disconnect if the
            testbed is connected using the `with` statement block.

            Implement any custom code here in Testbed subclasses to
            implement startup of the testbed given connected Device
            instances.
        '''
        pass


if __name__ == '__main__':
    def do_something_1():
        print('start 1')
        sleep(1)
        print('end 1')
        return 1


    def do_something_2():
        print('start 2')
        sleep(2)
        print('end 2')
        return 2


    def do_something_3(a, b, c):
        print('start 2')
        sleep(2.5)
        print('end 2')
        return a, b, c


    def do_something_4():
        print('start 1')
        sleep(3)
        raise ValueError('I had an error')
        print('end 1')
        return 1


    results = concurrently(do_something_1, do_something_2, do_something_3)

    print('results were', results)
# This software was developed by employees of the National Institute of
# Standards and Technology (NIST), an agency of the Federal Government.
# Pursuant to title 17 United States Code Section 105, works of NIST employees
# are not subject to copyright protection in the United States and are
# considered to be in the public domain. Permission to freely use, copy,
# modify, and distribute this software and its documentation without fee is
# hereby granted, provided that this notice and disclaimer of warranty appears
# in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' WITHOUT ANY WARRANTY OF ANY KIND, EITHER
# EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY
# THAT THE SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND FREEDOM FROM
# INFRINGEMENT, AND ANY WARRANTY THAT THE DOCUMENTATION WILL CONFORM TO THE
# SOFTWARE, OR ANY WARRANTY THAT THE SOFTWARE WILL BE ERROR FREE. IN NO EVENT
# SHALL NIST BE LIABLE FOR ANY DAMAGES, INCLUDING, BUT NOT LIMITED TO, DIRECT,
# INDIRECT, SPECIAL OR CONSEQUENTIAL DAMAGES, ARISING OUT OF, RESULTING FROM,
# OR IN ANY WAY CONNECTED WITH THIS SOFTWARE, WHETHER OR NOT BASED UPON
# WARRANTY, CONTRACT, TORT, OR OTHERWISE, WHETHER OR NOT INJURY WAS SUSTAINED
# BY PERSONS OR PROPERTY OR OTHERWISE, AND WHETHER OR NOT LOSS WAS SUSTAINED
# FROM, OR AROSE OUT OF THE RESULTS OF, OR USE OF, THE SOFTWARE OR SERVICES
# PROVIDED HEREUNDER. Distributions of NIST software should also include
# copyright and licensing statements of any third-party software that are
# legally bundled with the code in compliance with the conditions of those
# licenses.

'''
This implementation is deeply intertwined with low-level internals of
traitlets and obscure details of the python object model. Consider reading
the documentation closely and inheriting these objects instead of
reverse-engineering this code.
'''






logger = logging.getLogger('labbench')


class ConnectionError(traitlets.TraitError):
    pass


class DeviceStateError(traitlets.TraitError):
    pass


class DeviceNotReady(Exception):
    pass


class DeviceException(Exception):
    pass


class DeviceFatalError(Exception):
    pass


class DeviceConnectionLost(Exception):
    pass


class CommandNotImplementedError(NotImplementedError):
    pass


class MetaHasTraits(traitlets.MetaHasTraits):
    """A metaclass for documenting HasTraits."""

    def setup_class(cls, classdict):
        super().setup_class(classdict)

        cls.__rawdoc__ = cls.__doc__

        if cls.__doc__ is None:
            # Find the documentation for HasTraits
            for subcls in cls.__mro__[1:]:
                if hasattr(subcls, '__rawdoc__'):
                    if subcls.__rawdoc__:
                        doc = subcls.__rawdoc__.rstrip()
                        break
                else:
                    doc = ''
                    break
        else:
            doc = cls.__doc__

        cls.__doc__ = '\n\n'.join([str(doc), cls._doc_traits()])


class HasTraits(traitlets.HasTraits, metaclass=MetaHasTraits):
    '''
        Base class for accessing the local and remote state
        parameters of a device.  Normally,
        a device driver implemented as a :class:`Device` subclass
        should add driver-specific states by subclassing this class.

        Instantiating :class:`Device` (or one of its subclasses)
        automatically instantiate this class as well.
    '''

    _device = None
    __attributes__ = dir(traitlets.HasTraits())

    def __init__(self, device, *args, **kws):
        self._device = device
        super(traitlets.HasTraits, self).__init__(*args, **kws)

    def __getter__(self, trait):
        raise CommandNotImplementedError

    def __setter__(self, trait, value):
        raise CommandNotImplementedError

    @classmethod
    def _doc_traits(cls):
        '''
        :return: a dictionary of documentation for this class's traits
        '''
        ret = ['\n\ntrait attributes:']
        traits = cls.class_traits()

        for name in sorted(traits.keys()):
            trait = traits[name]
            ret.append('* `{name}`: `{type}`'
                       .format(help=trait.help,
                               type=trait.__class__.__name__,
                               name=name))

        return '\n\n'.join(ret)


class HasSettingsTraits(HasTraits):
    @classmethod
    def define(cls, **kws):
        ''' Return a copy of this class with default values of the traits
            redefined according to each keyword argument. For example::

                mylocaltraitcls.define(parameter=7)

            will return a deep copy of `mylocaltraitcls` where its trait
            named `parameter` is redefined to have `default_value=7`.
        '''

        # Dynamically define the result to be a new subclass
        newcls = type('settings', (cls,), {})

        traits = cls.class_traits()

        for k, v in kws.items():
            if k not in traits:
                raise traitlets.TraitError('cannot set default value of undefined trait {}'
                                           .format(k))
            trait = copy.copy(traits[k])
            trait.default_value = v
            setattr(newcls, k, trait)

        return newcls

    def __setattr__(self, key, value):
        ''' Prevent silent errors that could result from typos in state names
        '''
        exists = hasattr(self, key)

        # Need to check self.__attributes__ because traitlets.HasTraits.__init__ may not
        # have created attributes it needs yet
        if key not in self.__attributes__ and not exists:
            name = str(self)[1:].split(' ', 1)[0]
            msg = "{} has no '{}' setting defined".format(name, key)
            raise AttributeError(msg)
        if exists and not key.startswith('_') and callable(getattr(self, key)):
            name = str(self)[1:].split(' ', 1)[0]
            msg = "{} attribute '{}' is a method, not a setting. call as a function it instead" \
                .format(name, key)
            raise AttributeError(msg)
        super(HasSettingsTraits, self).__setattr__(key, value)


class HasStateTraits(HasTraits):
    @classmethod
    def setter(cls, func):
        ''' Use this as a decorator to define a setter function for all traits
            in this class. The setter should take two arguments: the instance
            of the trait to get, and the value to set. It should perform any
            operation needed to apply the given value to the trait's state
            in `self._device`. One example is to send a command defined by
            trait.command.

            Any return value from the function is ignored.

            A trait that has its own setter defined will ignore this
            one.

        '''
        cls.__setter__ = func
        return cls

    @classmethod
    def getter(cls, func):
        ''' Use this as a decorator to define a setter function for all traits
            in this class. The getter should take one argument: the instance
            of the trait to get. It should perform any
            operation needed to retrieve the current value of the device state
            corresponding to the supplied trait, using `self._device`.

            One example is to send a command defined by
            trait.command.

            The function should return a value that is the state from the
            device.

            A trait that has its own getter defined will ignore this
            one.
        '''
        cls.__getter__ = func
        return cls

    def __setattr__(self, key, value):
        ''' Prevent silent errors that could result from typos in state names
        '''

        exists = hasattr(self, key)

        # Need to check self.__attributes__ because traitlets.HasTraits.__init__ may not
        # have created attributes it needs yet
        if key not in self.__attributes__ and not exists:
            name = str(self)[1:].split(' ', 1)[0]
            msg = "{} has no '{}' state definition".format(name, key)
            raise AttributeError(msg)
        if exists and not key.startswith('_') and callable(getattr(self, key)):
            name = str(self)[1:].split(' ', 1)[0]
            msg = "{} attribute '{}' is a method, not a state. call as a function it instead" \
                .format(name, key)
            raise AttributeError(msg)
        super(HasStateTraits, self).__setattr__(key, value)


class TraitMixIn(object):
    ''' Includes added mix-in features for device control into traitlets types:
        - If the instance is an attribute of a HasStateTraits class, there are hooks for
          live synchronization with the remote device by sending or receiving data.
          Implement with one of
          * `getter` and/or `setter` keywords in __init__ to pass in a function;
          *  metadata passed in through the `command` keyword, which the parent
             HasStateTraits instance may use in its own `getter` and `setter`
             implementations
        - Adds metadata for autogenerating documentation (see `doc_attrs`)

        Order is important - the class should inherit TraitMixIn first and the
        desired traitlet type second.
    '''

    ''' Classes that use TraitMixIn as a mix-in should do so as follows:

        class LabbenchType(TraitMixIn,traitlets.TraitletType):
            pass
    '''

    doc_attrs = 'command', 'read_only', 'write_only', 'remap', 'cache'
    default_value = traitlets.Undefined
    write_only = False
    cache = False
    read_only_if_connected = False

    def __init__(self, default_value=Undefined, allow_none=False, read_only=None, help=None,
                 write_only=None, cache=None, command=None, getter=None, setter=None,
                 remap={}, **kwargs):

        def make_doc():
            attr_pairs = []
            for k in self.doc_attrs:
                v = getattr(self, k)
                if (k == 'default_value' and v != Undefined) \
                        and v is not None \
                        and not (k == 'allow_none' and v == False) \
                        and not (k == 'remap' and v == {}):
                    attr_pairs.append('{k}={v}'.format(k=k, v=repr(v)))
            params = {'name': type(self).__name__,
                      'attrs': ','.join(attr_pairs),
                      'help': self.help}
            sig = '{name}({attrs})\n\n\t{help}'.format(**params)
            sig = sig.replace('*', r'\*')
            if self.__doc__:
                sig += self.__doc__
            return sig

        # Identify the underlying class from Trait
        for c in inspect.getmro(self.__class__):
            if issubclass(c, TraitType) \
                    and not issubclass(c, TraitMixIn):
                self._parent = c
                break
        else:
            raise Exception(
                'could not identify any parent Trait class')

        # Work around a bug in traitlets.List as of version 4.3.2
        if isinstance(self, traitlets.Container) \
                and default_value is not Undefined:
            self.default_value = default_value

        super(TraitMixIn, self).__init__(default_value=default_value,
                                         allow_none=allow_none,
                                         read_only=read_only,
                                         help=help, **kwargs)

        if read_only == 'connected':
            read_only = False
            self.read_only_if_connected = True
        if write_only is not None:
            self.write_only = write_only
        if cache is not None:
            self.cache = cache
        if not isinstance(remap, dict):
            raise TypeError('remap must be a dictionary')

        self.command = command
        self.__last = None

        self.tag(setter=setter,
                 getter=getter)

        self.remap = remap
        self.remap_inbound = dict([(v, k) for k, v in remap.items()])

        self.info_text = self.__doc__ = make_doc()

    def get(self, obj, cls=None):
        ''' Overload the traitlet's get method. If `obj` is a HasSettingsTraits, call HasTraits.get
            like normal. Otherwise, this is a HasStateTraits, and inject a call to do a `set` from the remote device
        '''
        # If this is Device.settings instead of Device.state, do simple Trait.get,
        # skipping any communicate with the device
        if isinstance(obj, HasSettingsTraits):
            return self._parent.get(self, obj, cls)
        elif not isinstance(obj, HasStateTraits):
            raise TypeError('obj (of type {}) must be an instance of HasSettingsTraits or HasStateTraits'
                            .format(type(obj)))

        # If self.write_only, bail if we haven't cached the value
        if self.write_only and self.__last is None:
            raise traitlets.TraitError(
                'tried to get state value, but it is write-only and has not been set yet')

        # If self.write_only or we are caching, return a cached value
        elif self.write_only or (self.cache and self.__last is not None):
            return self.__last

        # First, look for a getter function or method that implements getting
        # the value from the device
        if callable(self.metadata['getter']):
            new = self.metadata['getter'](obj._device)

        # If there is no getter function, try calling the command_get() method in the Device instance.
        # Otherwise, raise an exception, because we don't know how to get the
        # value from the Device.
        else:  # self.command is not None:
            try:
                new = obj.__class__.__getter__(obj._device, self)
            except CommandNotImplementedError as e:
                raise DeviceStateError(
                    'no command or getter defined - cannot get state from device')

        # Remap the resulting value.
        new = self.remap_inbound.get(new, new)

        # Apply the value with the parent traitlet class.
        self._parent.set(self, obj, new)
        return self._parent.get(self, obj, cls)

    def set(self, obj, value):
        ''' Overload the traitlet's get method to inject a call to a
            method to set the state on a remote device.
        '''
        assert isinstance(obj, HasTraits)

        # If this is Device.settings instead of Device.state, do simple Trait.set,
        # skipping any communicate with the device
        if isinstance(obj, HasSettingsTraits):
            return self._parent.set(self, obj, value)

        # Trait.set already implements a check for self.read_only, so we don't
        # do that here.

        # Make sure the (potentially remapped) value meets criteria defined by
        # this traitlet subclass
        value = self.validate(obj, value)

        # Remap the resulting value to the remote value
        value = self.remap.get(value, value)

        # First, look for a getter function or method that implements getting
        # the value from the device
        if callable(self.metadata['setter']):
            self.metadata['setter'](obj._device, value)

        # If there isn't one, try calling the command_get() method in the Device instance.
        # Otherwise, raise an exception, because we don't know how to get the
        # value from the Device.
        else:  # self.command is not None:
            # try:
            # The below should raise CommandNotImplementedError if no command
            # or setter has been defined

            obj.__class__.__setter__(obj._device, self, value)
            # except CommandNotImplementedError as e:
            #     raise DeviceStateError(
            #         'no command or setter defined - cannot set state to device')

        # Apply the value to the traitlet, saving the cached value if necessary
        rd_only, self.read_only = self.read_only, False
        self._parent.set(self, obj, value)
        if self.cache or self.write_only:
            self.__last = self._parent.get(self, obj, self._parent)
        self.read_only = rd_only

    def setter(self, func, *args, **kws):
        self.metadata['setter'] = func
        return self

    def getter(self, func, *args, **kws):
        self.metadata['getter'] = func
        return self


class Int(TraitMixIn, traitlets.CInt):
    doc_attrs = ('min', 'max') + TraitMixIn.doc_attrs


class CFLoatSteppedTraitlet(traitlets.CFloat):
    def __init__(self, *args, **kws):
        self.step = kws.pop('step', None)
        super(CFLoatSteppedTraitlet, self).__init__(*args, **kws)


class Float(TraitMixIn, CFLoatSteppedTraitlet):
    doc_attrs = ('min', 'max', 'step') + TraitMixIn.doc_attrs

    def validate(self, obj, value):
        value = super(Float, self).validate(obj, value)

        # Round to nearest increment of step
        if self.step:
            value = round(value / self.step) * self.step
        return value


class Unicode(TraitMixIn, traitlets.CUnicode):
    default_value = ''


class Complex(TraitMixIn, traitlets.CComplex):
    pass


class Bytes(TraitMixIn, traitlets.CBytes):
    pass


class TCPAddress(TraitMixIn, traitlets.TCPAddress):
    pass


class List(TraitMixIn, traitlets.List):
    default_value = []


class EnumBytesTraitlet(traitlets.CBytes):
    def __init__(self, values=[], case_sensitive=False, **kws):
        if len(values) == 0:
            raise ValueError('Must define at least one enum value')

        self.values = [bytes(v) for v in values]
        self.case_sensitive = case_sensitive
        if case_sensitive:
            self.values = [v.upper() for v in self.values]

        self.info_text = 'castable to ' + \
                         ', '.join([repr(v) for v in self.values]) + \
                         ' (case insensitive)' if not case_sensitive else ''

        super(EnumBytesTraitlet, self).__init__(**kws)

    def validate(self, obj, value):
        try:
            value = bytes(value)
        except BaseException:
            self.error(obj, value)
        if self.case_sensitive:
            value = value.upper()
        if value not in self.values:
            self.error(obj, value)
        return value

    def devalidate(self, obj, value):
        return self.validate(obj, value)


class CaselessBytesEnum(TraitMixIn, EnumBytesTraitlet):
    doc_attrs = ('values', 'case_sensitive') + TraitMixIn.doc_attrs


class CaselessStrEnum(TraitMixIn, traitlets.CaselessStrEnum):
    doc_attrs = ('values',) + TraitMixIn.doc_attrs


class Dict(TraitMixIn, traitlets.Dict):
    pass


# class BoolTraitlet(traitlets.CBool):
# def __init__(self, trues=[True], falses=[False], **kws):
#     self._trues = [v.upper() if isinstance(v, str) else v for v in trues]
#     self._falses = [v.upper() if isinstance(v, str) else v for v in falses]
#     super(BoolTraitlet, self).__init__(**kws)
#
# def validate(self, obj, value):
#     if isinstance(value, str):
#         value = value.upper()
#     if value in self._trues:
#         return True
#     elif value in self._falses:
#         return False
#     elif not isinstance(value, numbers.Number):
#         raise ValueError(
#             'Need a boolean or numeric value to convert to integer, but given {} instead' \
#                 .format(repr(value)))
#     try:
#         return bool(value)
#     except:
#         self.error(obj, value)
#
# # Convert any castable value
# # to the first entry in self._trues or self._falses
# def devalidate(self, obj, value):
#     if self.validate(obj, value):
#         return self._trues[0]
#     else:
#         return self._falses[0]
#
# default_value = False


class Bool(TraitMixIn, traitlets.CBool):
    default_value = False


class DisconnectedBackend(object):
    ''' "Null Backend" implementation to raises an exception with discriptive
        messages on attempts to use a backend before a Device is connected.
    '''

    def __init__(self, dev):
        ''' dev may be a class or an object for error feedback
        '''
        self.__dev__ = dev

    def __getattribute__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            if inspect.isclass(self.__dev__):
                name = self.__dev__.__name__
            else:
                name = type(self.__dev__).__name__
            raise ConnectionError(
                'need to connect first to access backend.{key} in {clsname} instance (resource={resource})'
                    .format(key=key, clsname=name, resource=self.__dev__.settings.resource))

    def __repr__(self):
        return 'DisconnectedBackend()'


class DeviceLogAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """

    def process(self, msg, kwargs):
        return '%s - %s' % (self.extra['device'], msg), kwargs


def wrap(obj, to_name, from_func):
    ''' TODO: This looks like it duplicates functools.wrap - switch to
        functools.wrap?
    '''
    to_func = object.__getattribute__(obj, to_name)
    obj.__wrapped__[to_name] = to_func
    from_func.__func__.__doc__ = to_func.__doc__
    from_func.__func__.__name__ = to_func.__name__
    setattr(obj, to_name, from_func)


def trace_methods(cls, name, until_cls=None):
    ''' Look for a method called `name` in cls and all of its parent classes.
    '''
    methods = []
    last_method = None

    for cls in cls.__mro__:
        try:
            this_method = getattr(cls, name)
        except AttributeError:
            continue
        if this_method != last_method:
            methods.append(this_method)
            last_method = this_method
        if cls is until_cls:
            break

    return methods


def trim(docstring):
    if not docstring:
        return ''
    # Convert tabs to spaces (following the normal Python rules)
    # and split into a list of lines:
    lines = docstring.expandtabs().splitlines()
    # Determine minimum indentation (first line doesn't count):
    indent = sys.maxsize
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))
    # Remove indentation (first line is special):
    trimmed = [lines[0].strip()]
    if indent < sys.maxsize:
        for line in lines[1:]:
            trimmed.append(line[indent:].rstrip())
    # Strip off trailing and leading blank lines:
    while trimmed and not trimmed[-1]:
        trimmed.pop()
    while trimmed and not trimmed[0]:
        trimmed.pop(0)
    # Return a single string:
    return '\n'.join(trimmed)


class DeviceMetaclass(type):
    ''' Dynamically adjust the class documentation strings to include the traits
        defined in its settings. This way we get the documentation without
        error-prone copy and paste.
    '''

    def __init__(cls, name, bases, namespace):
        def autocomplete_wrapper(func, args, kws):
            all_ = ','.join(args + [k + '=' + k for k in kws.keys()])
            kws = dict(kws, __func__=func)
            expr = "lambda {a},*args,**kws: __func__({a},*args,**kws)".format(
                a=all_)
            wrapper = eval(expr, kws, {})
            for attr in '__name__', '__doc__', '__module__', '__qualname__':
                setattr(wrapper, attr, getattr(func, attr))
            return wrapper

        super(DeviceMetaclass, cls).__init__(name, bases, namespace)

        if len(cls.__mro__) < 2:
            return

        traits = cls.settings.class_traits()

        # Skip read-only traits
        traits = OrderedDict([(n, t)
                              for n, t in traits.items() if not t.read_only])

        kws = OrderedDict(resource=traits['resource'].default_value)
        for name, trait in traits.items():
            if not trait.read_only:
                kws[name] = trait.default_value

        wrapped = cls.__init__
        cls.__init__ = autocomplete_wrapper(cls.__init__, ['self'], kws)
        # bind_wrapper_to_class(cls, '__init__')
        import functools
        functools.update_wrapper(cls.__init__, wrapped)
        cls.__init__.__doc__ = dedent(
            cls.__init__.__doc__) if cls.__init__.__doc__ else ''

        if cls.__doc__ is None:
            cls.__doc__ = ''
        else:
            cls.__doc__ = trim(cls.__doc__)

        for name, trait in traits.items():
            ttype, text = trait.info_text.split('\n\n', 1)
            line = '\n\n:param `{type}` {name}:\n{info}' \
                .format(help=trait.help,
                        type=ttype,
                        info=text,
                        name=name)

            cls.__init__.__doc__ += line
            cls.__doc__ += line


class Device(object, metaclass=DeviceMetaclass):
    r'''`Device` is the base class common to all labbench
        drivers. Inherit it to implement a backend, or a specialized type of
        driver.

        Drivers that subclass `Device` get

        * device connection management via context management (the `with` statement)
        * test state management for easy test logging and extension to UI
        * a degree automatic stylistic consistency between drivers

        :param resource: resource identifier, with type and format determined by backend (see specific subclasses for details)
        :param **local_states: set the local state for each supplied state key and value

        .. note::
            Use `Device` by subclassing it only if you are
            implementing a driver that needs a new type of backend.

            Several types of backends have already been implemented
            as part of labbench:

                * VISADevice exposes a pyvisa backend for VISA Instruments
                * CommandLineWrapper exposes a threaded pipes backend for command line tools
                * Serial exposes a pyserial backend for serial port communication
                * DotNetDevice exposes a pythonnet for wrapping dotnet libraries

            (and others). If you are implementing a driver that uses one of
            these backends, inherit from the corresponding class above, not
            `Device`.
    '''

    class settings(HasSettingsTraits):
        ''' Container for settings traits in a Device. These settings
            are stored only on the host; setting or getting these values do not
            trigger live updates (or any communication) with the device. These
            define connection addressing information, communication settings,
            and options that only apply to implementing python support for the
            device.

            The device uses this container to define the keyword options supported
            by its __init__ function. These are applied when you instantiate the device.
            After you instantiate the device, you can still change the setting with::

                Device.settings.resource = 'insert-your-address-string-here'
        '''

        resource = Unicode(allow_none=True,
                           help='the data needed to make a connection to a device. its type and format are determined by the subclass implementation')
        concurrency_support = Bool(default_value=True, read_only=True,
                                   help='whether this :class:`Device` implementation supports threading')

    class state(HasStateTraits):
        ''' Container for state traits in a Device. Getting or setting state traits
            triggers live updates: communication with the device to get or set the
            value on the Device. Therefore, getting or setting state traits
            needs the device to be connected.

            To set a state value inside the device, use normal python assigment::

                device.state.parameter = value

            To get a state value from the device, you can also use it as a normal python variable::

                variable = device.state.parameter + 1
        '''

        connected = Bool(read_only=True,
                         help='whether the :class:`Device` instance is connected')

    backend = DisconnectedBackend(None)
    ''' .. attribute::state is the backend that controls communication with the device.
        it is to be set in `connect` and `disconnect` by the subclass that implements the backend.
    '''

    # Backend classes may optionally overload these, and do not need to call the parents
    # defined here
    def connect(self):
        ''' Backend implementations overload this to open a backend
            connection to the resource.
        '''
        pass

    def disconnect(self):
        ''' Backend implementations must overload this to disconnect an
            existing connection to the resource encapsulated in the object.
        '''
        self.backend = DisconnectedBackend(self)
        self.state.connected

    #    def command_get(self, command, trait):
    #        ''' Read a setting from a remote instrument, keyed on a command string.
    #            Implement this for message-based protocols, like VISA SCPI or some serial devices.
    #
    #            :param str command: the command message to apply to `value`
    #            :param trait: the state descriptor or traitlet
    #            :returns: the value retrieved from the instrument
    #        '''
    #        raise CommandNotImplementedError(
    #            'state "{attr}" is defined but not implemented! implement {cls}.command_set, or implement a setter for {cls}.state.{attr}'
    #            .format(cls=type(self).__name__, attr=trait))
    #
    #    def command_set(self, command, trait, value):
    #        ''' Apply an instrument setting to the instrument, keyed on a command string.
    #            Implement this for message-based protocols, like VISA SCPI or some serial devices.
    #
    #            :param str command: the command message to apply to `value`
    #            :param trait: the state descriptor or traitlet
    #            :param value: the value to set
    #        '''
    #        raise CommandNotImplementedError(
    #            'state "{attr}" is defined but not implemented! implement {cls}.command_get, or implement a getter for {cls}.state.{attr}'
    #            .format(cls=type(self).__name__, attr=trait))

    def __init__(self, resource=None, **settings):
        self.__wrapped__ = {}

        # Instantiate state, and observe connection state
        self.settings = self.settings(self)

        self.__imports__()

        self.backend = DisconnectedBackend(self)

        # Set local settings according to local_states
        all_settings = self.settings.traits()

        for k, v in dict(settings, resource=resource).items():
            if k == 'resource' and resource is None:
                continue
            if k not in all_settings:
                raise KeyError('tried to set initialize setting {k}, but it is not defined in {clsname}.settings'
                               .format(k=repr(k), clsname=type(self).__name__))
            setattr(self.settings, k, v)

        self.__params = ['resource={}'.format(repr(self.settings.resource))] + \
                        ['{}={}'.format(k, repr(v))
                         for k, v in settings.items()]

        self.logger = DeviceLogAdapter(logger, {'device': repr(self)})

        # Instantiate state now. It needs to be here, after settings are fully
        # instantiated, in case state implementation depends on settings
        self.state = self.state(self)

        wrap(self, 'connect', self.__connect_wrapper__)
        wrap(self, 'disconnect', self.__disconnect_wrapper__)

    def __connect_wrapper__(self, *args, **kws):
        ''' A wrapper for the connect() method. It works through the
            method resolution order of self.__class__, starting from
            labbench.Device, and calls its connect() method.
        '''
        if self.state.connected:
            self.logger.debug('{} already connected'.format(repr(self)))
            return

        self.backend = None

        for connect in trace_methods(self.__class__, 'connect', Device)[::-1]:
            connect(self)

        # Force an update to self.state.connected
        self.state.connected

    def __disconnect_wrapper__(self, *args, **kws):
        ''' A wrapper for the disconnect() method that runs
            cleanup() before calling disconnect(). disconnect()
            will still be called if there is an exception in cleanup().
        '''
        # Try to run cleanup(), but make sure to run
        # disconnect() even if it fails
        if not self.state.connected:
            self.logger.debug('{} already disconnected'.format(repr(self)))
            return

        methods = trace_methods(self.__class__, 'disconnect', Device)

        all_ex = []
        for disconnect in methods:
            try:
                disconnect(self)
            except BaseException as e:
                all_ex.append(sys.exc_info())

        # Print tracebacks for any suppressed exceptions
        for ex in all_ex[::-1]:
            # If ThreadEndedByMaster was raised, assume the error handling in
            # concurrently will print the error message
            if ex[0] is not ThreadEndedByMaster:
                depth = len(tuple(traceback.walk_tb(ex[2])))
                traceback.print_exception(*ex, limit=-(depth - 1))
                sys.stderr.write(
                    '(Exception suppressed to continue disconnect)\n\n')

        self.state.connected

        self.logger.debug('{} disconnected'.format(repr(self)))

    def __imports__(self):
        pass

    def __enter__(self):
        try:
            self.connect()
            return self
        except BaseException as e:
            args = list(e.args)
            if len(args) > 0:
                args[0] = '{}: {}'.format(repr(self), str(args[0]))
                e.args = tuple(args)
            raise e

    def __exit__(self, type_, value, traceback):
        try:
            self.disconnect()
        except BaseException as e:
            args = list(e.args)
            args[0] = '{}: {}'.format(repr(self), str(args[0]))
            e.args = tuple(args)
            raise e

    def __del__(self):
        self.disconnect()

    def __repr__(self):
        return '{}({})'.format(type(self).__name__,
                               repr(self.settings.resource))

    @state.connected.getter
    def __(self):
        return not isinstance(self.backend, DisconnectedBackend)

    __str__ = __repr__


def list_devices(depth=1):
    ''' Look for Device instances, and their names, in the calling
        code context (depth == 1) or its callers (if depth in (2,3,...)).
        Checks locals() in that context first.
        If no Device instances are found there, search the first
        argument of the first function argument, in case this is
        a method in a class.
    '''
    from inspect import getouterframes, currentframe
    from sortedcontainers import sorteddict

    f = getouterframes(currentframe())[depth]

    ret = sorteddict.SortedDict()
    for k, v in list(f.frame.f_locals.items()):
        if isinstance(v, Device):
            ret[k] = v

    # If the context is a function, look in its first argument,
    # in case it is a method. Search its class instance.
    if len(ret) == 0 and len(f.frame.f_code.co_varnames) > 0:
        obj = f.frame.f_locals[f.frame.f_code.co_varnames[0]]
        for k, v in obj.__dict__.items():
            if isinstance(v, Device):
                ret[k] = v

    return ret
