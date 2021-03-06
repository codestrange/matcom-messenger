from bisect import bisect_left
from hashlib import sha1
from logging import debug, error
from threading import Semaphore, Thread
from time import sleep
from rpyc.core.service import VoidService
from rpyc.core.stream import SocketStream
from rpyc.utils.factory import connect_stream
from .contact import Contact


class IterativeManager:
    def __init__(self, start_cond, start_point, args=(), kwargs=None):
        self.start_cond = start_cond
        self.target = start_point
        self.__args = args
        self.__kwargs = kwargs if kwargs else {} 

    def start(self):
        while self.start_cond():
            self.target(*self.__args, **self.__kwargs)


class ThreadManager:
    def __init__(self, alpha, start_cond, start_point, args=(), kwargs=None, time_sleep=1):
        self.semaphore = Semaphore(value=alpha)
        self.start_cond = start_cond
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self._cont = 0
        self._semcont = Semaphore()
        self.time_sleep = time_sleep

        def locked_start(*largs, **lkwargs):
            self.semaphore.acquire()
            start_point(*largs, **lkwargs)
            self._semcont.acquire()
            self._cont -= 1
            self._semcont.release()
            self.semaphore.release()

        self.start_point = locked_start

    def start(self):
        while True:
            value = self.start_cond()
            debug(f'ThreadManager.start - Result of start condition is: {value}')
            if value:
                debug(f'ThreadManager.start - Acquire General Semaphore')
                self.semaphore.acquire()
                debug(f'ThreadManager.start - Try to create Thread')
                t = Thread(target=self.start_point, args=self.args, kwargs=self.kwargs)
                debug(f'ThreadManager.start - Acquire Secondary Semaphore')
                self._semcont.acquire()
                self._cont += 1
                debug(f'ThreadManager.start - Add one to counter of threads. _cont = {self._cont}')
                debug(f'ThreadManager.start - Release Secondary Semaphore')
                self._semcont.release()
                debug(f'ThreadManager.start - Start thread')
                t.start()
                debug(f'ThreadManager.start - Release General Semaphore')
                self.semaphore.release()
            else:
                self._semcont.acquire()
                debug(f'ThreadManager.start - Acquire Secondary Semaphore')
                if self._cont == 0:
                    debug(f'ThreadManager.start - Release Secondary Semaphore')
                    self._semcont.release()
                    debug(f'ThreadManager.start - Finish manager')
                    return
                debug(f'ThreadManager.start - Release Secondary Semaphore')
                self._semcont.release()
            debug(f'ThreadManager.start - Sleeping {self.time_sleep} seconds and try again')
            sleep(self.time_sleep)


class KContactSortedArray:
    def __init__(self, k: int, reference: int):
        self.k = k
        self.values = []
        self.reference = reference
        self.semaphore = Semaphore()

    def push(self, contact: Contact) -> bool:
        self.semaphore.acquire()
        difference = self.reference ^ contact.id
        index = bisect_left([d for d, _ in self.values], difference)
        if not self.values or index >= len(self.values) or self.values[index][0] != difference:
            self.values.insert(index, (difference, contact))
            while len(self.values) > self.k:
                self.values.pop()
        self.semaphore.release()

    def __iter__(self):
        self.semaphore.acquire()
        for _, c in self.values:
            yield c
        self.semaphore.release()


def get_hash(elem: str) -> int:
    return int.from_bytes(sha1(elem.encode()).digest(), 'little')


def get_id(elem: tuple) -> int:
    return get_hash(f'{elem[0]}:{elem[1]}')


def try_function(times=1, sleep_time=0):
    def decorator(function):
        def inner(*args, **kwargs):
            count = 0
            while count < times:
                try:
                    result = function(*args, **kwargs)
                    return True, result
                except Exception as e:
                    count += 1
                    error(f'try_function - {e}')
                if sleep_time:
                    sleep(sleep_time)
            return False, None
        return inner
    return decorator


def connect(host, port, service=VoidService, config={}, ipv6=False, keepalive=False, timeout=3):
    s = SocketStream.connect(host, port, ipv6=ipv6, keepalive=keepalive, timeout=timeout)
    return connect_stream(s, service, config)
