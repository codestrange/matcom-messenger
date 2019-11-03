from bisect import bisect
from threading import Semaphore, Thread
from time import sleep
from .contact import Contact


class ThreadManager:
    def __init__(self, alpha, start_cond, start_point, args=(), kwargs={}, time_sleep=1):
        self.semaphore = Semaphore(value=alpha)
        self.start_cond = start_cond
        self.args = args
        self.kwargs = kwargs
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
            if self.start_cond():
                self.semaphore.acquire()
                t = Thread(target=self.start_point, args=self.args, kwargs=self.kwargs)
                self._semcont.acquire()
                self._cont += 1
                self._semcont.release()
                t.start()
                self.semaphore.release()
            else:
                self._semcont.acquire()
                if self._cont == 0:
                    self._semcont.release()
                    return
                self._semcont.release()
            sleep(self.time_sleep)


class KContactSortedArray:
    def __init__(self, k:int, reference:Contact):
        self.k = k
        self.values = []
        self.reference = reference
        self.semaphore = Semaphore()

    def push(self, contact:Contact) -> bool:
        self.semaphore.acquire()
        difference = self.reference.hash ^ contact.hash
        index = bisect([d for d, c in self.values], difference)
        self.values.insert(index, (difference, contact))
        while len(self.values) > self.k:
            self.values.pop()
        self.semaphore.release()

    def __iter__(self):
        self.semaphore.acquire()
        for d, c in self.values:
            yield c
        self.semaphore.release()
