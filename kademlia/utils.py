from threading import Semaphore, Thread
from time import sleep


class ThreadManager:

    def __init__(self, alpha, start_cond, start_point, *args, **kwargs):
        self.semaphore = Semaphore(value=alpha)
        self.start_cond = start_cond
        self.args = args
        self.kwargs = kwargs
        self._cont = 0
        self._semcont = Semaphore()
        
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
            elif self._semcont.acquire() and self._cont == 0:
                self._semcont.release()
                return
            else:
                sleep(10)
                       
