from threading import Semaphore

class Data:
    def __init__(self):
        self.lock = Semaphore()
