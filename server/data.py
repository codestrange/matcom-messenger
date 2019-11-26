from threading import Semaphore

class Data(dict):
    def __init__(self):
        super(Data, self).__init__()
        self.lock = Semaphore()
