from contact import Contact
from multiprocessing import Semaphore


class Bucket:
    def __init__(self, k:int):
        self.k = k
        self.nodes = []
        self.semaphore = Semaphore()

    def update(self, contact:Contact, index:int=None) -> bool:
        index = len(self.nodes) if index is None or index > len(self.nodes) else index
        index = 0 if index < 0 else index

    def find(self, contact:Contact) -> int:
        pass

    def remove_by_index(self, index:int) -> bool:
        pass

    def remove_by_contact(self, contact:Contact) -> bool:
        pass
