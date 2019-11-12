from threading import Semaphore
from .contact import Contact


class Bucket:
    def __init__(self, k:int):
        self.k = k
        self.nodes = []
        self.semaphore = Semaphore()

    def update(self, contact:Contact, index:int=None) -> bool:
        index = len(self.nodes) if index is None or index > len(self.nodes) else index
        index = 0 if index < 0 else index
        self.remove_by_contact(contact)
        if len(self.nodes) >= self.k:
            return False
        self.nodes.insert(index, contact)

    def find(self, contact:Contact) -> int:
        for index, node in enumerate(self.nodes):
            if node == contact:
                return index
        return -1

    def remove_by_index(self, index:int) -> bool:
        try:
            self.nodes.pop(index)
            return True
        except IndexError:
            return False

    def remove_by_contact(self, contact:Contact) -> bool:
        index = self.find(contact)
        if index < 0:
            return False
        self.nodes.pop(index)

    def __iter__(self):
        return iter(self.nodes)

    def __repr__(self):
        return repr(self.nodes)

    def __str__(self):
        return str(self.nodes)
