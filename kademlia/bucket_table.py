from bucket import Bucket
from contact import Contact


class BucketTable:
    def __init__(self, k:int, b:int, hash:int):
        self.k = k
        self.b = b
        self.hash = hash
        self.buckets = []
        for _ in range(b):
            self.buckets.append(Bucket(self.k))

    def get_bucket(self, hash:int) -> Bucket:
        distance = self.hash ^ hash
        index = max([i for i in range(self.b) if (distance & (1<<i)) > 0])
        self.buckets[index].acquire()
        result = self.buckets[index]
        self.buckets[index].release()
        return result

    def update(self, contact:Contact) -> bool:
        bucket = self.get_bucket(contact.hash)
        bucket.semaphore.acquire()
        result = bucket.update(contact)
        bucket.semaphore.release()
        return result
