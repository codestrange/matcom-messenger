from logging import debug
from .bucket import Bucket
from .contact import Contact


class BucketTable:
    def __init__(self, k:int, b:int, id:int):
        self.k = k
        self.b = b
        self.id = id
        self.buckets = []
        for _ in range(b):
            self.buckets.append(Bucket(self.k))

    def get_bucket(self, id:int) -> Bucket:
        index = self.get_bucket_index(id)
        self.buckets[index].acquire()
        result = self.buckets[index]
        self.buckets[index].release()
        return result

    def get_bucket_index(self, id:int) -> int:
        distance = self.id ^ id
        debug(f'Distance between {self.id} and {id} = {distance}')
        return max([i for i in range(self.b) if distance & (1<<i) > 0])

    def update(self, contact:Contact) -> bool:
        bucket = self.get_bucket(contact.id)
        bucket.semaphore.acquire()
        result = bucket.update(contact)
        bucket.semaphore.release()
        return result

    def get_closest_buckets(self, id:int) -> list:
        index = self.get_bucket_index(id)
        left = self.buckets[:index]
        center = self.buckets[index]
        right = self.buckets[index+1:]
        lindex = len(left) - 1
        rindex = 0
        center.semaphore.acquire()
        for contact in center:
            yield contact
        center.semaphore.release()
        while lindex >= 0 and rindex < len(right):
            left[lindex].semaphore.acquire()
            for contact in left[lindex]:
                yield contact
            left[lindex].semaphore.release()
            right[rindex].semaphore.acquire()
            for contact in right[rindex]:
                yield contact
            right[rindex].semaphore.acquire()
            lindex -= 1
            rindex += 1
        while lindex >= 0:
            left[lindex].semaphore.acquire()
            for contact in left[lindex]:
                yield contact
            left[lindex].semaphore.release()
            lindex -= 1
        while rindex < len(right):
            right[rindex].semaphore.acquire()
            for contact in right[rindex]:
                yield contact
            right[rindex].semaphore.acquire()
            rindex += 1

    def __iter__(self):
        return iter(self.buckets)
