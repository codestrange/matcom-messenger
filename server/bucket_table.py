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
        debug(f'BucketTable.get_bucket - Getting bucket for id: {id}.')
        index = self.get_bucket_index(id)
        self.buckets[index].semaphore.acquire()
        result = self.buckets[index]
        debug(f'BucketTable.get_bucket - Result bucket with index: {index} is result: {result}.')
        self.buckets[index].semaphore.release()
        return result

    def get_bucket_index(self, id:int) -> int:
        debug(f'BucketTable.get_bucket_index - Getting bucket index for id: {id}.')
        distance = self.id ^ id
        debug(f'BucketTable.get_bucket_index - Distance between {self.id} and {id} = {distance}.')
        index = max([i for i in range(self.b) if distance & (1<<i) > 0], default=0)
        debug(f'BucketTable.get_bucket_index - Resulting index: {index}.')
        return index

    def update(self, contact:Contact) -> bool:
        debug(f'BucketTable.update - Updating contact: {contact}.')
        bucket = self.get_bucket(contact.id)
        debug(f'BucketTable.update - Bucket to update bucket: {bucket}.')
        bucket.semaphore.acquire()
        result = bucket.update(contact)
        debug(f'BucketTable.update - Result for update result: {result}.')
        bucket.semaphore.release()
        return result

    def get_closest_buckets(self, id:int) -> list:
        debug(f'BucketTable.get_closest_buckets - Starting method with id: {id}')
        index = self.get_bucket_index(id)
        debug(f'BucketTable.get_closest_buckets - Index of bucket of id: {id} is {index}')
        left = self.buckets[:index]
        center = self.buckets[index]
        right = self.buckets[index+1:]
        debug(f'BucketTable.get_closest_buckets - Iterators members are left: {left} center:{center} right: {right}.')
        lindex = len(left) - 1
        rindex = 0
        center.semaphore.acquire()
        for contact in center:
            debug(f'BucketTable.get_closest_buckets - Returning contact: {contact}.')
            yield contact
        center.semaphore.release()
        while lindex >= 0 and rindex < len(right):
            left[lindex].semaphore.acquire()
            for contact in left[lindex]:
                debug(f'BucketTable.get_closest_buckets - Returning contact: {contact}.')
                yield contact
            left[lindex].semaphore.release()
            right[rindex].semaphore.acquire()
            for contact in right[rindex]:
                debug(f'BucketTable.get_closest_buckets - Returning contact: {contact}.')
                yield contact
            right[rindex].semaphore.release()
            lindex -= 1
            rindex += 1
        while lindex >= 0:
            left[lindex].semaphore.acquire()
            for contact in left[lindex]:
                debug(f'BucketTable.get_closest_buckets - Returning contact: {contact}.')
                yield contact
            left[lindex].semaphore.release()
            lindex -= 1
        while rindex < len(right):
            right[rindex].semaphore.acquire()
            for contact in right[rindex]:
                debug(f'BucketTable.get_closest_buckets - Returning contact: {contact}.')
                yield contact
            right[rindex].semaphore.release()
            rindex += 1
        debug(f'BucketTable.get_closest_buckets - Finish the method')

    def __iter__(self):
        return iter(self.buckets)

    def __len__(self):
        return len(self.buckets)
