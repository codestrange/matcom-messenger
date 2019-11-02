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

    def get_bucket(self, contact:Contact) -> Bucket:
        distance = self.hash ^ contact.hash
        index = max([i for i in range(self.b) if (distance & (1<<i)) > 0])
        return self.buckets[index]
        

    def update(self, contact:Contact) -> bool:
        bucket = self.get_bucket(contact)
        return bucket.update(contact)

        
