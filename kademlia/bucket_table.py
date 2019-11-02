from bucket import Bucket


class BucketTable:
    def __init__(self, k:int, b:int, hash:str):
        self.k = k
        self.b = b
        self.hash = hash
        self.buckets = [None] * b

    def get_bucket(contact:Contact) -> Bucket:
        pass

    def update(contact:Contact) -> bool:
        pass
