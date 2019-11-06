from hashlib import sha1

def get_hash(elem: str) -> int:
    return int.from_bytes(sha1(elem.encode()).digest(), 'little')
