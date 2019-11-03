class Contact:
    def __init__(self, hash:int=None, ip:str=None, port:int=None):
        self.hash = hash
        self.ip = ip
        self.port = port

    def __eq__(self, contact) -> bool:
        return self.hash == contact.hash and self.ip == contact.ip and self.port == contact.port

    def __repr__(self):
        return f"<{self.hash}, {self.ip}, {self.port}>"
