class Contact:
    def __init__(self, hash:str=None, ip:str=None, port:int=None):
        self.hash = hash
        self.ip = ip
        self.port = port

    def __eq__(self, contact:Contact) -> bool:
        return self.hash = contact.hash and self.ip = contact.ip and self.port = contact.port
