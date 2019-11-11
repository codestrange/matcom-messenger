from json import dumps, loads


class Contact:
    def __init__(self, id:int=None, ip:str=None, port:int=None):
        self.id = id
        self.ip = ip
        self.port = port

    def clone(contact):
        return Contact(contact.id, contact.ip, contact.port)

    def __eq__(self, contact) -> bool:
        return self.id == contact.id and self.ip == contact.ip and self.port == contact.port

    def __repr__(self):
        return f"<{self.id}, {self.ip}, {self.port}>"

    def to_json(self):
        return dumps({
            'id': self.id,
            'ip': self.ip,
            'port': self.port
        })

    @staticmethod
    def from_json(contact):
        contact = loads(contact)
        return Contact(contact['id'], contact['ip'], contact['port'])
