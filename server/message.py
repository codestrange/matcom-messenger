from json import dumps, loads
from .utils import get_hash

class Message:
    def __init__(self, text: str, sender: int, time: str, group: int = None):
        self.text = text
        self.sender = sender
        self.time = time
        self.group = group
        self.id = get_hash(':'.join([str(sender), time, text]))

    def __hash__(self):
        return self.id

    def __eq__(self, obj):
        return self.__hash__() == obj.__hash__()

    def __ne__(self, obj):
        return self.__hash__() != obj.__hash__()

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return dumps({
            'time': self.time,
            'sender': self.sender,
            'text': self.text,
            'group': self.group
        })

    def to_json(self):
        return str(self)

    @staticmethod
    def from_json(data:str):
        data = loads(data)
        return Message(data['text'], data['sender'], data['time'], group=data['group'])
