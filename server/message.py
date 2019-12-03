from json import dumps, loads
from .utils import get_hash

class Message:
    def __init__(self, text: str, sender: int, time: str):
        self.text = text
        self.sender = text
        self.time = time
        self.id = get_hash(':'.join([sender, time, text]))

    def __hash__(self):
        return self.id

    def to_json(self):
        return dumps({
            'time': self.time,
            'sender': self.sender,
            'text': self.text
        })
    
    @staticmethod
    def from_json(data:str):
        return Message(data['text'], data['time'], data['sender'])
