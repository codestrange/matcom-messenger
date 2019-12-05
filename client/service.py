from rpyc import connect, discover, Service
from ..server.message import Message
from ..server.user_data import UserData
from ..server.utils import try_function


class ClientService(Service):
    def __init__(self, app):
        self.app = app

    def exposed_send_message(self, message: str):
        message = Message.from_json(message)
        try:
            self.insert_message(message)
        except Exception:
            return False
        return True
    
    def insert_message(self, message: Message):
        pass

    @staticmethod
    def send_message_to(text: str, sender_id: int, ip: str, port: int, time: str):
        message = Message(text, sender_id, time)
        smessage = message.to_json()
        try: #Direct connection
            peer = connect(ip, port)
            result = peer.root.send_message(smessage)
            if result:
                return result
            raise Exception()
        except Exception: #Send message to DHT
            try:
                dht_nodes = discover('TRACKER')
                for node in dht_nodes:
                    try:
                        conn = connect(*node)
                        result = conn.root.client_store(sender_id, smessage, option=5)
                        if result:
                            return True
                    except Exception:
                        continue
            except Exception:
                pass
        return False

    @staticmethod
    def get_user_data(id: int, remove_messages: bool = False):
        try:
            dht_nodes = discover('TRACKER')
            for node in dht_nodes:
                try:
                    conn = connect(*node)
                    result = conn.root.client_find_value(id, remove_messages)
                    if result:
                        return True
                except Exception:
                    continue
        except Exception:
            return False

    @staticmethod
    def store_user_data(phone: str, nonce: int, name: str, password: str, ip: str, port: int):
        user = UserData(name, phone, password, -1, nonce, ip, port)
        try:
            dht_nodes = discover('TRACKER')
            for node in dht_nodes:
                try:
                    conn = connect(*node)
                    result = conn.root.client_store(user.get_id(), user)
                    if result:
                        return True
                except Exception:
                    continue
        except Exception:
            return False
