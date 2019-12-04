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
    def send_message_to(message: Message, to_user: UserData):
        smessage = message.to_json()
        to_ip, to_port = '0.0.0.0', 3000 
        try: #Direct connection
            peer = connect(to_ip, to_port)
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
                        result = conn.root.client_store(to_user.get_id(), smessage, option=5)
                        if result:
                            return result
                        raise Exception()
                    except Exception:
                        continue
            except Exception:
                pass
        return False
