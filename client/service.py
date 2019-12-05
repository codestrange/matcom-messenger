from rpyc import connect, discover, Service
from sqlalchemy.exc import SQLAlchemyError
from ..server.message import Message
from ..server.user_data import UserData
from ..server.utils import try_function
from .app.models import MessageModel, ContactModel


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
        with self.app.app_context():
            m = MessageModel(message.text, time=message.time)
            c = ContactModel.query.filter_by(tracker_id=str(message.sender)).first()
            if not c:
                result = ClientService.get_user_data(message.sender)
                if not result:
                    raise Exception()
                user_data = UserData.from_json(result)
                c = ContactModel(user_data.get_id(), user_data.get_phone(), user_data.get_name(), *user_data.get_dir())
            m.sender = c
            try:
                self.app.db.session.add(c)
                self.app.db.session.commit()
            except SQLAlchemyError:
                self.app.db.session.rollback()


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
                        return result
                except Exception:
                    continue
        except Exception:
            return None

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
