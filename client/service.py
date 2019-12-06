from datetime import datetime
from rpyc import discover, Service
from sqlalchemy.exc import SQLAlchemyError
from .app.models import MessageModel, ContactModel
from ..server import connect, Message, UserData


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
            m = MessageModel(message.text, time=datetime.strptime(message.time, '%Y-%m-%d %H:%M:%S.%f'))
            c = ContactModel.query.filter_by(tracker_id=str(message.sender)).first()
            if not c:
                result = ClientService.get_user_data(message.sender)
                if not result:
                    raise Exception()
                user_data = UserData.from_json(result)
                c = ContactModel(user_data.get_id(), user_data.get_phone(), user_data.get_name()[0], *user_data.get_dir()[0])
            m.sender = c
            try:
                self.app.db.session.add(c)
                self.app.db.session.commit()
            except SQLAlchemyError:
                self.app.db.session.rollback()


    @staticmethod
    def send_message_to(app, text: str, sender_id: int, ip: str, port: int, time: str):
        sender_id = int(sender_id)
        message = Message(text, sender_id, time)
        smessage = message.to_json()
        try: #Direct connection
            peer = connect(ip, port)
            result = peer.root.send_message(smessage)
            if result:
                return result
            #Verify if the recieber relocated to another ip:port
            new_data = ClientService.updateDB(app, sender_id)
            if new_data:
                peer = connect(*(new_data.get_dir()[0]))
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
    def send_message_to_group(app, text: str, sender_id: int, group_id: int, ip: str, port: int, time: str):
        sender_id = int(sender_id)
        sender_id = int(group_id)
        try:
            peer = connect(ip, port)
            group = peer.root.client_find_value(group_id)
            if not group:
                return False
            for member, _ in group.get_members():
                try:
                    member = ClientService.updateDB(app, member)
                    if member:
                        result = ClientService.send_message_to(app, text, sender_id, member, *(member.get_dir()[0]))
                        if result:
                            return result
                except:
                    pass
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
                    result = conn.root.client_store(user.get_id(), user.to_json())
                    if result:
                        return True
                except Exception:
                    continue
        except Exception:
            return False

    @staticmethod
    def updateDB(app, user: int):
        user = int(user)
        try:
            user = ClientService.get_user_data(user)
            if user:
                user = UserData.from_json(user)
                puser = ContactModel.query.filter_by(tracker_id=str(user.get_id())).first()
                puser = puser if puser else ContactModel(str(user.get_id()), user.get_phone(), user.get_name()[0], *(user.get_dir()[0]))
                try:
                    app.db.session.add(puser)
                    app.db.session.commit()
                except SQLAlchemyError as e:
                    print(e)
                    app.db.session.rollback()
            return user
        except Exception:
            return None
