from datetime import datetime
from logging import error
from rpyc import discover, Service
from sqlalchemy.exc import SQLAlchemyError
from .app.models import ContactModel, GroupModel, MessageModel, UserModel
from ..server import connect, Message, UserData


class ClientService(Service):
    def __init__(self, app):
        self.app = app

    def exposed_send_message(self, message: str):
        message = Message.from_json(message)
        try:
            self.insert_message(message)
        except Exception as e:
            error(f'ClientService.exposed_send_message - {e}')
            return False
        return True

    def insert_message(self, message: Message):
        with self.app.app_context():
            m = MessageModel(message.text, time=datetime.strptime(message.time, '%Y-%m-%d %H:%M:%S.%f'))
            c = ContactModel.query.filter_by(tracker_id=str(message.sender)).first()
            if not c:
                result = ClientService.update_db(self.app, message.sender)
                if not result:
                    raise Exception()
                c = ContactModel.query.filter_by(tracker_id=str(message.sender)).first()
            m.sender = c
            group = None
            if message.group:
                g = GroupModel.query.filter_by(tracker_id=str(message.group)).first()
                if not g:
                    result = ClientService.update_db(self.app, message.group, True)
                    if not result:
                        raise Exception()
                    g = GroupModel.query.filter_by(tracker_id=str(message.group)).first()
                group = g
            m.group = group
            try:
                self.app.db.session.add(m)
                self.app.db.session.commit()
            except SQLAlchemyError as e:
                error(f'ClientService.insert_message - {e}')
                self.app.db.session.rollback()

    @staticmethod
    def send_message_to(app, text: str, sender_id: int, to_id: int, ip: str, port: int, time: str, group_id: int = None):
        sender_id = int(sender_id)
        to_id = int(to_id)
        user = UserModel.query.first()
        if user:
            if int(user.tracker_id) == to_id:
                return True
        message = Message(text, sender_id, time, group=group_id)
        smessage = message.to_json()
        try:  # Direct connection
            peer = connect(ip, port, timeout=0.5)
            result = peer.root.send_message(smessage)
            if result:
                return result
            # Verify if the recieber relocated to another ip:port
            new_data = ClientService.update_db(app, to_id)
            if new_data:
                peer = connect(*(new_data.get_dir()[0]), timeout=0.5)
                result = peer.root.send_message(smessage)
                if result:
                    return result
            raise Exception()
        except Exception as e:  # Send message to DHT
            error(f'ClientService.send_message_to - {e}')
            try:
                dht_nodes = discover('TRACKER')
                for node in dht_nodes:
                    try:
                        conn = connect(*node)
                        result = conn.root.client_store(to_id, smessage, option=5)
                        if result:
                            return True
                    except Exception as e2:
                        error(f'ClientService.send_message_to - {e2}')
                        continue
            except Exception as e1:
                error(f'ClientService.send_message_to - {e1}')
        return False

    @staticmethod
    def send_message_to_group(app, text: str, sender_id: int, group_id: int, time: str):
        sender_id = int(sender_id)
        group_id = int(group_id)
        try:
            dht_nodes = discover('TRACKER')
        except Exception as e1:
            error(f'ClientService.send_message_to_group - {e1}')
            return False
        for node in dht_nodes:
            try:
                peer = connect(*node, timeout=0.5)
                group = peer.root.client_find_value(group_id)
                if not group:
                    return False
                result = False
                group = UserData.from_json(group)
                for member in group.get_members():
                    dir = None
                    m = ContactModel.query.filter_by(tracker_id=str(member)).first()
                    if m:
                        dir = m.ip, m.port
                    else:
                        m = ClientService.update_db(app, member)
                        if not m:
                            continue
                        dir = m.get_dir()[0]
                    result = result | ClientService.send_message_to(app, text, sender_id, member, *dir, time, group_id=group_id)
                if result:
                    return True
            except Exception as e:
                error(f'ClientService.send_message_to_group - {e}')
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
                except Exception as e1:
                    error(f'ClientService.get_user_data - {e1}')
                    continue
        except Exception as e:
            error(f'ClientService.get_user_data - {e}')
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
                except Exception as e1:
                    error(f'ClientService.store_user_data - {e1}')
                    continue
        except Exception as e:
            error(f'ClientService.store_user_data - {e}')
            return False

    @staticmethod
    def update_db(app, user: int, is_group: bool = False):
        user = int(user)
        try:
            user = ClientService.get_user_data(user)
            if user:
                user = UserData.from_json(user)
                puser = None
                if is_group:
                    puser = GroupModel.query.filter_by(tracker_id=str(user.get_id())).first()
                    if puser:
                        puser.name = user.get_name()[0]
                    else:
                        puser = GroupModel(str(user.get_id()), user.get_name()[0])
                else:
                    puser = ContactModel.query.filter_by(tracker_id=str(user.get_id())).first()
                    if puser:
                        puser.phone = user.get_phone()
                        puser.name = user.get_name()[0]
                        puser.ip, puser.port = user.get_dir()[0]
                    else:
                        puser = ContactModel(str(user.get_id()), user.get_phone(), user.get_name()[0], *(user.get_dir()[0]))
                try:
                    app.db.session.add(puser)
                    app.db.session.commit()
                except SQLAlchemyError as e1:
                    error(f'ClientService.update_db - {e1}')
                    app.db.session.rollback()
            return user
        except Exception as e:
            error(f'ClientService.update_db - {e}')
            return None

    @staticmethod
    def add_user_to_group(app, user_id, group_id):
        user_id = int(user_id)
        group_id = int(group_id)
        try:
            dht_nodes = discover('TRACKER')
            for node in dht_nodes:
                try:
                    peer = connect(*node, timeout=0.5)
                    result = peer.root.client_store(user_id, group_id, option=1)
                    result = result and peer.root.client_store(group_id, user_id, option=3)
                    if result:
                        return True
                except Exception as e1:
                    error(f'ClientService.add_user_to_group - {e1}')
        except Exception as e:
            error(f'ClientService.add_user_to_group - {e}')
        return False

    @staticmethod
    def remove_user_from_group(app, user_id, group_id):
        user_id = int(user_id)
        group_id = int(group_id)
        try:
            dht_nodes = discover('TRACKER')
            for node in dht_nodes:
                try:
                    peer = connect(*node, timeout=0.5)
                    result = peer.root.client_store(user_id, group_id, option=2)
                    result = result and peer.root.client_store(group_id, user_id, option=4)
                    if result:
                        return True
                except Exception as e1:
                    error(f'ClientService.remove_user_from_group - {e1}')
        except Exception as e:
            error(f'ClientService.remove_user_from_group - {e}')
        return False
