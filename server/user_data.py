from json import dumps, loads
from logging import debug
from threading import Semaphore
from .message import Message
from .utils import get_hash


class UserData:
    def __init__(self, name: str = None, phone: str = None, password: str = None, creation_time: int = None, nonce: int = 0):
        self.__name = name
        self.__name_time = -1 if creation_time is None else creation_time
        self.__sem_name = Semaphore()
        self.__nonce = nonce
        self.__phone = phone
        self.__id = get_hash(':'.join([self.__phone, str(self.__nonce)])) if phone else None
        self.__members = set()
        self.__non_members = set()
        self.__members_time = {}
        self.__sem_members = Semaphore()
        self.__groups = set()
        self.__non_groups = set()
        self.__groups_time = {}
        self.__sem_groups = Semaphore()
        self.__password = get_hash(password) if isinstance(password, str) else password
        self.__password_time = -1 if creation_time is None else creation_time
        self.__sem_password = Semaphore()
        self.__messages = set()
        self.__sem_messages = Semaphore()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        self.__sem_name.acquire()
        self.__sem_members.acquire()
        self.__sem_groups.acquire()
        self.__sem_password.acquire()
        self.__sem_messages.acquire()
        result = dumps({
            'name': self.__name,
            'name_time': self.__name_time,
            'nonce': self.__nonce,
            'password': self.__password,
            'password_time': self.__password_time,
            'members': list(self.__members),
            'non_members': list(self.__non_members),
            'members_time': self.__members_time,
            'groups': list(self.__groups),
            'non_groups': list(self.__non_groups),
            'groups_time': self.__groups_time,
            'phone': self.__phone,
            'id': self.__id,
            'messages': list(self.__messages),
        })
        self.__sem_messages.release()
        self.__sem_name.release()
        self.__sem_members.release()
        self.__sem_groups.release()
        self.__sem_password.release()
        return result

    def get_id(self):
        return self.__id

    def get_name(self):
        self.__sem_name.acquire()
        result = self.__name, self.__name_time
        self.__sem_name.release()
        return result

    def get_password(self):
        self.__sem_password.acquire()
        result = self.__password, self.__password_time
        self.__sem_password.release()
        return result

    def get_members(self):
        self.__sem_members.acquire()
        result = list(self.__members)
        self.__sem_members.release()
        return result

    def get_groups(self):
        self.__sem_groups.acquire()
        result = list(self.__groups)
        self.__sem_groups.release()
        return result

    def get_nonce(self):
        return self.__nonce

    def get_phone(self):
        return self.__phone

    def set_name(self, nname: str, ntime: int):
        self.__sem_name.acquire()
        result = False
        if ntime > self.__name_time:
            result = True
            self.__name, self.__name_time = nname, ntime
        self.__sem_name.release()
        return result

    def set_password(self, npassword: str, ntime: int):
        self.__sem_password.acquire()
        result = False
        if ntime > self.__password_time:
            result = True
            self.__password = get_hash(npassword) if not isinstance(npassword, int) else npassword
            self.__password_time = ntime
        self.__sem_password.release()
        return result

    def add_group(self, group: int, time: int):
        self.__sem_groups.acquire()
        if not group in self.__groups_time:
            self.__groups.add(group)
            self.__groups_time[group] = time
        elif group in self.__groups:
            self.__groups_time[group] = max(self.__groups_time[group], time)
        elif self.__groups_time[group] < time:
            self.__non_groups.remove(group)
            self.__groups.add(group)
            self.__groups_time[group] = time
        self.__sem_groups.release()

    def remove_group(self, group: int, time: int):
        self.__sem_groups.acquire()
        if group in self.__groups_time and group in self.__non_groups:
            self.__groups_time[group] = max(self.__groups_time[group], time)
        elif group in self.__groups_time and self.__groups_time[group] < time:
            self.__groups.remove(group)
            self.__non_groups.add(group)
            self.__groups_time[group] = time
        self.__sem_groups.release()

    def add_member(self, member: int, time: int):
        self.__sem_members.acquire()
        if not member in self.__members_time:
            self.__members.add(member)
            self.__members_time[member] = time
        elif member in self.__members:
            self.__members_time[member] = max(self.__members_time[member], time)
        elif self.__members_time[member] < time:
            self.__non_members.remove(member)
            self.__members.add(member)
            self.__members_time[member] = time
        self.__sem_members.release()

    def remove_member(self, member: int, time: int):
        self.__sem_members.acquire()
        if member in self.__members_time and member in self.__non_members:
            self.__members_time[member] = max(self.__members_time[member], time)
        elif member in self.__members_time and self.__members_time[member] < time:
            self.__members.remove(member)
            self.__non_members.add(member)
            self.__members_time[member] = time
        self.__sem_members.release()

    def to_json(self):
        return str(self)

    def add_message(self, message:Message):
        self.__sem_messages.acquire()
        self.__messages.add(message)
        self.__sem_messages.release()

    def clear_messages(self):
        self.__sem_messages.acquire()
        self.__messages.clear()
        self.__sem_messages.release()

    @staticmethod
    def from_json(data: str):
        data = loads(data)
        user = UserData(data['name'], data['phone'], data['password'], -1, nonce=data['nonce'])
        user.set_name(data['name'], data['name_time'])
        user.set_password(data['password'], data['password_time'])
        for group in data['groups']:
            user.add_group(group, data['groups_time'][str(group)])
        for member in data['members']:
            user.add_member(member, data['members_time'][str(member)])
        for group in data['non_groups']:
            user.__non_groups.add(group)
            user.__groups_time[group] = data['groups_time'][str(group)]
        for member in data['non_members']:
            user.__non_members.add(member)
            user.__members_time[member] = data['members_time'][str(member)]
        for message in data['messages']:
            user.add_message(message)
        assert(user.__id == data['id'])
        return user

    def update(self, new_user_data):
        self.__id = new_user_data.__id
        self.__nonce = new_user_data.__nonce
        self.__phone = new_user_data.__phone
        self.set_name(*new_user_data.get_name())
        self.set_password(*new_user_data.get_password())
        self.__sem_members.acquire()
        for member in new_user_data.__members:
            if member in self.__members:
                self.__members_time[member] = max(self.__members_time[member], new_user_data.__members_time[member])
            elif member in self.__non_members:
                if self.__members_time[member] < new_user_data.__members_time[member]:
                    self.__non_members.remove(member)
                    self.__members.add(member)
                    self.__members_time[member] = new_user_data.__members_time[member]
            else:
                self.__members.add(member)
                self.__members_time[member] = new_user_data.__members_time[member]
        for member in new_user_data.__non_members:
            if member in self.__non_members:
                self.__members_time[member] = max(self.__members_time[member], new_user_data.__members_time[member])
            elif member in self.__members:
                if self.__members_time[member] < new_user_data.__members_time[member]:
                    self.__members.remove(member)
                    self.__non_members.add(member)
                    self.__members_time[member] = new_user_data.__members_time[member]
            else:
                self.__non_members.add(member)
                self.__members_time[member] = new_user_data.__members_time[member]
        self.__sem_members.release()
        self.__sem_groups.acquire()
        for group in new_user_data.__groups:
            if group in self.__groups:
                self.__groups_time[group] = max(self.__groups_time[group], new_user_data.__groups_time[group])
            elif group in self.__non_groups:
                if self.__groups_time[group] < new_user_data.__groups_time[group]:
                    self.__non_groups.remove(group)
                    self.__groups.add(group)
                    self.__groups_time[group] = new_user_data.__groups_time[group]
            else:
                self.__groups.add(group)
                self.__groups_time[group] = new_user_data.__groups_time[group]
        for group in new_user_data.__non_groups:
            if group in self.__non_groups:
                self.__groups_time[group] = max(self.__groups_time[group], new_user_data.__groups_time[group])
            elif group in self.__groups:
                if self.__groups_time[group] < new_user_data.__groups_time[group]:
                    self.__groups.remove(group)
                    self.__non_groups.add(group)
                    self.__groups_time[group] = new_user_data.__groups_time[group]
            else:
                self.__non_groups.add(group)
                self.__groups_time[group] = new_user_data.__groups_time[group]
        self.__sem_messages.acquire()
        for message in new_user_data.__messages:
            self.add_message(message)
        self.__sem_messages.release()
        self.__sem_groups.release()

    def set_times(self, time: int):
        debug(f'UserData - Start set times')
        debug(f'UserData - Acquire lock for name')
        self.__sem_name.acquire()
        self.__name_time = time
        self.__sem_name.release()
        debug(f'UserData - Release lock for name')
        debug(f'UserData - Acquire lock for password')
        self.__sem_password.acquire()
        self.__password_time = time
        self.__sem_password.release()
        debug(f'UserData - Release lock for password')
        debug(f'UserData - End set times')
