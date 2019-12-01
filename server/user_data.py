from json import dumps, loads
from threading import Semaphore
from .utils import get_hash


class UserData:
    def __init__(self, name: str = None, phone: str = None, password: str = None, creation_time: int = -1, nonce: int = 0):
        self.__name = name
        self.__name_time = creation_time
        self.__sem_name = Semaphore()
        self.__nonce = nonce
        self.__phone = phone
        self.__id = get_hash(':'.join([self.__phone, str(self.__nonce)]))
        self.__members = set()
        self.__sem_members = Semaphore()
        self.__groups = set()
        self.__sem_groups = Semaphore()
        self.__password = get_hash(password) if not isinstance(password, int) else password
        self.__password_time = creation_time
        self.__sem_password = Semaphore()

    def __repr__(self):
        return str(self)
    
    def __str__(self):
        self.__sem_name.acquire()
        self.__sem_members.acquire()
        self.__sem_groups.acquire()
        self.__sem_password.acquire()
        result = dumps({
            'name': self.__name,
            'name_time': self.__name_time,
            'nonce': self.__nonce,
            'password': self.__password,
            'password_time': self.__password_time,
            'members': list(self.__members),
            'groups': list(self.__groups),
            'phone': self.__phone,
            'id': self.__id,
        })
        self.__sem_name.release()
        self.__sem_members.release()
        self.__sem_groups.release()
        self.__sem_password.release()
        return result

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

    def get_id(self):
        return self.__id

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

    def add_group(self, group: int):
        self.__sem_groups.acquire()
        self.__groups.add(group)
        self.__sem_groups.release()

    def add_member(self, member: int):
        self.__sem_members.acquire()
        self.__members.add(member)
        self.__sem_members.release()

    def to_json(self):
        return str(self)

    @staticmethod
    def from_json(data: str):
        data = loads(data)
        user = UserData(data['name'], data['phone'], data['password'], -1, nonce=data['nonce'])
        user.set_name(data['name'], data['name_time'])
        user.set_name(data['password'], data['password_time'])
        for group in data['groups']:
            user.add_group(group)
        for member in data['members']:
            user.add_member(member)
        assert(user.__id == data['id'])
        return user

    def update(self, new_user_data):
        self.__id = new_user_data.__id
        self.__nonce = new_user_data.__nonce
        self.__phone = new_user_data.__phone
        self.set_name(*new_user_data.get_name())
        self.set_password(*new_user_data.get_password())

    def set_times(self, time: int):
        self.__sem_name.acquire()
        self.__name_time = time
        self.__sem_name.release()
        self.__sem_password.acquire()
        self.__password_time = time
        self.__sem_password.release()
