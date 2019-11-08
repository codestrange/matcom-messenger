from threading import Semaphore
from rpyc import connect, Connection, Service
from .bucket import Bucket
from .bucket_table import BucketTable
from .contact import Contact
from .utils import try_function


class ProtocolService(Service):
    def __init__(self, my_contact:Contact, k:int, b:int, value_cloner):
        super(ProtocolService, self).__init__()
        self.data = {}
        self.lamport = 0
        self.lamport_lock = Semaphore()
        self.my_contact = Contact.clone(my_contact)
        self.table = BucketTable(k, b, my_contact.hash)
        self.value_cloner = value_cloner

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_store(self, client:Contact, client_lamport:int, key:int, value:object, store_time:int) -> bool:
        client = Contact.clone(client)
        value = self.value_cloner(value)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            actual_value, actual_time = self.data[key]
        except KeyError:
            actual_value, actual_time = (value, store_time)
        self.data[key] = (value, store_time) if store_time > actual_time else (actual_value, actual_time)
        return True

    def exposed_ping(self, client:Contact, client_lamport:int) -> bool:
        client = Contact.clone(client)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        return self.my_contact

    def exposed_find_node(self, client:Contact, client_lamport:int, id:int) -> list:
        client = Contact.clone(client)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        return self.table.get_bucket(id).nodes

    def exposed_find_value(self, client:Contact, client_lamport:int, key:int) -> object:
        client = Contact.clone(client)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            value, stored_time = self.data[key]
            return value, stored_time
        except KeyError:
            return None

    def update_contact(self, contact:Contact):
        if not self.table.update(contact):
            bucket = self.table.get_bucket(contact.hash)
            to_remove = None
            bucket.semaphore.acquire()
            for stored in bucket:
                if not self.ping(stored.ip, stored.port)[0]:
                    to_remove = stored
            if to_remove:
                bucket.remove_by_contact(to_remove)
                bucket.update(contact)
            bucket.semaphore.release()

    def update_lamport(self, client_lamport:int=0):
        self.lamport_lock.acquire()
        self.lamport = max(client_lamport, self.lamport + 1)
        self.lamport_lock.release()

    def connect(self, contact: Contact) -> Connection:
        self.update_lamport()
        connection = connect(contact.ip, str(contact.port))
        connection.ping()
        return connection

    @try_function()
    def ping(self, contact:Contact) -> bool:
        connection = self.connect(contact)
        return connection.root.ping(self.my_contact, self.lamport)

    @try_function()
    def store(self, contact:Contact, key:int, value:object, store_time:int) -> bool:
        connection = self.connect(contact)
        return connection.root.store(self.my_contact, self.lamport, key, value, store_time)

    @try_function()
    def find_node(self, contact:Contact, id:int) -> list:
        connection = self.connect(contact)
        return connection.root.find_node(self.my_contact, self.lamport, id)

    @try_function()
    def find_value(self, contact:Contact, key:int) -> object:
        connection = self.connect(contact)
        return connection.root.find_value(self.my_contact, self.lamport, key)
