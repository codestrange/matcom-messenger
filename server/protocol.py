from logging import debug, error
from threading import Semaphore
from rpyc import connect, Connection, Service
from .bucket import Bucket
from .bucket_table import BucketTable
from .contact import Contact
from .utils import try_function


class ProtocolService(Service):
    def __init__(self, k:int, b:int, value_cloner):
        super(ProtocolService, self).__init__()
        debug(f'ProtocolService.__init__ - Executing the constructor with k: {k} y b: {b}')
        self.data = {}
        self.lamport = 0
        self.lamport_lock = Semaphore()
        self.value_cloner = value_cloner
        self.k = k
        self.b = b
        self.is_initialized = False
        self.is_initialized_lock = Semaphore()

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_init(self, contact:Contact):
        if self.is_initialized:
            return True
        self.my_contact = Contact.from_json(contact)
        debug(f'ProtocolService.exposed_init - Executing the init with the contact: {self.my_contact}')
        self.table = BucketTable(self.k, self.b, self.my_contact.id)
        self.is_initialized = True
        return True

    def exposed_store(self, client:Contact, client_lamport:int, key:int, value:object, store_time:int) -> bool:
        if not self.is_initialized:
            error(f'ProtocolService.exposed_store - Instance not initialized')
            return False
        client = Contact.from_json(client)
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
        if not self.is_initialized:
            error(f'ProtocolService.exposed_ping - Instance not initialized')
            return None
        client = Contact.from_json(client)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        return self.my_contact.to_json()

    def exposed_find_node(self, client:Contact, client_lamport:int, id:int) -> list:
        if not self.is_initialized:
            error(f'ProtocolService.exposed_find_node - Instance not initialized')
            return None
        client = Contact.from_json(client)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        result = []
        count = 0
        for contact in list(self.table.get_closest_buckets(id)):
            result.append(contact)
            count += 1
            if count >= self.k:
                break
        return result

    def exposed_find_value(self, client:Contact, client_lamport:int, key:int) -> object:
        if not self.is_initialized:
            error(f'ProtocolService.exposed_find_value - Instance not initialized')
            return None
        client = Contact.from_json(client)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            value, stored_time = self.data[key]
            return value, stored_time
        except KeyError:
            return None

    def update_contact(self, contact:Contact):
        if not self.table.update(contact):
            bucket = self.table.get_bucket(contact.id)
            to_remove = None
            bucket.semaphore.acquire()
            for stored in bucket:
                if not self.ping_to(stored.ip, stored.port)[0]:
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
    def ping_to(self, contact:Contact) -> bool:
        connection = self.connect(contact)
        return connection.root.ping(self.my_contact.to_json(), self.lamport)

    @try_function()
    def store_to(self, contact:Contact, key:int, value:object, store_time:int) -> bool:
        connection = self.connect(contact)
        return connection.root.store(self.my_contact.to_json(), self.lamport, key, value, store_time)

    @try_function()
    def find_node_to(self, contact:Contact, id:int) -> list:
        connection = self.connect(contact)
        return connection.root.find_node(self.my_contact.to_json(), self.lamport, id)

    @try_function()
    def find_value_to(self, contact:Contact, key:int) -> object:
        connection = self.connect(contact)
        return connection.root.find_value(self.my_contact.to_json(), self.lamport, key)
