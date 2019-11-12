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
        debug(f'ProtocolService.exposed_init - Initializing with contact: {contact}.')
        debug(f'ProtocolService.exposed_init - Executing the init with the contact: {self.my_contact}')
        self.table = BucketTable(self.k, self.b, self.my_contact.id)
        self.is_initialized = True
        debug(f'ProtocolService.exposed_init - End initializing with contact: {contact}.')
        return True

    def exposed_store(self, client:Contact, client_lamport:int, key:int, value:object, store_time:int) -> bool:
        debug(f'ProtocolService.exposed_store - Trying to store value in key: {key} at time: {store_time}.')
        if not self.is_initialized:
            error(f'ProtocolService.exposed_store - Instance not initialized')
            return False
        client = Contact.from_json(client)
        debug(f'ProtocolService.exposed_store - Incoming connection from {client}.')
        value = self.value_cloner(value)
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            actual_value, actual_time = self.data[key]
        except KeyError:
            actual_value, actual_time = (value, store_time)
        self.data[key] = (value, store_time) if store_time > actual_time else (actual_value, actual_time)
        debug(f'ProtocolService.exposed_store - End of connection from {client}.')
        return True

    def exposed_ping(self, client:Contact, client_lamport:int) -> bool:
        if not self.is_initialized:
            error(f'ProtocolService.exposed_ping - Instance not initialized')
            return None
        client = Contact.from_json(client)
        debug(f'ProtocolService.exposed_ping - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        debug(f'ProtocolService.exposed_ping - End of connection from {client}.')
        return self.my_contact.to_json()

    def exposed_find_node(self, client:Contact, client_lamport:int, id:int) -> list:
        if not self.is_initialized:
            error(f'ProtocolService.exposed_find_node - Instance not initialized')
            return None
        client = Contact.from_json(client)
        debug(f'ProtocolService.exposed_find_node - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        result = []
        count = 0
        for contact in list(self.table.get_closest_buckets(id)):
            result.append(contact.to_json())
            count += 1
            if count >= self.k:
                break
        debug(f'ProtocolService.exposed_find_node - Replaying with {result}.')
        debug(f'ProtocolService.exposed_find_node - End of connection from {client}.')
        return result

    def exposed_find_value(self, client:Contact, client_lamport:int, key:int) -> object:
        if not self.is_initialized:
            error(f'ProtocolService.exposed_find_value - Instance not initialized')
            return None
        client = Contact.from_json(client)
        debug(f'ProtocolService.exposed_find_value - Incoming connection from {client}.')
        debug(f'ProtocolService.exposed_find_value - Asking for key: {key}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            value, stored_time = self.data[key]
            debug(f'ProtocolService.exposed_find_value - Replaying with value: {value} and value_time: {stored_time}.')
            debug(f'ProtocolService.exposed_find_value - Incoming connection from {client}.')
            return value, stored_time
        except KeyError:
            debug(f'ProtocolService.exposed_find_value - Value not founded.')
            debug(f'ProtocolService.exposed_find_value - Incoming connection from {client}.')
            return None

    def update_contact(self, contact:Contact):
        debug(f'ProtocolService.update_contact - Updating contact: {contact}.')
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
        debug(f'ProtocolService.update_contact - Contact updated.')

    def update_lamport(self, client_lamport:int=0):
        debug(f'ProtocolService.update_lamport - Updating actual time with time: {client_lamport}.')
        self.lamport_lock.acquire()
        self.lamport = max(client_lamport, self.lamport + 1)
        self.lamport_lock.release()
        debug(f'ProtocolService.update_lamport - Time updated.')

    def connect(self, contact: Contact) -> Connection:
        debug(f'Protocol.connect - Trying to connect with contact: {contact}.')
        self.update_lamport()
        connection = connect(contact.ip, str(contact.port))
        connection.ping()
        debug(f'ProtocolService.Protocol.connect - Connection with contact: {contact} stablished.')
        return connection

    @try_function()
    def ping_to(self, contact:Contact) -> bool:
        debug(f'ProtocolService.ping_to - Trying ping to contact: {contact}.')
        connection = self.connect(contact)
        return connection.root.ping(self.my_contact.to_json(), self.lamport)

    @try_function()
    def store_to(self, contact:Contact, key:int, value:object, store_time:int) -> bool:
        debug(f'ProtocolService.store_to - Trying store to contact: {contact} for key: {key}.')
        connection = self.connect(contact)
        return connection.root.store(self.my_contact.to_json(), self.lamport, key, value, store_time)

    @try_function()
    def find_node_to(self, contact:Contact, id:int) -> list:
        debug(f'ProtocolService.find_node_to - Trying find_node to contact: {contact} for id: {id}')
        connection = self.connect(contact)
        return connection.root.find_node(self.my_contact.to_json(), self.lamport, id)

    @try_function()
    def find_value_to(self, contact:Contact, key:int) -> object:
        debug(f'ProtocolService.find_node_to - Trying find_value to contact: {contact} for key: {key}')
        connection = self.connect(contact)
        return connection.root.find_value(self.my_contact.to_json(), self.lamport, key)
