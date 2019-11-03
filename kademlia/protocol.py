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
        self.my_contact = Contact.clone(my_contact)
        self.table = BucketTable(k, b, my_contact.hash)
        self.value_cloner = value_cloner

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_store(self, client:Contact, client_lamport:int, key:int, value:object) -> bool:
        client = Contact.clone(client)
        value = self.value_cloner(value)
        self.lamport = max(client_lamport, self.lamport + 1) 
        self.update_contact(client)
        self.data[key] = value
        return True

    def exposed_ping(self, client:Contact, client_lamport:int) -> bool:
        client = Contact.clone(client)
        self.lamport = max(client_lamport, self.lamport + 1)
        self.update_contact(client)
        return True

    def exposed_find_node(self, client:Contact, client_lamport:int, id:int) -> list:
        client = Contact.clone(client)
        self.lamport = max(client_lamport, self.lamport + 1)
        self.update_contact(client)
        return self.table.get_bucket(id).nodes

    def exposed_find_value(self, client:Contact, client_lamport:int, key:int) -> object:
        client = Contact.clone(client)
        self.lamport = max(client_lamport, self.lamport + 1)
        self.update_contact(client)
        try:
            return self.data[key]
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

    @try_function()
    def ping(self, ip, port):
        connection = connect(ip, str(port))
        connection.ping()
        self.lamport += 1
        connection.root.ping(self.my_contact, self.lamport)
        return connection
