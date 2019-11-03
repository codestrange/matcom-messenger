from rpyc import connect, Connection, Service
from .bucket import Bucket
from .bucket_table import BucketTable
from .contact import Contact


class ProtocolService(Service):
    def __init__(self, id:int, k:int, b:int):
        super(ProtocolService, self).__init__()
        self.data = {}
        self.id = id
        self.table = BucketTable(k, b, id)

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_store(self, client:Contact, key:int, value:object) -> bool:
        self.update_contact(client)
        self.data[key] = value
        return True

    def exposed_ping(self, client:Contact) -> bool:
        self.update_contact(client)
        return True

    def exposed_find_node(self, client:Contact, id:int) -> Bucket:
        self.update_contact(client)
        return self.table.get_bucket(id)

    def exposed_find_value(self, client:Contact, key:int) -> object:
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
                try:
                    connection = connect(stored.ip, str(stored.port))
                    connection.ping()
                except:
                    to_remove = stored
                    break
            if to_remove:
                bucket.remove_by_contact(to_remove)
                bucket.update(contact)
            bucket.semaphore.release()
