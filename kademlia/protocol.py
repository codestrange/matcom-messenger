from rpyc import Connection, Service
from bucket import Bucket
from bucket_table import BucketTable


class ProtocolService(Service):
    def __init__(self, id:int):
        # Invoked parent init
        self.data = {}
        self.id = id
        self.table = BucketTable()

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_store(self, client:Contact, key:int, value:object) -> bool:
        self.update_contact(client)
        self.data[key] = values
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

    def update_contact(contact:Contact):
        pass
