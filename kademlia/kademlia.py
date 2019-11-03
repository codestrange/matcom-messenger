from rpyc import Connection
from .contact import 
from .protocol import ProtocolService


class KademliaService(ProtocolService):
    def __init__(self, id:int, k: int, b:int, a:int)
        super(KademliaService, self).__init__(id, k, b)

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_client_store(self, key:int, value:object) -> bool:
        pass

    def exposed_client_find_node(self, id:int) -> list:
        pass

    def exposed_client_find_value(self, key:int) -> object:
        pass
