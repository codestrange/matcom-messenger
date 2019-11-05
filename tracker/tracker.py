from random import randint
from threading import Thread
from time import sleep
from rpyc import connect
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import UDPRegistryClient, UDPRegistryServer, DEFAULT_PRUNING_TIMEOUT
from ..kademlia import Contact, KademliaService


class TrackerService(KademliaService):
    @staticmethod
    def __start_register():
        while True:
            server = UDPRegistryServer(pruning_timeout=DEFAULT_PRUNING_TIMEOUT)
            try:
                server.start()
                break
            except:
                sleep(5)

    @staticmethod
    def __start_service(contact:Contact):
        while True:
            try:
                service = TrackerService(contact, 3, 126, 3, None)
                server = ThreadedServer(service, port=port, registrar=UDPRegistryClient(), protocol_config={ 'allow_public_attrs': True})
                server.start()
                break
            except:
                sleep(5)

    @staticmethod
    def start():
        id = TrackerService.get_id()
        ip = TrackerService.get_ip()
        port = randint(8000, 9000)
        contact = Contact(id, ip, port)
        thread_register = Thread(target=TrackerService.__start_register)
        thread_service = Thread(target=TrackerService.__start_service, args=(contact, ))
        thread_register.start()
        thread_service.start()
        while True:
            try:
                conn = connect(contact.ip, contact.port)
                conn.ping()
                result = conn.root.connect_to_network()
                if result:
                    break
            except:
                sleep(5)

    @staticmethod
    def get_id() -> str:
        pass

    @staticmethod
    def get_ip() -> str:
        pass
