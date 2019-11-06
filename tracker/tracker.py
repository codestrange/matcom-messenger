import socket
from random import randint
from threading import Thread
from time import sleep
from rpyc import discover
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import UDPRegistryClient, UDPRegistryServer, DEFAULT_PRUNING_TIMEOUT
from .utils import get_hash
from ..kademlia.kademlia import Contact, KademliaService


class TrackerService(KademliaService):

    @staticmethod
    def __start_register():
        while True:
            server = UDPRegistryServer(pruning_timeout=DEFAULT_PRUNING_TIMEOUT)
            try:
                server.start()
                break
            except:
                sleep(10)

    @staticmethod
    def __start_service():
        while True:
            try:
                id = TrackerService.get_id()
                ip = TrackerService.get_ip()
                port = randint(8000, 9000)
                contact = Contact(id, ip, port)
                service = TrackerService(contact, 3, 126, 3, None)
                server = ThreadedServer(service, port=port, registrar=UDPRegistryClient(), protocol_config={ 'allow_public_attrs': True})
                server.start()
                break
            except:
                sleep(10)

    @staticmethod
    def get_id() -> str:
        return get_hash(':'.join([TrackerService.get_ip()]))

    @staticmethod
    def get_ip() -> str:
        ip = '0.0.0.0'
        try:
            peers = discover(TrackerService.ALIASES[0])
            for peer in peers:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    s.connect(peer)
                    ip = s.getsockname()[0]
                except:
                    continue
                finally:
                    s.close()
        except:
            ip = socket.gethostbyname(socket.gethostname()) # This should never happen if 
        return ip
