import socket
from random import randint
from threading import Thread
from time import sleep
from rpyc import  connect, discover
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import UDPRegistryClient, UDPRegistryServer, DEFAULT_PRUNING_TIMEOUT
from .utils import get_hash
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
        ip = TrackerService.get_ip()
        port = randint(8000, 9000)
        id = TrackerService.get_id_hash(f"{ip}:{port}")
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
    def get_id_hash(id: str) -> int:
        return get_hash(id)

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
