from logging import basicConfig, debug, error, exception, DEBUG, BASIC_FORMAT
from random import randint
from threading import Thread
from time import sleep
from socket import gethostbyname, gethostname, socket, AF_INET, SOCK_DGRAM
from rpyc import  connect, discover
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import UDPRegistryClient, UDPRegistryServer, DEFAULT_PRUNING_TIMEOUT
from .utils import get_hash
from .kademlia import Contact, KademliaService


basicConfig(format='%(levelname)s - %(name)s: %(message)s', level=DEBUG)


class TrackerService(KademliaService):
    @staticmethod
    def __start_register():
        while True:
            server = None
            try:
                debug('Starting the registration server')
                server = UDPRegistryServer(pruning_timeout=DEFAULT_PRUNING_TIMEOUT)
                server.start()
                break
            except:
                error('Error starting server to register, sleeping 5 seconds and trying again')
                if not server is None:
                    server.close()
                sleep(5)

    @staticmethod
    def __start_service(contact:Contact):
        while True:
            server = None
            try:
                debug('Creating instace of service')
                service = TrackerService(contact, 3, 126, 3, None)
                debug('Creating instace of ThreadedServer')
                server = ThreadedServer(service, port=contact.port, registrar=UDPRegistryClient(), protocol_config={ 'allow_public_attrs': True})
                debug('Starting the service')
                server.start()
                break
            except Exception as e:
                error('Error starting service, sleeping 5 seconds and trying again')
                exception(e)
                if not server is None:
                    server.close()
                sleep(5)

    @staticmethod
    def start():
        debug('Starting a thread for the registration server')
        thread_register = Thread(target=TrackerService.__start_register)
        thread_register.start()
        debug('Sleeping 3 seconds for the server to register to start')
        sleep(3)
        debug('Getting ip')
        ip = TrackerService.get_ip()
        debug(f'IP obtained: {ip}')
        debug('Randomly generating port between 8000 and 9000')
        port = randint(8000, 9000)
        debug(f'Randomly generated port: {port}')
        debug(f'Calculating the id of the node through its address: {ip}:{port}')
        id = TrackerService.get_id_hash(f"{ip}:{port}")
        debug(f'Id generated with the SHA1 hash function: {id}')
        debug('Creating node contact')
        contact = Contact(id, ip, port)
        debug(f'Generated contact: {contact}')
        debug('Starting a thread for the service')
        thread_service = Thread(target=TrackerService.__start_service, args=(contact, ))
        thread_service.start()
        debug('Sleeping 3 seconds for the service to start')
        sleep(3)
        while True:
            try:
                debug('Trying to connect to the service to start the JOIN')
                conn = connect(contact.ip, contact.port)
                debug('Pinging the service')
                conn.ping()
                debug('Executing the remote connect to network method in the service')
                result = conn.root.connect_to_network()
                if result:
                    break
                error('Error doing JOIN, wait 5 seconds and try again')
                sleep(5)
            except:
                error('Error doing JOIN, wait 5 seconds and try again')
                sleep(5)

    @staticmethod
    def get_id_hash(id: str) -> int:
        debug(f'Calculating hash from: {id}')
        return get_hash(id)

    @staticmethod
    def get_ip() -> str:
        ip = '0.0.0.0'
        try:
            debug('Discovering nodes to establish a connection to obtain the IP')
            peers = discover(TrackerService.ALIASES[0])
            debug(f'Nodes discovered to obtain IP: {peers}')
            for peer in peers:
                s = socket(AF_INET, SOCK_DGRAM)
                try:
                    debug(f'Attempting to connect to the node: {peer}')
                    s.connect(peer)
                    ip = s.getsockname()[0]
                except:
                    error(f'Error connecting to node: {peer}')
                    sleep(3)
                    continue
                finally:
                    s.close()
        except:
            error('Obtaining IP from a socket locally because no node was discovered')
            ip = gethostbyname(gethostname()) # This should never happen if 
        return ip
