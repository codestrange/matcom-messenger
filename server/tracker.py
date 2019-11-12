from logging import basicConfig, debug, error, info, DEBUG, BASIC_FORMAT
from random import randint
from threading import Thread
from time import sleep
from socket import gethostbyname, gethostname, socket, AF_INET, SOCK_DGRAM
from rpyc import  connect, discover
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import UDPRegistryClient, UDPRegistryServer, DEFAULT_PRUNING_TIMEOUT
from .utils import get_hash
from .kademlia import Contact, KademliaService


basicConfig(filename='system.log', filemode='w', format='%(asctime)s - %(levelname)s - %(name)s: %(message)s', level=DEBUG)


class TrackerService(KademliaService):
    @staticmethod
    def __start_register():
        while True:
            server = None
            try:
                debug('TrackerService.__start_register - Starting the registration server')
                server = UDPRegistryServer(pruning_timeout=DEFAULT_PRUNING_TIMEOUT)
                server.start()
                break
            except:
                error('TrackerService.__start_register - Error starting server to register, sleeping 5 seconds and trying again')
                if not server is None:
                    server.close()
                sleep(5)

    @staticmethod
    def __start_service(port:int):
        while True:
            server = None
            try:
                debug('TrackerService.__start_service - Creating instace of service')
                service = TrackerService(3, 160, 3, None)
                debug('TrackerService.__start_service - Creating instace of ThreadedServer')
                server = ThreadedServer(service, port=port, registrar=UDPRegistryClient(), protocol_config={ 'allow_public_attrs': True})
                debug('TrackerService.__start_service - Starting the service')
                server.start()
                break
            except Exception as e:
                error('TrackerService.__start_service - Error starting service, sleeping 5 seconds and trying again')
                error(e)
                if not server is None:
                    server.close()
                sleep(5)

    @staticmethod
    def start(port_random=False):
        port = 8081
        if port_random:
            debug('TrackerService.start - Randomly generating port between 8000 and 9000')
            port = randint(8000, 9000)
        debug(f'TrackerService.start - Randomly generated port: {port}')
        debug('TrackerService.start - Starting a thread for the registration server')
        thread_register = Thread(target=TrackerService.__start_register)
        thread_register.start()
        debug('TrackerService.start - Sleeping 3 seconds for the server to register to start')
        sleep(3)
        debug('TrackerService.start - Starting a thread for the service')
        thread_service = Thread(target=TrackerService.__start_service, args=(port, ))
        thread_service.start()
        debug('TrackerService.start - Sleeping 3 seconds for the service to start')
        sleep(3)
        debug('TrackerService.start - Getting ip')
        ip = TrackerService.get_ip()
        debug(f'TrackerService.start - IP obtained: {ip}')
        debug(f'TrackerService.start - Calculating the id of the node through its address: {ip}:{port}')
        hash_id = TrackerService.get_id_hash(f"{ip}:{port}")
        debug(f'TrackerService.start - Id generated with the SHA1 hash function: {hash_id}')
        contact = Contact(hash_id, ip, port)
        while True:
            try:
                debug('TrackerService.start - Trying to connect to the service to start the JOIN')
                conn = connect(ip, port, config={'sync_request_timeout': 1000000})
                debug('TrackerService.start - Pinging the service')
                conn.ping()
                debug('TrackerService.start - Executing the remote connect to network method in the service')
                result = conn.root.connect_to_network(contact.to_json())
                debug(f'TrackerService.start - Finish connect to network with result = {result}')
                if result:
                    break
                error('TrackerService.start - Error doing JOIN, wait 5 seconds and try again')
                sleep(5)
            except Exception as e:
                error(f'TrackerService.start - Exception: {e}')
                error('TrackerService.start - Error doing JOIN, wait 5 seconds and try again')
                sleep(5)
        info('TrackerService.start - Server start successfully')

    @staticmethod
    def get_id_hash(id: str) -> int:
        debug(f'TrackerService.get_id_hash - Calculating hash from: {id}')
        return get_hash(id)

    @staticmethod
    def get_ip() -> str:
        ip = '0.0.0.0'
        try:
            debug('TrackerService.get_ip - Discovering nodes to establish a connection to obtain the IP')
            peers = discover(TrackerService.get_name(TrackerService))
            debug(f'TrackerService.get_ip - Nodes discovered to obtain IP: {peers}')
            for peer in peers:
                s = socket(AF_INET, SOCK_DGRAM)
                try:
                    debug(f'TrackerService.get_ip - Attempting to connect to the node: {peer}')
                    s.connect(peer)
                    ip = s.getsockname()[0]
                except:
                    error(f'TrackerService.get_ip - Error connecting to node: {peer}')
                    sleep(3)
                    continue
                finally:
                    s.close()
        except:
            error('TrackerService.get_ip - Obtaining IP from a socket locally because no node was discovered')
            ip = gethostbyname(gethostname()) # This should never happen if 
        return ip