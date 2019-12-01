from logging import basicConfig, debug, error, info, DEBUG
from random import randint
from threading import Thread
from time import sleep
from socket import gethostbyname, gethostname, socket, AF_INET, SOCK_DGRAM
from rpyc import  connect, discover
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import UDPRegistryClient, UDPRegistryServer, DEFAULT_PRUNING_TIMEOUT
from .user_data import UserData
from .utils import get_hash
from .kademlia import Contact, KademliaService


class TrackerService(KademliaService):

    @staticmethod
    def log_table(ip, port=None):
        if isinstance(ip, tuple) and len(ip) > 1:
            ip, port = ip[0], ip[1]
        if port is None:
            raise Exception('Port is None.')
        conn = connect(ip, port)
        conn.root.client_table()

    @staticmethod
    def log_data(ip, port=None):
        if isinstance(ip, tuple) and len(ip) > 1:
            ip, port = ip[0], ip[1]
        if port is None:
            raise Exception('Port is None.')
        conn = connect(ip, port)
        conn.root.client_data()

    def exposed_client_table(self):
        result = ''
        for index, bucket in enumerate(self.table):
            if bucket:
                result += f'Bucket: {index}\n'
                for node in bucket:
                    result += f'{node}\n'
        with open(f'table_{self.my_contact.ip}_{self.my_contact.port}.log', 'w') as file:
            file.write(result)

    def exposed_client_data(self):
        result = ''
        for key in self.data:
            result += f'{key}:{self.data[key]}\n'
        with open(f'data_{self.my_contact.ip}_{self.my_contact.port}.log', 'w') as file:
            file.write(result)

    def exposed_store(self, client: Contact, client_lamport: int, key: int, value: str) -> bool:
        debug(f'TrackerService.exposed_store - Trying to store value in key: {key} with value: {value}.')
        if not self.is_initialized:
            error(f'TrackerService.exposed_store - Instance not initialized')
            return False, self.lamport
        client = Contact.from_json(client)
        value = UserData.from_json(value)
        debug(f'TrackerService.exposed_store - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        if key in self.data:
            debug(f'TrackerService.exposed_store - {key} in data')
            debug(f'TrackerService.exposed_store - Acquire lock for data')
            self.data_lock.acquire()
            debug(f'TrackerService.exposed_store - Updating value')
            self.data[key].update(value)
            self.data_lock.release()
            debug(f'TrackerService.exposed_store - Release lock for data')
        else:
            debug(f'TrackerService.exposed_store - Not {key} in data')
            debug(f'TrackerService.exposed_store - Acquire lock for data')
            self.data_lock.acquire()
            self.data[key] = value
            self.data_lock.release()
            debug(f'TrackerService.exposed_store - Release lock for data')
        debug(f'TrackerService.exposed_store - End of connection from {client}.')
        return True, self.lamport

    def exposed_client_store(self, key: int, value: str, use_self_time: bool = True) -> bool:
        if not self.is_initialized:
            error(f'TrackerService.exposed_client_store - Instance not initialized')
            return None
        debug('TrackerService.exposed_client_store - Starting the queue')
        queue = Queue()
        debug('TrackerService.exposed_client_store - Starting the visited nodes set')
        visited = set()
        debug('TrackerService.exposed_client_store - Starting the KClosestNode array')
        top_contacts = KContactSortedArray(self.k, key)
        debug('TrackerService.exposed_client_store - Starting the semaphore for the queue')
        queue_lock = Semaphore()
        debug(f'TrackerService.exposed_client_store - Insert self contact: {self.my_contact} to the queue')
        queue.put(self.my_contact)
        debug(f'TrackerService.exposed_client_store - Insert self contact: {self.my_contact} to the visited nodes set')
        visited.add(self.my_contact)
        debug(f'TrackerService.exposed_client_store - Insert self contact: {self.my_contact} to the KClosestNode array')
        top_contacts.push(self.my_contact)
        debug(f'TrackerService.exposed_client_store - Starting the iteration on contacts more closes to key: {key}')
        for contact in self.table.get_closest_buckets(key):
            debug(f'TrackerService.exposed_client_store - Insert the contact: {contact} to the queue')
            queue.put(contact)
            debug(f'TrackerService.exposed_client_store - Insert the contact: {contact} to the visited nodes set')
            visited.add(contact)
            debug(f'TrackerService.exposed_client_store - Insert the contact: {contact} to the KClosestNode array')
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                debug('TrackerService.exposed_client_store -  Initial alpha nodes completed')
                break
        debug('TrackerService.exposed_client_store - Starting the ThreadManager')
        manager = ThreadManager(self.a, queue.qsize, self.store_lookup, args=(key, queue, top_contacts, visited, queue_lock))
        manager.start()
        success = False
        value = UserData.from_json(value)
        if use_self_time:
            value.set_times(self.lamport)
        debug(f'TrackerService.exposed_client_store - Iterate the closest K nodes to find the key: {key}')
        for contact in top_contacts:
            debug(f'TrackerService.exposed_client_store - Storing key: {key} with value: {value} in contact: {contact}')
            result, _ = self.store_to(contact, key, value)
            if not result:
                error(f'TrackerService.exposed_client_store - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
            success = success or result
        debug(f'TrackerService.exposed_client_store - Finish method with result: {success}')
        return success
    @staticmethod
    def __start_register():
        while True:
            server = None
            try:
                debug('TrackerService.__start_register - Starting the registration server')
                server = UDPRegistryServer(pruning_timeout=DEFAULT_PRUNING_TIMEOUT)
                server.start()
                break
            except Exception as e:
                error(f'TrackerService.__start_register - Error starting server to register, sleeping 5 seconds and trying again. Exception: {e}')
                if not server is None:
                    server.close()
                sleep(5)

    @staticmethod
    def __start_service(port: int):
        while True:
            server = None
            try:
                debug('TrackerService.__start_service - Creating instace of service')
                service = TrackerService(3, 160, 3)
                debug('TrackerService.__start_service - Creating instace of ThreadedServer')
                server = ThreadedServer(service, port=port, registrar=UDPRegistryClient(), protocol_config={'allow_public_attrs': True})
                debug('TrackerService.__start_service - Starting the service')
                server.start()
                break
            except Exception as e:
                error('TrackerService.__start_service - Error starting service, sleeping 5 seconds and trying again')
                error(e)
                if not server is None:
                    server.close()
                sleep(0.2)

    @staticmethod
    def start(port_random=False, log_to_file=True, inf_port=8000, sup_port=9000):
        port = 8081
        if port_random:
            port = randint(inf_port, sup_port)
        if log_to_file:
            basicConfig(filename=f'logs/system_{port}.log', filemode='w', format='%(asctime)s - %(levelname)s - %(name)s: %(message)s', level=DEBUG)
        else:
            basicConfig(format='%(asctime)s - %(levelname)s - %(name)s: %(message)s', level=DEBUG)
        debug(f'TrackerService.start - Generated port: {port}')
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
                sleep(0.2)
            except Exception as e:
                error(f'TrackerService.start - Exception: {e}')
                error('TrackerService.start - Error doing JOIN, wait 5 seconds and try again')
                sleep(0.2)
        info('TrackerService.start - Server started successfully')

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
                except Exception as e:
                    error(f'TrackerService.get_ip - Error connecting to node: {peer}. Exception: {e}')
                    sleep(0.1)
                    continue
                finally:
                    s.close()
        except Exception as e:
            error(f'TrackerService.get_ip - Obtaining IP from a socket locally because no node was discovered. Exception: {e}')
            ip = gethostbyname(gethostname()) # This should never happen if
        return ip
