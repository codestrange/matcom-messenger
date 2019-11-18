from logging import debug, error
from queue import Empty, Queue
from random import randint
from threading import Semaphore
from time import sleep
from rpyc import connect, Connection, discover
from rpyc.utils.factory import DiscoveryError
from .contact import Contact
from .protocol import ProtocolService
from .contact import Contact
from .utils import KContactSortedArray, ThreadManager, try_function


class KademliaService(ProtocolService):
    def __init__(self, k: int, b:int, a:int):
        super(KademliaService, self).__init__(k, b)
        debug(f'KademliaService.exposed_init - Executing the init with the k: {k},b: {b} and a: {a}')
        self.a = a
        self.is_started_node = False

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_client_store(self, key:int, value:str) -> bool:
        if not self.is_initialized:
            error(f'KademliaService.exposed_client_store - Instance not initialized')
            return None
        debug('KademliaService.exposed_client_store - Starting the queue')
        queue = Queue()
        debug('KademliaService.exposed_client_store - Starting the visited nodes set')
        visited = set()
        debug('KademliaService.exposed_client_store - Starting the KClosestNode array')
        top_contacts = KContactSortedArray(self.k, key)
        debug('KademliaService.exposed_client_store - Starting the semaphore for the queue')
        queue_lock = Semaphore()
        debug(f'KademliaService.exposed_client_store - Starting the iteration on contacts more closes to key: {key}')
        for contact in self.table.get_closest_buckets(key):
            debug(f'KademliaService.exposed_client_store - Insert the contact: {contact} to the queue')
            queue.put(contact)
            debug(f'KademliaService.exposed_client_store - Insert the contact: {contact} to the visited nodes set')
            visited.add(contact)
            debug(f'KademliaService.exposed_client_store - Insert the contact: {contact} to the KClosestNode array')
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                debug('KademliaService.exposed_client_store -  Initial alpha nodes completed')
                break
        debug('KademliaService.exposed_client_store - Starting the ThreadManager')
        manager = ThreadManager(self.a, queue.qsize, self.store_lookup, args=(key, queue, top_contacts, visited, queue_lock))
        manager.start()
        success = False
        debug(f'KademliaService.exposed_client_store - Iterate the closest K nodes to find the key: {key}')
        for contact in top_contacts:
            debug(f'KademliaService.exposed_client_store - Storing key: {key} with value: {value} in contact: {contact}')
            result, _ = self.store_to(contact, key, value, self.lamport)
            if not result:
                error(f'KademliaService.exposed_client_store - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
            success = success or result
        debug(f'KademliaService.exposed_client_store - Finish method with result: {success}')
        return success

    def store_lookup(self, key:int, queue:Queue, top:KContactSortedArray, visited:set, queue_lock:Semaphore):
        contact = None
        try:
            debug(f'KademliaService.store_lookup - Removing a contact from the queue')
            contact = queue.get(timeout=1)
            debug(f'KademliaService.store_lookup - Contact {contact} out of the queue')
        except Empty:
            debug(f'KademliaService.store_lookup - Empty queue')
            return
        debug(f'KademliaService.store_lookup - Make the find_node on the contact: {contact}')
        result, new_contacts = self.find_node_to(contact, key)
        if not result:
            debug(f'KademliaService.store_lookup - No connection to the node: {contact} was established')
            return
        debug(f'KademliaService.store_lookup - Update the table with contact: {contact}')
        self.table.update(contact)
        debug(f'KademliaService.store_lookup - Cloning contacts received')
        new_contacts = map(Contact.from_json, new_contacts)
        debug(f'KademliaService.store_lookup - Iterate by contacts')
        for new_contact in new_contacts:
            debug(f'KademliaService.store_lookup - Pinging to contact: {new_contact}')
            if not self.ping_to(new_contact)[0]:
                debug(f'KademliaService.store_lookup - The contact: {new_contact} not respond')
                continue
            debug(f'KademliaService.store_lookup - Update the table with contact: {new_contact}')
            self.table.update(new_contact)
            debug(f'KademliaService.store_lookup - Lock the queue')
            queue_lock.acquire()
            if not new_contact in visited:
                debug(f'KademliaService.store_lookup - The contact: {new_contact} is NOT in the queue')
                debug(f'KademliaService.store_lookup - Inserting the contact: {new_contact} to the queue and KClosestNode array and marking as visited')
                visited.add(new_contact)
                queue_lock.release()
                queue.put(new_contact)
                top.push(new_contact)
            else:
                debug(f'KademliaService.store_lookup - The contact: {new_contact} is in the queue')
                queue_lock.release()

    def exposed_client_find_node(self, id:int) -> list:
        if not self.is_initialized:
            error(f'KademliaService.exposed_client_find_node - Instance not initialized')
            return None
        if id == self.my_contact.id:
            debug(f'KademliaService.exposed_client_find_node - This node is the node finded.')
            debug(f'KademliaService.exposed_client_find_node - The node with id was found: {id}, the node is {self.my_contact}')
            return self.my_contact.to_json()
        debug('KademliaService.exposed_client_find_node - Starting the queue')
        queue = Queue()
        debug('KademliaService.exposed_client_find_node - Starting the visited nodes set')
        visited = set()
        debug('KademliaService.exposed_client_find_node - Starting the KClosestNode array')
        top_contacts = KContactSortedArray(self.k, id)
        debug('KademliaService.exposed_client_find_node - Starting the semaphore for the queue')
        queue_lock = Semaphore()
        debug(f'KademliaService.exposed_client_find_node - Starting the iteration on contacts more closes to id: {id}')
        for contact in self.table.get_closest_buckets(id):
            debug(f'KademliaService.exposed_client_find_node - Insert the contact: {contact} to the queue')
            queue.put(contact)
            debug(f'KademliaService.exposed_client_find_node - Insert the contact: {contact} to the visited nodes set')
            visited.add(contact)
            debug(f'KademliaService.exposed_client_find_node - Insert the contact: {contact} to the KClosestNode array')
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                debug('KademliaService.exposed_client_find_node -  Initial alpha nodes completed')
                break
        debug('KademliaService.exposed_client_find_node - Starting the ThreadManager')
        manager = ThreadManager(self.a, queue.qsize, self.find_node_lookup, args=(id, queue, top_contacts, visited, queue_lock))
        manager.start()
        debug(f'KademliaService.exposed_client_find_node - Iterate the closest K nodes to find the node: {id}')
        for contact in top_contacts:
            if contact.id == id:
                debug(f'KademliaService.exposed_client_find_node - The node with id was found: {id}, the node is {contact}')
                return contact.to_json()
        debug('KademliaService.exposed_client_find_node - Finish method without finded node')
        return None

    def find_node_lookup(self, id:int, queue:Queue, top:KContactSortedArray, visited:set, queue_lock:Semaphore):
        contact = None
        try:
            debug(f'KademliaService.find_node_lookup - Removing a contact from the queue')
            contact = queue.get(timeout=1)
            debug(f'KademliaService.find_node_lookup - Contact {contact} out of the queue')
        except Empty:
            debug(f'KademliaService.find_node_lookup - Empty queue')
            return
        debug(f'KademliaService.find_node_lookup - Make the find_node on the contact: {contact}')
        result, new_contacts = self.find_node_to(contact, id)
        if not result:
            debug(f'KademliaService.find_node_lookup - No connection to the node: {contact} was established')
            return
        debug(f'KademliaService.find_node_lookup - Update the table with contact: {contact}')
        self.table.update(contact)
        debug(f'KademliaService.find_node_lookup - Cloning contacts received')
        new_contacts = map(Contact.from_json, new_contacts)
        debug(f'KademliaService.find_node_lookup - Iterate by contacts')
        for new_contact in new_contacts:
            if new_contact == self.my_contact:
                debug(f'KademliaService.find_node_lookup - The new_contact is equal to the my_contact, continue.')
                continue
            debug(f'KademliaService.find_node_lookup - Pinging to contact: {new_contact}')
            if not self.ping_to(new_contact)[0]:
                debug(f'KademliaService.find_node_lookup - The contact: {new_contact} not respond')
                continue
            debug(f'KademliaService.find_node_lookup - Update the table with contact: {new_contact}')
            self.table.update(new_contact)
            debug(f'KademliaService.find_node_lookup - Lock the queue')
            queue_lock.acquire()
            if not new_contact in visited:
                debug(f'KademliaService.find_node_lookup - The contact: {new_contact} is NOT in the queue')
                debug(f'KademliaService.find_node_lookup - Inserting the contact: {new_contact} to the queue and KClosestNode array and marking as visited')
                visited.add(new_contact)
                queue_lock.release()
                queue.put(new_contact)
                top.push(new_contact)
            else:
                debug(f'KademliaService.find_node_lookup - The contact: {new_contact} is in the queue')
                queue_lock.release()

    def exposed_client_find_value(self, key:int) -> object:
        if not self.is_initialized:
            error(f'KademliaService.exposed_client_find_value - Instance not initialized')
            return None
        debug('KademliaService.exposed_client_find_value - Starting the queue')
        queue = Queue()
        debug('KademliaService.exposed_client_find_value - Starting the visited nodes set')
        visited = set()
        debug('KademliaService.exposed_client_find_value - Starting the KClosestNode array')
        top_contacts = KContactSortedArray(self.k, key)
        debug('KademliaService.exposed_client_find_value - Starting the semaphore for the queue')
        queue_lock = Semaphore()
        last_value = (None, -1)
        debug('KademliaService.exposed_client_find_value - Starting the semaphore for the last value')
        last_value_lock = Semaphore()
        debug(f'KademliaService.exposed_client_find_value - Starting the iteration on contacts more closes to key: {key}')
        for contact in self.table.get_closest_buckets(key):
            debug(f'KademliaService.exposed_client_find_value - Insert the contact: {contact} to the queue')
            queue.put(contact)
            debug(f'KademliaService.exposed_client_find_value - Insert the contact: {contact} to the visited nodes set')
            visited.add(contact)
            debug(f'KademliaService.exposed_client_find_value - Insert the contact: {contact} to the KClosestNode array')
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                debug('KademliaService.exposed_client_find_value -  Initial alpha nodes completed')
                break
        debug('KademliaService.exposed_client_find_value - Starting the ThreadManager')
        manager = ThreadManager(self.a, queue.qsize, self.find_value_lookup, args=(key, queue, top_contacts, visited, queue_lock, last_value, last_value_lock))
        manager.start()
        debug(f'KademliaService.exposed_client_find_value - Iterate the closest K nodes to find the key: {key}')
        value, time = last_value
        if value is None:
            return None
        for contact in top_contacts:
            debug(f'KademliaService.exposed_client_find_value - Storing key: {key} with value: {value} in contact: {contact}')
            result, _ = self.store_to(contact, key, value, time)
            if not result:
                error(f'KademliaService.exposed_client_find_value - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
        debug(f'KademliaService.exposed_client_store - Finish method with value result: {value}')
        return value

    def find_value_lookup(self, key:int, queue:Queue, top:KContactSortedArray, visited:set, queue_lock:Semaphore, last_value:tuple, last_value_lock:Semaphore):
        contact = None
        try:
            debug(f'KademliaService.find_value_lookup - Removing a contact from the queue')
            contact = queue.get(timeout=1)
            debug(f'KademliaService.find_value_lookup - Contact {contact} out of the queue')
        except Empty:
            debug(f'KademliaService.find_value_lookup - Empty queue')
            return
        debug(f'KademliaService.find_value_lookup - Make the find_node on the contact: {contact}')
        result, new_contacts = self.find_node_to(contact, key)
        if not result:
            debug(f'KademliaService.find_value_lookup - No connection to the node: {contact} was established')
            return
        debug(f'KademliaService.find_value_lookup - Make the find_value on the contact: {contact}')
        result, (value, time) = self.find_value_to(contact, key)
        if not result:
            debug(f'KademliaService.find_value_lookup - No connection to the node: {contact} was established')
            return
        debug(f'KademliaService.find_value_lookup - Update the table with contact: {contact}')
        self.table.update(contact)
        debug(f'KademliaService.find_value_lookup - Cloning contacts received')
        new_contacts = map(Contact.from_json, new_contacts)
        debug(f'KademliaService.find_value_lookup - Acquire lock for last value')
        last_value_lock.acquire()
        debug(f'KademliaService.find_value_lookup - Checking for update last value. Actual Time: {time}, Last Time: {last_value[1]}')
        if time > last_value[1]:
            debug(f'KademliaService.find_value_lookup - Update the last value')
            last_value = (value, time)
        debug(f'KademliaService.find_value_lookup - Release lock for last value')
        last_value_lock.release()
        debug(f'KademliaService.find_value_lookup - Iterate by contacts')
        for new_contact in new_contacts:
            debug(f'KademliaService.find_value_lookup - Pinging to contact: {new_contact}')
            if not self.ping_to(new_contact)[0]:
                debug(f'KademliaService.find_value_lookup - The contact: {new_contact} not respond')
                continue
            debug(f'KademliaService.find_value_lookup - Update the table with contact: {new_contact}')
            self.table.update(new_contact)
            debug(f'KademliaService.find_value_lookup - Lock the queue')
            queue_lock.acquire()
            if not new_contact in visited:
                debug(f'KademliaService.find_value_lookup - The contact: {new_contact} is NOT in the queue')
                debug(f'KademliaService.find_value_lookup - Inserting the contact: {new_contact} to the queue and KClosestNode array and marking as visited')
                visited.add(new_contact)
                queue_lock.release()
                queue.put(new_contact)
                top.push(new_contact)
            else:
                debug(f'KademliaService.find_value_lookup - The contact: {new_contact} is in the queue')
                queue_lock.release()

    def exposed_connect_to_network(self, contact:str):
        self.exposed_init(contact)
        contact = Contact.from_json(contact)
        while not self.is_started_node:
            try:
                if not self.is_initialized:
                    raise Exception(f'KademliaService.exposed_connect_to_network - Instance not initialized')
                try:
                    service_name = KademliaService.get_name(self.__class__)
                    debug(f'KademliaService.exposed_connect_to_network - Server name in the connect_to_network: {service_name}')
                    nodes = discover(service_name)
                    debug(f'KademliaService.exposed_connect_to_network - Discovered nodes: {nodes}')
                except DiscoveryError:
                    raise Exception(f'KademliaService.exposed_connect_to_network - No service found')
                mark = False
                for ip, port in nodes:
                    if ip == self.my_contact.ip and port == self.my_contact.port:
                        continue
                    count = 0
                    while count < 5:
                        try:
                            debug(f'KademliaService.exposed_connect_to_network - Establishing connection with {ip}:{port}')
                            conn = connect(ip, port)
                            debug(f'KademliaService.exposed_connect_to_network - Pinging to {ip}:{port}')
                            result = conn.root.ping(self.my_contact.to_json(), self.lamport)
                            if result:
                                contact = Contact.from_json(result)
                            else:
                                raise Exception(f'KademliaService.exposed_connect_to_network - The contact with address {ip}:{port} is not initialized')
                            debug(f'KademliaService.exposed_connect_to_network - The contact {contact} responded to the ping correctly')
                            break
                        except Exception as e:
                            error(f'Exception: {e} when trying ping to node with ip: {ip} and port: {port}')
                            count += 1
                    if count == 5:
                        debug(f'KademliaService.exposed_connect_to_network - The service with address {ip}: {port} does not respond')
                        continue
                    if not contact == self.my_contact:
                        mark = True
                        self.table.update(contact)
                if not mark:
                    raise Exception('KademliaService.exposed_connect_to_network - Not discover node different')
                try:
                    self.exposed_client_find_node(self.my_contact.id)
                except Exception as e:
                    raise Exception(f'KademliaService.exposed_connect_to_network - I can\'t perform the first iterative find node because: {e}')
                count_of_buckets = len(self.table)
                for i in range(count_of_buckets):
                    if not len(self.table.get_bucket(i).nodes):
                        continue
                    count = 0
                    while count < 5:
                        key = randint(2**i, 2**(i + 1) - 1)
                        try:
                            self.exposed_client_find_node(key)
                            break
                        except Exception as e:
                            error(f'KademliaService.exposed_connect_to_network - I cannot perform the iterative find node. Exception: {e}')
                            count += 1
                    if count == 5:
                        debug(f'KademliaService.exposed_connect_to_network - I cannot perform the iterative find node')
                self.is_started_node = True
                debug(f'KademliaService.exposed_connect_to_network - Finish method. Node is started')
                return True
            except Exception as e:
                error(e)
                debug('KademliaService.exposed_connect_to_network - Sleep for 5 seconds and try to connect to the network again')
                sleep(0.2)
        return False

    @staticmethod
    def get_name(cls) -> str:
        name = cls.__name__
        service = 'Service'
        if name.endswith(service):
            return name[:-len(service)]
