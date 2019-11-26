from logging import debug, error
from queue import Empty, Queue
from random import randint
from threading import Semaphore
from time import sleep
from rpyc import connect, Connection, discover, Service
from rpyc.utils.factory import DiscoveryError
from .bucket_table import BucketTable
from .contact import Contact
from .data import Data
from .utils import connect, KContactSortedArray, ThreadManager, try_function


class KademliaService(Service):
    def __init__(self, k: int, b: int, a: int, update_time: int = None):
        super(KademliaService, self).__init__()
        debug(f'KademliaService.exposed_init - Executing the init with the k: {k},b: {b} and a: {a}')
        self.a = a
        self.is_started_node = False
        self.data = Data()
        self.lamport = 0
        self.lamport_lock = Semaphore()
        self.k = k
        self.b = b
        self.is_initialized = False
        self.is_initialized_lock = Semaphore()
        self.my_contact = None
        self.table = None
        self.update_time = update_time

    def exposed_init(self, contact: Contact):
        if self.is_initialized:
            return True
        self.my_contact = Contact.from_json(contact)
        debug(f'KademliaService.exposed_init - Initializing with contact: {contact}.')
        debug(f'KademliaService.exposed_init - Executing the init with the contact: {self.my_contact}')
        self.table = BucketTable(self.k, self.b, self.my_contact.id)
        self.is_initialized = True
        debug(f'KademliaService.exposed_init - End initializing with contact: {contact}.')
        return True

    def exposed_ping(self, client: Contact, client_lamport: int) -> bool:
        if not self.is_initialized:
            error(f'KademliaService.exposed_ping - Instance not initialized')
            return None, self.lamport
        client = Contact.from_json(client)
        debug(f'KademliaService.exposed_ping - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        debug(f'KademliaService.exposed_ping - End of connection from {client}.')
        return self.my_contact.to_json(), self.lamport

    def exposed_store(self, client: Contact, client_lamport: int, key: int, value: str, store_time: int) -> bool:
        debug(f'KademliaService.exposed_store - Trying to store value in key: {key} at time: {store_time}.')
        if not self.is_initialized:
            error(f'KademliaService.exposed_store - Instance not initialized')
            return False, self.lamport
        client = Contact.from_json(client)
        debug(f'KademliaService.exposed_store - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            debug(f'KademliaService.exposed_store - Acquire lock for data')
            self.data.lock.acquire()
            actual_value, actual_time = self.data[key]
            self.data.lock.release()
            debug(f'KademliaService.exposed_store - Release lock for data')
        except KeyError:
            actual_value, actual_time = (value, store_time)
        self.data.lock.acquire()
        self.data[key] = (value, store_time) if store_time > actual_time else (actual_value, actual_time)
        self.data.lock.release()
        debug(f'KademliaService.exposed_store - End of connection from {client}.')
        return True, self.lamport

    def exposed_find_node(self, client: Contact, client_lamport: int, id: int) -> list:
        if not self.is_initialized:
            error(f'KademliaService.exposed_find_node - Instance not initialized')
            return None, self.lamport
        client = Contact.from_json(client)
        debug(f'KademliaService.exposed_find_node - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        result = []
        count = 0
        for contact in list(self.table.get_closest_buckets(id)):
            result.append(contact.to_json())
            count += 1
            if count >= self.k:
                break
        debug(f'KademliaService.exposed_find_node - Replaying with {result}.')
        debug(f'KademliaService.exposed_find_node - End of connection from {client}.')
        return result, self.lamport

    def exposed_find_value(self, client: Contact, client_lamport: int, key: int) -> object:
        if not self.is_initialized:
            error(f'KademliaService.exposed_find_value - Instance not initialized')
            return None, self.lamport
        client = Contact.from_json(client)
        debug(f'KademliaService.exposed_find_value - Incoming connection from {client}.')
        debug(f'KademliaService.exposed_find_value - Asking for key: {key}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            value, stored_time = self.data[key]
            debug(f'KademliaService.exposed_find_value - Replaying with value: {value} and value_time: {stored_time}.')
            debug(f'KademliaService.exposed_find_value - Incoming connection from {client}.')
            return (value, stored_time), self.lamport
        except KeyError:
            debug(f'KademliaService.exposed_find_value - Value not founded.')
            debug(f'KademliaService.exposed_find_value - Incoming connection from {client}.')
            return None, self.lamport

    def exposed_client_store(self, key: int, value: str, store_time: int = None) -> bool:
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
        debug(f'KademliaService.exposed_client_store - Insert self contact: {self.my_contact} to the queue')
        queue.put(self.my_contact)
        debug(f'KademliaService.exposed_client_store - Insert self contact: {self.my_contact} to the visited nodes set')
        visited.add(self.my_contact)
        debug(f'KademliaService.exposed_client_store - Insert self contact: {self.my_contact} to the KClosestNode array')
        top_contacts.push(self.my_contact)
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
        time = self.lamport if store_time is None else store_time
        debug(f'KademliaService.exposed_client_store - Time for store: {time}')
        debug(f'KademliaService.exposed_client_store - Iterate the closest K nodes to find the key: {key}')
        for contact in top_contacts:
            debug(f'KademliaService.exposed_client_store - Storing key: {key} with value: {value} in contact: {contact}')
            result, _ = self.store_to(contact, key, value, time)
            if not result:
                error(f'KademliaService.exposed_client_store - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
            success = success or result
        debug(f'KademliaService.exposed_client_store - Finish method with result: {success}')
        return success

    def store_lookup(self, key: int, queue: Queue, top: KContactSortedArray, visited: set, queue_lock: Semaphore):
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
        self.update_contact(contact)
        debug(f'KademliaService.store_lookup - Cloning contacts received')
        new_contacts = map(Contact.from_json, new_contacts)
        debug(f'KademliaService.store_lookup - Iterate by contacts')
        for new_contact in new_contacts:
            debug(f'KademliaService.store_lookup - Pinging to contact: {new_contact}')
            if not self.ping_to(new_contact)[0]:
                debug(f'KademliaService.store_lookup - The contact: {new_contact} not respond')
                continue
            debug(f'KademliaService.store_lookup - Update the table with contact: {new_contact}')
            self.update_contact(new_contact)
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

    def exposed_client_find_node(self, id: int) -> list:
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
        debug(f'KademliaService.exposed_client_find_node - Insert self contact: {self.my_contact} to the queue')
        queue.put(self.my_contact)
        debug(f'KademliaService.exposed_client_find_node - Insert self contact: {self.my_contact} to the visited nodes set')
        visited.add(self.my_contact)
        debug(f'KademliaService.exposed_client_find_node - Insert self contact: {self.my_contact} to the KClosestNode array')
        top_contacts.push(self.my_contact)
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

    def find_node_lookup(self, id: int, queue: Queue, top: KContactSortedArray, visited: set, queue_lock: Semaphore):
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
        self.update_contact(contact)
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
            self.update_contact(new_contact)
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

    def exposed_client_find_value(self, key: int) -> object:
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
        debug(f'KademliaService.exposed_client_find_value - Insert self contact: {self.my_contact} to the queue')
        queue.put(self.my_contact)
        debug(f'KademliaService.exposed_client_find_value - Insert self contact: {self.my_contact} to the visited nodes set')
        visited.add(self.my_contact)
        debug(f'KademliaService.exposed_client_find_value - Insert self contact: {self.my_contact} to the KClosestNode array')
        top_contacts.push(self.my_contact)
        last_value = [None, -1]
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

    def find_value_lookup(self, key: int, queue: Queue, top: KContactSortedArray, visited: set, queue_lock: Semaphore, last_value: list, last_value_lock: Semaphore):
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
        self.update_contact(contact)
        debug(f'KademliaService.find_value_lookup - Cloning contacts received')
        new_contacts = map(Contact.from_json, new_contacts)
        debug(f'KademliaService.find_value_lookup - Acquire lock for last value')
        last_value_lock.acquire()
        debug(f'KademliaService.find_value_lookup - Checking for update last value. Actual Time: {time}, Last Time: {last_value[1]}')
        if time > last_value[1]:
            debug(f'KademliaService.find_value_lookup - Update the last value')
            last_value[0], last_value[1] =  value, time
        debug(f'KademliaService.find_value_lookup - Release lock for last value')
        last_value_lock.release()
        debug(f'KademliaService.find_value_lookup - Iterate by contacts')
        for new_contact in new_contacts:
            debug(f'KademliaService.find_value_lookup - Pinging to contact: {new_contact}')
            if not self.ping_to(new_contact)[0]:
                debug(f'KademliaService.find_value_lookup - The contact: {new_contact} not respond')
                continue
            debug(f'KademliaService.find_value_lookup - Update the table with contact: {new_contact}')
            self.update_contact(new_contact)
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

    def exposed_connect_to_network(self, contact: str):
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
                            result, _ = conn.root.ping(self.my_contact.to_json(), self.lamport)
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
                    if contact != self.my_contact:
                        mark = True
                        self.update_contact(contact)
                if not mark:
                    raise Exception('KademliaService.exposed_connect_to_network - Not discover node different')
                try:
                    self.exposed_client_find_node(self.my_contact.id)
                except Exception as e:
                    raise Exception(f'KademliaService.exposed_connect_to_network - I can\'t perform the first iterative find node because: {e}')
                count_of_buckets = len(self.table)
                for i in range(count_of_buckets):
                    if not self.table.get_bucket(i):
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

    def update_values(force=False):
        if self.update_time is None and not force:
            return
        debug(f'KademliaService.update_values - Starting')
        if self.lamport % self.update_time and not force:
            debug(f'KademliaService.update_values - No time for update')
            return
        debug(f'KademliaService.update_values - Acquire lock for data')
        self.data.lock.acquire()
        debug(f'KademliaService.update_values - Copying data for temporal list')
        temp = []
        for key in self.data:
            temp.append((key, self.data[key]))
        debug(f'KademliaService.update_values - Clear data')
        self.data.clear()
        self.data.lock.release()
        debug(f'KademliaService.update_values - Release lock for data')
        success = False
        for i in temp:
            debug(f'KademliaService.update_values - Call client store with key: {i[0]}, values: {i[1][0]} and time: {i[1][1]}')
            success = success or self.exposed_client_store(i[0], i[1][0], i[1][1])
        debug(f'KademliaService.update_values - Finish with result: {success}')
        return success

    def update_contact(self, contact: Contact):
        debug(f'KademliaService.update_contact - Updating contact: {contact}.')
        if contact == self.my_contact:
            return
        if not self.table.update(contact):
            bucket = self.table.get_bucket(contact.id)
            to_remove = None
            bucket.semaphore.acquire()
            for stored in bucket:
                if not self.ping_to(stored)[0]:
                    to_remove = stored
            if to_remove:
                bucket.remove_by_contact(to_remove)
                bucket.update(contact)
            bucket.semaphore.release()
        debug(f'KademliaService.update_contact - Contact updated.')

    def update_lamport(self, client_lamport: int = 0):
        debug(f'KademliaService.update_lamport - Updating actual time with time: {client_lamport}.')
        self.lamport_lock.acquire()
        self.lamport = max(client_lamport, self.lamport + 1)
        self.lamport_lock.release()
        debug(f'KademliaService.update_lamport - Time updated.')

    def connect(self, contact: Contact) -> Connection:
        debug(f'Protocol.connect - Trying to connect with contact: {contact}.')
        self.update_lamport()
        connection = connect(contact.ip, str(contact.port), timeout=0.5)
        connection.ping()
        debug(f'KademliaService.Protocol.connect - Connection with contact: {contact} stablished.')
        return connection

    @staticmethod
    def get_name(arg) -> str:
        name = arg.__name__
        service = 'Service'
        if name.endswith(service):
            return name[:-len(service)]

    @try_function()
    def ping_to(self, contact: Contact) -> bool:
        debug(f'KademliaService.ping_to - Trying ping to contact: {contact}.')
        connection = self.connect(contact)
        result, peer_time = connection.root.ping(self.my_contact.to_json(), self.lamport)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def store_to(self, contact: Contact, key: int, value: str, store_time: int) -> bool:
        debug(f'KademliaService.store_to - Trying store to contact: {contact} for key: {key}.')
        connection = self.connect(contact)
        result, peer_time = connection.root.store(self.my_contact.to_json(), self.lamport, key, value, store_time)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def find_node_to(self, contact: Contact, id: int) -> list:
        debug(f'KademliaService.find_node_to - Trying find_node to contact: {contact} for id: {id}')
        connection = self.connect(contact)
        result, peer_time = connection.root.find_node(self.my_contact.to_json(), self.lamport, id)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def find_value_to(self, contact: Contact, key: int) -> object:
        debug(f'KademliaService.find_node_to - Trying find_value to contact: {contact} for key: {key}')
        connection = self.connect(contact)
        result, peer_time = connection.root.find_value(self.my_contact.to_json(), self.lamport, key)
        self.update_lamport(peer_time)
        return result
