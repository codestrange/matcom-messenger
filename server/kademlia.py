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
    def __init__(self, my_contact:Contact, k: int, b:int, a:int, value_cloner):
        super(KademliaService, self).__init__(my_contact, k, b, value_cloner)
        self.a = a
        self.is_started_node = False

    def on_connect(self, conn:Connection):
        pass

    def on_disconnect(self, conn:Connection):
        pass

    def exposed_client_store(self, key:int, value:object) -> bool:
        value = self.value_cloner(value)
        queue = Queue()
        visited = set()
        top_contacts = KContactSortedArray(self.k, key)
        queue_lock = Semaphore()
        for contact in self.table.get_closest_buckets(key):
            queue.put(contact)
            visited.add(contact)
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                break
        manager = ThreadManager(self.a, queue.qsize, self.store_lookup, args=(key, queue, top_contacts, visited, queue_lock))
        manager.start()
        success = False
        for contact in top_contacts:
            result, _ = self.store(contact, key, value, self.lamport)
            success = success or result
        return success

    def store_lookup(self, key:int, queue:Queue, top:KContactSortedArray, visited:set, queue_lock:Semaphore):
        contact = None
        try:
            contact = queue.get(timeout=1)
        except Empty:
            return
        result, new_contacts = self.find_node(contact, key)
        if not result:
            return
        self.table.update(contact)
        new_contacts = map(Contact.clone, new_contacts)
        for new_contact in new_contacts:
            if not self.ping(new_contact)[0]:
                continue
            self.table.update(new_contact)
            queue_lock.acquire()
            if not new_contact in visited:
                visited.add(new_contact)
                queue_lock.release()
                queue.put(new_contact)
                top.push(new_contact)
            else:
                queue_lock.release()

    def exposed_client_find_node(self, id:int) -> list:
        queue = Queue()
        visited = set()
        top_contacts = KContactSortedArray(self.k, id)
        queue_lock = Semaphore()
        for contact in self.table.get_closest_buckets(id):
            queue.put(contact)
            visited.add(contact)
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                break
        manager = ThreadManager(self.a, queue.qsize, self.find_node_lookup, args=(id, queue, top_contacts, visited, queue_lock))
        manager.start()
        for contact in top_contacts:
            if contact.hash == id:
                return contact
        return None

    def find_node_lookup(self, id:int, queue:Queue, top:KContactSortedArray, visited:set, queue_lock:Semaphore):
        contact = None
        try:
            contact = queue.get(timeout=1)
        except Empty:
            return
        result, new_contacts = self.find_node(contact, id)
        if not result:
            return
        self.table.update(contact)
        new_contacts = map(Contact.clone, new_contacts) 
        for new_contact in new_contacts:
            if not self.ping(new_contact)[0]:
                continue
            self.table.update(new_contact)
            queue_lock.acquire()
            if not new_contact in visited:
                visited.add(new_contact)
                queue_lock.release()
                queue.put(new_contact)
                top.push(new_contact)
            else:
                queue_lock.release()

    def exposed_client_find_value(self, key:int) -> object:
        queue = Queue()
        visited = set()
        top_contacts = KContactSortedArray(self.k, key)
        queue_lock = Semaphore()
        last_value = (None, -1)
        last_value_lock = Semaphore()
        for contact in self.table.get_closest_buckets(key):
            queue.put(contact)
            visited.add(contact)
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                break
        manager = ThreadManager(self.a, queue.qsize, self.find_value_lookup, args=(key, queue, top_contacts, visited, queue_lock, last_value, last_value_lock))
        manager.start()
        value, time = last_value
        if value is None:
            return None
        for contact in top_contacts:
            self.store(contact, key, value, time)
        return value

    def find_value_lookup(self, key:int, queue:Queue, top:KContactSortedArray, visited:set, queue_lock:Semaphore, last_value:object, last_value_lock:Semaphore):
        contact = None
        try:
            contact = queue.get(timeout=1)
        except Empty:
            return
        result, new_contacts = self.find_node(contact, key)
        if not result:
            return
        result, (value, time) = self.find_value(contact, key)
        if not result:
            return
        self.table.update(contact)
        value = self.value_cloner(value)
        new_contacts = map(Contact.clone, new_contacts)
        last_value_lock.acquire()
        if time > last_value[1]:
            last_value = (value, time)
        last_value_lock.release()
        for new_contact in new_contacts:
            if not self.ping(new_contact)[0]:
                continue
            self.table.update(new_contact)
            queue_lock.acquire()
            if not new_contact in visited:
                visited.add(new_contact)
                queue_lock.release()
                queue.put(new_contact)
                top.push(new_contact)
            else:
                queue_lock.release()

    def exposed_connect_to_network(self):
        while not self.is_started_node:
            try:
                try:
                    service_name = KademliaService.get_name(self.__class__)
                    debug(f'KademliaService.exposed_connect_to_network - Server name in the connect_to_network: {service_name}')
                    nodes = discover(service_name)
                    debug(f'KademliaService.exposed_connect_to_network - Discovered nodes: {nodes}')
                except DiscoveryError:
                    raise Exception(f'KademliaService.exposed_connect_to_network - No service found')
                mark = False
                for ip, port in nodes:
                    count = 0
                    while count < 5:
                        try:
                            debug(f'KademliaService.exposed_connect_to_network - Establishing connection with {ip}:{port}')
                            conn = connect(ip, port)
                            debug(f'KademliaService.exposed_connect_to_network - Pinging to {ip}:{port}')
                            contact = Contact.clone(conn.root.ping())
                            debug(f'KademliaService.exposed_connect_to_network - The contact {contact} responded to the ping correctly')
                            break
                        except:
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
                    self.exposed_client_find_node(self.my_contact.hash)
                except Exception as e:
                    raise Exception(f'KademliaService.exposed_connect_to_network - I can\'t perform the first iterative find node because: {e}')
                count_of_buckets = len(self.table)
                for i in range(count_of_buckets):
                    count = 0
                    while count < 5:
                        key = randint(2**i, 2**(i + 1) - 1)
                        try:
                            self.exposed_client_find_node(key)
                            break
                        except:
                            count += 1
                    if count == 5:
                        debug(f'KademliaService.exposed_connect_to_network - I cannot perform the iterative find node')
                self.is_started_node = True
                return True
            except Exception as e:
                error(e)
                debug('KademliaService.exposed_connect_to_network - Sleep for 5 seconds and try to connect to the network again')
                sleep(5)
        return False

    @staticmethod
    def get_name(cls) -> str:
        name = cls.__name__
        service = 'Service'
        if name.endswith(service):
            return name[:-len(service)]
