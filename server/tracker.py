from logging import basicConfig, debug, error, info, DEBUG
from queue import Empty, Queue
from random import randint
from threading import Thread, Semaphore
from time import sleep
from socket import gethostbyname, gethostname, socket, AF_INET, SOCK_DGRAM
from rpyc import discover
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import UDPRegistryClient, UDPRegistryServer, DEFAULT_PRUNING_TIMEOUT
from .message import Message
from .user_data import UserData
from .utils import connect, get_hash, KContactSortedArray, IterativeManager, try_function
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
        with open(f'logs/table_{self.my_contact.ip}_{self.my_contact.port}.log', 'w') as file:
            file.write(result)

    def exposed_client_data(self):
        result = ''
        for key in self.data:
            result += f'{key}:{self.data[key]}\n'
        with open(f'logs/data_{self.my_contact.ip}_{self.my_contact.port}.log', 'w') as file:
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

    def exposed_add_group(self, client: Contact, client_lamport: int, key: int, group: int, time: int) -> bool:
        debug(f'TrackerService.exposed_add_group - Trying to add group in key: {key} with group: {group}.')
        if not self.is_initialized:
            error(f'TrackerService.exposed_add_group - Instance not initialized')
            return False, self.lamport
        client = Contact.from_json(client)
        debug(f'TrackerService.exposed_add_group - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        if key in self.data:
            debug(f'TrackerService.exposed_add_group - {key} in data')
            debug(f'TrackerService.exposed_add_group - Acquire lock for data')
            self.data_lock.acquire()
            debug(f'TrackerService.exposed_add_group - Updating value')
            self.data[key].add_group(group, time)
            self.data_lock.release()
            debug(f'TrackerService.exposed_add_group - Release lock for data')
        debug(f'TrackerService.exposed_add_group - End of connection from {client}.')
        return True, self.lamport

    def exposed_remove_group(self, client: Contact, client_lamport: int, key: int, group: int, time: int) -> bool:
        debug(f'TrackerService.exposed_remove_group - Trying to add group in key: {key} with group: {group}.')
        if not self.is_initialized:
            error(f'TrackerService.exposed_remove_group - Instance not initialized')
            return False, self.lamport
        client = Contact.from_json(client)
        debug(f'TrackerService.exposed_remove_group - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        if key in self.data:
            debug(f'TrackerService.exposed_remove_group - {key} in data')
            debug(f'TrackerService.exposed_remove_group - Acquire lock for data')
            self.data_lock.acquire()
            debug(f'TrackerService.exposed_remove_group - Updating value')
            self.data[key].remove_group(group, time)
            self.data_lock.release()
            debug(f'TrackerService.exposed_remove_group - Release lock for data')
        debug(f'TrackerService.exposed_remove_group - End of connection from {client}.')
        return True, self.lamport

    def exposed_add_member(self, client: Contact, client_lamport: int, key: int, member: int, time: int) -> bool:
        debug(f'TrackerService.exposed_add_member - Trying to add member in key: {key} with member: {member}.')
        if not self.is_initialized:
            error(f'TrackerService.exposed_add_member - Instance not initialized')
            return False, self.lamport
        client = Contact.from_json(client)
        debug(f'TrackerService.exposed_add_member - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        if key in self.data:
            debug(f'TrackerService.exposed_add_member - {key} in data')
            debug(f'TrackerService.exposed_add_member - Acquire lock for data')
            self.data_lock.acquire()
            debug(f'TrackerService.exposed_add_member - Updating value')
            self.data[key].add_member(member, time)
            self.data_lock.release()
            debug(f'TrackerService.exposed_add_member - Release lock for data')
        debug(f'TrackerService.exposed_add_member - End of connection from {client}.')
        return True, self.lamport

    def exposed_remove_member(self, client: Contact, client_lamport: int, key: int, member: int, time: int) -> bool:
        debug(f'TrackerService.exposed_remove_member - Trying to add member in key: {key} with member: {member}.')
        if not self.is_initialized:
            error(f'TrackerService.exposed_remove_member - Instance not initialized')
            return False, self.lamport
        client = Contact.from_json(client)
        debug(f'TrackerService.exposed_remove_member - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        if key in self.data:
            debug(f'TrackerService.exposed_remove_member - {key} in data')
            debug(f'TrackerService.exposed_remove_member - Acquire lock for data')
            self.data_lock.acquire()
            debug(f'TrackerService.exposed_remove_member - Updating value')
            self.data[key].remove_member(member, time)
            self.data_lock.release()
            debug(f'TrackerService.exposed_remove_member - Release lock for data')
        debug(f'TrackerService.exposed_remove_member - End of connection from {client}.')
        return True, self.lamport

    def exposed_add_message(self, client: Contact, client_lamport: int, key: int, message: Message) -> bool:
        message = Message.from_json(message)
        debug(f'TrackerService.exposed_add_message - Trying to add member in key: {key} with member: {message}.')
        if not self.is_initialized:
            error(f'TrackerService.exposed_add_message - Instance not initialized')
            return False, self.lamport
        client = Contact.from_json(client)
        debug(f'TrackerService.exposed_add_message - Incoming connection from {client}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        if key in self.data:
            debug(f'TrackerService.exposed_add_message - {key} in data')
            debug(f'TrackerService.exposed_add_message - Acquire lock for data')
            self.data_lock.acquire()
            debug(f'TrackerService.exposed_add_message - Updating value')
            self.data[key].add_message(message)
            self.data_lock.release()
            debug(f'TrackerService.exposed_add_message - Release lock for data')
        debug(f'TrackerService.exposed_add_message - End of connection from {client}.')
        return True, self.lamport

    def exposed_client_store(self, key: int, value: str, option: int = 0, use_self_time: bool = True) -> bool:
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
        debug('TrackerService.exposed_client_store - Starting the IterativeManager')
        manager = IterativeManager(queue.qsize, self.store_lookup, args=(key, queue, top_contacts, visited, queue_lock))
        manager.start()
        success = False
        if option == 0:
            value = UserData.from_json(value)
            if use_self_time:
                value.set_times(self.lamport)
        elif option == 5:
            value = Message.from_json(value)
        debug(f'TrackerService.exposed_client_store - Iterate the closest K nodes to find the key: {key}')
        for contact in top_contacts:
            if option == 0:
                debug(f'TrackerService.exposed_client_store - Storing key: {key} with value: {value} in contact: {contact}')
                result, _ = self.store_to(contact, key, value.to_json())
                if not result:
                    error(f'TrackerService.exposed_client_store - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
                success = success or result
            elif option < 5:
                function = None
                if option == 1:
                    function = self.add_group_to
                elif option == 2:
                    function = self.remove_group_to
                elif option == 3:
                    function = self.add_member_to
                elif option == 4:
                    function = self.remove_member_to
                debug(f'TrackerService.exposed_client_store - Storing key: {key} with value: {value} in contact: {contact}, with option: {option}')
                result, _ = function(contact, key, value, self.lamport)
                if not result:
                    error(f'TrackerService.exposed_client_store - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
                success = success or result
            elif option == 5:
                debug(f'TrackerService.exposed_client_store - Storing key: {key} with value: {value} in contact: {contact}')
                result, _ = self.add_message_to(contact, key, value)
                if not result:
                    error(f'TrackerService.exposed_client_store - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
                success = success or result
        debug(f'TrackerService.exposed_client_store - Finish method with result: {success}')
        return success

    def threaded_update_values(self, force):
        debug(f'TrackerService.update_values - Acquire lock for data')
        self.data_lock.acquire()
        debug(f'TrackerService.update_values - Copying data for temporal list')
        temp = []
        for key in self.data:
            temp.append((key, self.data[key]))
        debug(f'TrackerService.update_values - Clear data')
        self.data.clear()
        self.data_lock.release()
        debug(f'TrackerService.update_values - Release lock for data')
        success = False
        for i in temp:
            debug(f'TrackerService.update_values - Call client store with key: {i[0]}, value: {i[1]}')
            success = success or self.exposed_client_store(i[0], i[1], use_self_time=False)
        debug(f'TrackerService.update_values - Finish with result: {success}')

    def exposed_find_value(self, client: Contact, client_lamport: int, key: int, remove_messages: bool = False) -> tuple:
        if not self.is_initialized:
            error(f'TrackerService.exposed_find_value - Instance not initialized')
            return None, self.lamport
        client = Contact.from_json(client)
        debug(f'TrackerService.exposed_find_value - Incoming connection from {client}.')
        debug(f'TrackerService.exposed_find_value - Asking for key: {key}.')
        self.update_lamport(client_lamport)
        self.update_contact(client)
        try:
            value = self.data[key]
            debug(f'TrackerService.exposed_find_value - Replaying with value: {value}.')
            debug(f'TrackerService.exposed_find_value - End connection from {client}.')
            result = value.to_json()
            if remove_messages:
                value.clear_messages()
            return result, self.lamport
        except KeyError:
            debug(f'TrackerService.exposed_find_value - Value not founded.')
            debug(f'TrackerService.exposed_find_value - End connection from {client}.')
            return None, self.lamport

    def exposed_client_find_value(self, key: int, remove_messages: bool = False) -> object:
        if not self.is_initialized:
            error(f'TrackerService.exposed_client_find_value - Instance not initialized')
            return None
        debug('TrackerService.exposed_client_find_value - Starting the queue')
        queue = Queue()
        debug('TrackerService.exposed_client_find_value - Starting the visited nodes set')
        visited = set()
        debug('TrackerService.exposed_client_find_value - Starting the KClosestNode array')
        top_contacts = KContactSortedArray(self.k, key)
        debug('TrackerService.exposed_client_find_value - Starting the semaphore for the queue')
        queue_lock = Semaphore()
        debug(f'TrackerService.exposed_client_find_value - Insert self contact: {self.my_contact} to the queue')
        queue.put(self.my_contact)
        debug(f'TrackerService.exposed_client_find_value - Insert self contact: {self.my_contact} to the visited nodes set')
        visited.add(self.my_contact)
        debug(f'TrackerService.exposed_client_find_value - Insert self contact: {self.my_contact} to the KClosestNode array')
        top_contacts.push(self.my_contact)
        last_value = UserData()
        debug('TrackerService.exposed_client_find_value - Starting the semaphore for the last value')
        last_value_lock = Semaphore()
        debug(f'TrackerService.exposed_client_find_value - Starting the iteration on contacts more closes to key: {key}')
        for contact in self.table.get_closest_buckets(key):
            debug(f'TrackerService.exposed_client_find_value - Insert the contact: {contact} to the queue')
            queue.put(contact)
            debug(f'TrackerService.exposed_client_find_value - Insert the contact: {contact} to the visited nodes set')
            visited.add(contact)
            debug(f'TrackerService.exposed_client_find_value - Insert the contact: {contact} to the KClosestNode array')
            top_contacts.push(contact)
            if queue.qsize() >= self.a:
                debug('TrackerService.exposed_client_find_value -  Initial alpha nodes completed')
                break
        debug('TrackerService.exposed_client_find_value - Starting the IterativeManager')
        manager = IterativeManager(queue.qsize, self.find_value_lookup, args=(key, queue, top_contacts, visited, queue_lock, last_value, last_value_lock, remove_messages))
        manager.start()
        debug(f'TrackerService.exposed_client_find_value - Iterate the closest K nodes to find the key: {key}')
        value = last_value
        if value.get_name()[0] is None:
            return None
        rvalue = value.to_json()
        if remove_messages:
            value.clear_messages()
        for contact in top_contacts:
            debug(f'TrackerService.exposed_client_find_value - Storing key: {key} with value: {value} in contact: {contact}')
            result, _ = self.store_to(contact, key, value.to_json())
            if not result:
                error(f'TrackerService.exposed_client_find_value - The stored of key: {key} with value: {value} in contact: {contact} was NOT successfuly')
        debug(f'TrackerService.exposed_client_find_value - Finish method with value result: {value}')
        return rvalue

    def find_value_lookup(self, key: int, queue: Queue, top: KContactSortedArray, visited: set, queue_lock: Semaphore, last_value: UserData, last_value_lock: Semaphore, remove_messages: bool):
        contact = None
        try:
            debug(f'TrackerService.find_value_lookup - Removing a contact from the queue')
            contact = queue.get(timeout=1)
            debug(f'TrackerService.find_value_lookup - Contact {contact} out of the queue')
        except Empty:
            debug(f'TrackerService.find_value_lookup - Empty queue')
            return
        debug(f'TrackerService.find_value_lookup - Make the find_node on the contact: {contact}')
        result, new_contacts = self.find_node_to(contact, key)
        if not result:
            debug(f'TrackerService.find_value_lookup - No connection to the node: {contact} was established')
            return
        debug(f'TrackerService.find_value_lookup - Make the find_value on the contact: {contact}')
        result, value = self.find_value_to(contact, key, remove_messages)
        if not result:
            debug(f'TrackerService.find_value_lookup - No connection to the node: {contact} was established')
            return
        debug(f'TrackerService.find_value_lookup - Cloning contacts received')
        new_contacts = map(Contact.from_json, new_contacts)
        if value:
            value = UserData.from_json(value)
            debug(f'TrackerService.find_value_lookup - Acquire lock for last value')
            last_value_lock.acquire()
            debug(f'TrackerService.find_value_lookup - Checking for update last value.')
            debug(f'TrackerService.find_value_lookup - Update the last value')
            last_value.update(value)
            debug(f'TrackerService.find_value_lookup - Release lock for last value')
            last_value_lock.release()
        debug(f'TrackerService.find_value_lookup - Update the table with contact: {contact}')
        self.update_contact(contact)
        debug(f'TrackerService.find_value_lookup - Iterate by contacts')
        for new_contact in new_contacts:
            debug(f'TrackerService.find_value_lookup - Pinging to contact: {new_contact}')
            if not self.ping_to(new_contact)[0]:
                debug(f'TrackerService.find_value_lookup - The contact: {new_contact} not respond')
                continue
            debug(f'TrackerService.find_value_lookup - Update the table with contact: {new_contact}')
            self.update_contact(new_contact)
            debug(f'TrackerService.find_value_lookup - Lock the queue')
            queue_lock.acquire()
            if new_contact not in visited:
                debug(f'TrackerService.find_value_lookup - The contact: {new_contact} is NOT in the queue')
                debug(f'TrackerService.find_value_lookup - Inserting the contact: {new_contact} to the queue and KClosestNode array and marking as visited')
                visited.add(new_contact)
                queue_lock.release()
                queue.put(new_contact)
                top.push(new_contact)
            else:
                debug(f'TrackerService.find_value_lookup - The contact: {new_contact} is in the queue')
                queue_lock.release()

    @try_function()
    def store_to(self, contact: Contact, key: int, value: str) -> bool:
        debug(f'TrackerService.store_to - Trying store to contact: {contact} for key: {key}.')
        result, peer_time = None, None
        if self.my_contact == contact:
            result, peer_time = self.exposed_store(self.my_contact.to_json(), self.lamport, key, value)
        else:
            connection = self.connect(contact)
            result, peer_time = connection.root.store(self.my_contact.to_json(), self.lamport, key, value)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def add_group_to(self, contact: Contact, key: int, group: int, time: int) -> bool:
        debug(f'TrackerService.add_group_to - Trying add group to contact: {contact} for key: {key}.')
        result, peer_time = None, None
        if self.my_contact == contact:
            result, peer_time = self.exposed_add_group(self.my_contact.to_json(), self.lamport, key, group, time)
        else:
            connection = self.connect(contact)
            result, peer_time = connection.root.add_group(self.my_contact.to_json(), self.lamport, key, group, time)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def remove_group_to(self, contact: Contact, key: int, group: int, time: int) -> bool:
        debug(f'TrackerService.remove_group_to - Trying remove group to contact: {contact} for key: {key}.')
        result, peer_time = None, None
        if self.my_contact == contact:
            result, peer_time = self.exposed_remove_group(self.my_contact.to_json(), self.lamport, key, group, time)
        else:
            connection = self.connect(contact)
            result, peer_time = connection.root.remove_group(self.my_contact.to_json(), self.lamport, key, group, time)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def add_member_to(self, contact: Contact, key: int, member: int, time: int) -> bool:
        debug(f'TrackerService.add_member_to - Trying add member to contact: {contact} for key: {key}.')
        result, peer_time = None, None
        if self.my_contact == contact:
            result, peer_time = self.exposed_add_member(self.my_contact.to_json(), self.lamport, key, member, time)
        else:
            connection = self.connect(contact)
            result, peer_time = connection.root.add_member(self.my_contact.to_json(), self.lamport, key, member, time)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def remove_member_to(self, contact: Contact, key: int, member: int, time: int) -> bool:
        debug(f'TrackerService.remove_member_to - Trying remove member to contact: {contact} for key: {key}.')
        result, peer_time = None, None
        if self.my_contact == contact:
            result, peer_time = self.exposed_remove_member(self.my_contact.to_json(), self.lamport, key, member, time)
        else:
            connection = self.connect(contact)
            result, peer_time = connection.root.remove_member(self.my_contact.to_json(), self.lamport, key, member, time)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def find_value_to(self, contact: Contact, key: int, remove_messages: bool = False) -> object:
        debug(f'TrackerService.find_node_to - Trying find_value to contact: {contact} for key: {key}')
        result, peer_time = None, None
        if self.my_contact == contact:
            result, peer_time = self.exposed_find_value(self.my_contact.to_json(), self.lamport, key, remove_messages)
        else:
            connection = self.connect(contact)
            result, peer_time = connection.root.find_value(self.my_contact.to_json(), self.lamport, key, remove_messages)
        self.update_lamport(peer_time)
        return result

    @try_function()
    def add_message_to(self, contact: Contact, key: int, message: Message) -> bool:
        debug(f'TrackerService.add_message_to - Trying add message to contact: {contact} for key: {key}.')
        result, peer_time = None, None
        if self.my_contact == contact:
            result, peer_time = self.exposed_add_message(self.my_contact.to_json(), self.lamport, key, message.to_json())
        else:
            connection = self.connect(contact)
            result, peer_time = connection.root.add_message(self.my_contact.to_json(), self.lamport, key, message.to_json())
        self.update_lamport(peer_time)
        return result

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
                if server is not None:
                    server.close()
                sleep(5)

    @staticmethod
    def __start_service(port: int):
        while True:
            server = None
            try:
                debug('TrackerService.__start_service - Creating instace of service')
                service = TrackerService(3, 160, 3, 1000000)
                debug('TrackerService.__start_service - Creating instace of ThreadedServer')
                server = ThreadedServer(service, port=port, registrar=UDPRegistryClient(), protocol_config={'allow_public_attrs': True})
                debug('TrackerService.__start_service - Starting the service')
                server.start()
                break
            except Exception as e:
                error('TrackerService.__start_service - Error starting service, sleeping 5 seconds and trying again')
                error(e)
                if server is not None:
                    server.close()
                sleep(0.2)

    @staticmethod
    def __start_update_network(ip: str, port: int):
        sleep(10)
        while True:
            try:
                debug(f'TrackerService.__start_update_network - Trying to connect to the service to update network')
                conn = connect(ip, port, config={'sync_request_timeout': 1000000})
                debug(f'TrackerService.__start_update_network - Executing the remote update network method in the service')
                conn.root.client_update_network()
            except Exception as e:
                error(f'TrackerService.__start_update_network - {e}')
            sleep(10)

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
                # conn.ping()
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
        debug('TrackerService.start - Starting a thread for the update network')
        thread_update = Thread(target=TrackerService.__start_update_network, args=(ip, port))
        thread_update.start()
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
            ip = gethostbyname(gethostname())  # This should never happen if the Registry is online
        return ip
