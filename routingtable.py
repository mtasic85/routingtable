__all__ = ['RoutingTable', 'Contact', 'ProtocolCommand', 'Node']

import math
import uuid
import time
import struct
import random
import socket
import asyncio
import marshal

class PrintColors(object):
    VIOLET = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'

class ContactList(object):
    def __init__(self):
        self.items = []
        self.items_id_map = {}
        self.items_raddr_map = {}

    def __len__(self):
        return len(self.items)

    def add(self, c):
        self.items.append(c)

        if c.id is None and not c.bootstrap:
            raise ValueError('Contact it cannot be None, it its is not bootstrap node')

        self.items_id_map[c.id] = c
        self.items_raddr_map[c.remote_host, c.remote_port] = c
        return c

    def get(self, id_or_remote_address):
        c = None

        if isinstance(id_or_remote_address, (str, bytes)):
            c_id = id_or_remote_address
            
            try:
                c = self.items_id_map[c_id]
            except KeyError as e:
                pass
        elif isinstance(id_or_remote_address, (tuple, list)):
            remote_host, remote_port = id_or_remote_address

            try:
                c = self.items_raddr_map[remote_host, remote_port]
            except KeyError as e:
                pass

        return c

    def remove(self, c_or_id):
        c = None

        if isinstance(c_or_id, Contact):
            c = c_or_id
            self.items.remove(c)
            del self.items_id_map[c.id]
            del self.items_raddr_map[c.remote_host, c.remote_port]
        else:
            c_id = c_or_id
            c = self.items_id_map.pop(c_id)
            self.items.remove(c)
            del self.items_raddr_map[c.remote_host, c.remote_port]

        return c

    def random(self, without_id=None, max_old=None):
        # filter contacts
        if without_id:
            if max_old:
                contacts = [c for c in self.items if c.id != without_id and t - c.last_seen <= max_old]
            else:
                contacts = [c for c in self.items if c.id != without_id]
        else:
            if max_old:
                contacts = [c for c in self.items if t - c.last_seen <= max_old]
            else:
                contacts = [c for c in self.items]

        # random contact
        if random.randint(0, 10) == 5:
            # chance is 10% to return boostrap contact
            # this is good because if all nodes fail, bootstrap should be
            # the most reliable node
            c = None

            for n in self.items:
                if n.bootstrap:
                    c = n
                    break
        elif len(contacts):
            c = random.choice(contacts)
        else:
            c = None

        return c

    def all(self, version=0, max_old=None, max_contacts=None):
        contacts = []

        for c in self.items:
            if c.bootstrap:
                contacts.append(c)
                continue

            if max_old and c.last_seen and time.time() - c.last_seen > max_old:
                continue

            contacts.append(c)

        # sort by last seen time
        t = time.time()
        contacts.sort(key=lambda c: t - c.last_seen)

        if max_contacts is not None:
            if isinstance(max_contacts, int):
                contacts = contacts[:max_contacts]
            elif isinstance(max_contacts, float):
                e = int(len(contacts) * max_contacts)
                contacts = contacts[:e]

        return contacts

    def remove_older_than(self, max_old):
        t = time.time()

        for c in self.items[:]:
            if t - c.last_seen > max_old:
                print(PrintColors.YELLOW, 'remove_older_than', self, max_old, c, PrintColors.END)
                self.contacts.remove(c)
                del self.items_id_map[c.id]
                del self.items_raddr_map[c.remote_host, c.remote_port]

class RoutingTable(object):
    def __init__(self):
        self.version = 0
        self.contacts = ContactList()
        self.add_contacts = ContactList()
        self.remove_contacts = ContactList()

class Contact(object):
    def __init__(self, id=None, local_host=None, local_port=None, remote_host=None, remote_port=None, bootstrap=False, version=None):
        self.id = id
        self.local_host = local_host
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.bootstrap = bootstrap
        self.version = version
        self.last_seen = None

    def __repr__(self):
        return '<{}:{} local={}:{} remote={}:{} bootstrap={}>'.format(
            self.__class__.__name__,
            self.id,
            self.local_host,
            self.local_port,
            self.remote_host,
            self.remote_port,
            self.bootstrap,
        )

    def __getstate__(self):
        return {
            'id': self.id,
            'local_host': self.local_host,
            'local_port': self.local_port,
            'remote_host': self.remote_host,
            'remote_port': self.remote_port,
            'bootstrap': self.bootstrap,
            'version': self.version,
        }

class ProtocolCommand(object):
    def __init__(self, node, protocol_version, command_code):
        self.node = node
        self.protocol_version = protocol_version
        self.command_code = command_code
    
    def req(self):
        raise NotImplementedError
    
    def on_req(self):
        raise NotImplementedError

    def res(self):
        raise NotImplementedError

    def on_res(self):
        raise NotImplementedError

class Node(object):
    # protocol version 1.0
    NODE_PROTOCOL_VERSION_MAJOR = 1
    NODE_PROTOCOL_VERSION_MINOR = 0

    # protocol types
    NODE_PROTOCOL_REQ = 0
    NODE_PROTOCOL_RES = 1

    # protocol commands
    NODE_PROTOCOL_PING = 0
    NODE_PROTOCOL_DISCOVER_NODES = 1

    def __init__(self, loop, id=None, listen_host='0.0.0.0', listen_port=6633, bootstrap=False):
        self.loop = loop
        
        if id == None:
            id = str(uuid.uuid4())

        self.id = id

        self.listen_host = listen_host
        self.listen_port = listen_port
        self.bootstrap = bootstrap

        self.recv_buffer = {} # {(remote_host: remote_port): [socket_data, ...]}
        self.recv_packs = {} # {msg_id: {pack_index: pack_data}}
        self.rt = RoutingTable()
        
        # udp socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))
        self.loop.add_reader(self.sock, self.rect_sock_data)

        self.loop.call_soon(self.discover_nodes)
        self.loop.call_soon(self.check_recv_buffer)

    #
    # socket
    #
    def check_recv_buffer(self):
        for remote_address, recv_buffer in self.recv_buffer.items():
            if not len(recv_buffer):
                continue

            self.process_sock_data(b'', remote_address)

        self.loop.call_later(random.random(), self.check_recv_buffer)

    def rect_sock_data(self):
        data, remote_address = self.sock.recvfrom(1500)
        # print('read_sock [DATA]:', remote_address, len(data), data)

        self.process_sock_data(data, remote_address)

    def process_sock_data(self, data, remote_address):
        remote_host, remote_port = remote_address

        if remote_address not in self.recv_buffer:
            self.recv_buffer[remote_address] = []

        self.recv_buffer[remote_address].append(data)
        recv_buffer = b''.join(self.recv_buffer[remote_address])
        pack_header_size = struct.calcsize('!QIIII')

        if len(recv_buffer) < pack_header_size:
            return

        del self.recv_buffer[remote_address][:]
        pack_header = recv_buffer[:pack_header_size]
        recv_buffer_rest = recv_buffer[pack_header_size:]
        msg_id, msg_size, msg_n_packs, pack_size, pack_index = struct.unpack('!QIIII', pack_header)

        if pack_size > len(recv_buffer_rest):
            self.recv_buffer[remote_address].append(pack_header)
            self.recv_buffer[remote_address].append(recv_buffer_rest)
            return

        pack_data = recv_buffer_rest[:pack_size]
        rest_data = recv_buffer_rest[pack_size:]
        self.recv_buffer[remote_address].append(rest_data)
        # print('read_sock [PACK]:', msg_size, msg_n_packs, pack_size, pack_index, pack_data)

        if msg_id not in self.recv_packs:
            self.recv_packs[msg_id] = {}

        self.recv_packs[msg_id][pack_index] = pack_data

        if len(self.recv_packs[msg_id]) < msg_n_packs:
            return

        msg = b''.join([self.recv_packs[msg_id][i] for i in range(msg_n_packs)])
        # print('read_sock [MSG]:', msg)

        self.parse_message(msg, remote_host, remote_port)

    def send_message(self, message_data, remote_host, remote_port):
        for pack in self.build_message(message_data):
            self.sock.sendto(pack, (remote_host, remote_port))

    def build_message(self, message_data):
        message_id = random.randint(0, 2 ** 64)
        step = 1400 - 3 * 4
        pack_index = 0

        message_n_packs = int(math.ceil(len(message_data) / step))

        for s in range(0, len(message_data), step):
            e = s + step
            pack_data = message_data[s:e]
            pack = self.build_pack(message_id, len(message_data), message_n_packs, len(pack_data), pack_index, pack_data)
            pack_index += 1
            yield pack

    def build_pack(self, message_id, message_size, message_n_packs, pack_size, pack_index, pack_data):
        pack = struct.pack('!QIIII', message_id, message_size, message_n_packs, pack_size, pack_index)
        pack += pack_data
        return pack

    def parse_message(self, message, remote_host, remote_port):
        message_header_size = struct.calcsize('!BBBB')
        message_header = message[:message_header_size]
        message_data = message[message_header_size:]
        message_version_major, message_version_minor, message_type, message_command = struct.unpack('!BBBB', message_header)

        if message_command == self.NODE_PROTOCOL_PING:
            pass
        elif message_command == self.NODE_PROTOCOL_DISCOVER_NODES:
            if message_type == self.NODE_PROTOCOL_REQ:
                if message_data:
                    args, kwargs = marshal.loads(message_data)
                else:
                    args, kwargs = (), {}

                self.on_req_discover_nodes(remote_host, remote_port, *args, **kwargs)
            elif message_type == self.NODE_PROTOCOL_RES:
                res = marshal.loads(message_data)
                self.on_res_discover_nodes(remote_host, remote_port, res)

    #
    # NODE_PROTOCOL_DISCOVER_NODES
    #
    def discover_nodes(self):
        # request
        c = self.rt.contacts.random(without_id=self.id)

        if not c:
            # self.loop.call_later(10.0 + random.random() * 1.0, self.discover_nodes)
            self.loop.call_later(random.random() * 10.0, self.discover_nodes)
            return

        print('discover_nodes:', c)
        node_id = self.id
        node_local_host = self.listen_host
        node_local_port = self.listen_port

        args = ()
        kwargs = {
            'id': node_id,
            'local_host': node_local_host,
            'local_port': node_local_port,
        }

        res = (args, kwargs)
        req_data = marshal.dumps(res)

        # message
        message_data = struct.pack(
            '!BBBB',
            self.NODE_PROTOCOL_VERSION_MAJOR,
            self.NODE_PROTOCOL_VERSION_MINOR,
            self.NODE_PROTOCOL_REQ,
            self.NODE_PROTOCOL_DISCOVER_NODES,
        )

        message_data += req_data

        # send message
        self.send_message(message_data, c.remote_host, c.remote_port)

        # schedule next discover
        # self.loop.call_later(10.0 + random.random() * 1.0, self.discover_nodes)
        self.loop.call_later(random.random() * 10.0, self.discover_nodes)

    def on_req_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
        # print('on_req_discover_nodes:', remote_host, remote_port, args, kwargs)
        c = self.rt.contacts.get(kwargs['id'])

        # update contact's `last_seen`, or add contact
        if c:
            c.last_seen = time.time()
        else:
            c = self.rt.contacts.get((remote_host, remote_port))
        
            if c:
                c.last_seen = time.time()
            else:
                c = Contact(
                    id = kwargs['id'],
                    local_host = kwargs['local_host'],
                    local_port = kwargs['local_port'],
                    remote_host = remote_host,
                    remote_port = remote_port,
                    bootstrap = kwargs.get('bootstrap', False),
                )

                # because `c` is requesting to discover nodes
                # put it into known active contacts
                c.last_seen = time.time()
                self.rt.contacts.add(c)

        # forward to res_discover_nodes
        self.res_discover_nodes(remote_host, remote_port, *args, **kwargs)

    def res_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
        # forget old nodes which are not discovered recenly
        self.rt.contacts.remove_older_than(60.0)

        # response
        node_id = self.id
        node_local_host = self.listen_host
        node_local_port = self.listen_port
        # node_contacts = self.rt.contacts.all(version=kwargs.get('version', 0), max_old=15.0, max_contacts=0.90)
        node_contacts = self.rt.contacts.all()
        node_contacts = [c.__getstate__() for c in node_contacts]

        res = {
            'id': node_id,
            'local_host': node_local_host,
            'local_port': node_local_port,
            'contacts': node_contacts,
        }

        res_data = marshal.dumps(res)

        # message data
        message_data = struct.pack(
            '!BBBB',
            self.NODE_PROTOCOL_VERSION_MAJOR,
            self.NODE_PROTOCOL_VERSION_MINOR,
            self.NODE_PROTOCOL_RES,
            self.NODE_PROTOCOL_DISCOVER_NODES,
        )

        message_data += res_data

        # send message
        self.send_message(message_data, remote_host, remote_port)

    def on_res_discover_nodes(self, remote_host, remote_port, res):
        # print('on_res_discover_nodes:', remote_host, remote_port, res)
        print('on_res_discover_nodes len(res[\'contacts\']):', remote_host, remote_port, len(res['contacts']), len(self.rt.contacts))

        # update requesting node/contact in routing table
        c = self.rt.contacts.get(res['id'])

        if c:
            c.last_seen = time.time()
        else:
            c = self.rt.contacts.get((remote_host, remote_port))

            if c:
                c.last_seen = time.time()
            else:
                c = Contact(
                    id = res['id'],
                    local_host = res['local_host'],
                    local_port = res['local_port'],
                    remote_host = remote_host,
                    remote_port = remote_port,
                    bootstrap = res.get('bootstrap', False),
                )

                # because `c` was know when was requested from it
                # to send its nodes in discovery process,
                # add this `c` to known contacts if it was removed
                # from there by any chance
                c.last_seen = time.time()
                self.rt.contacts.add(c)

        # forget old nodes which are not discovered recenly
        self.rt.contacts.remove_older_than(60.0)

        # update discovered nodes/contacts
        for cd in res['contacts']:
            c = self.rt.contacts.get(cd['id'])

            if c:
                c.last_seen = time.time()
            else:
                c = self.rt.contacts.get((cd['remote_host'], cd['remote_port']))

                if c:
                    c.last_seen = time.time()
                else:
                    c = Contact(
                        id = cd['id'],
                        local_host = cd['local_host'],
                        local_port = cd['local_port'],
                        remote_host = cd['remote_host'],
                        remote_port = cd['remote_port'],
                        bootstrap = cd.get('bootstrap', False),
                    )

                    c.last_seen = time.time()
                    self.rt.add_contacts.add(c)

if __name__ == '__main__':
    # event loop
    loop = asyncio.get_event_loop()

    node = Node(loop)
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6633, '127.0.0.1', 6633))
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6634, '127.0.0.1', 6633))
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6635, '127.0.0.1', 6633))
    
    # run loop
    loop.run_forever()
    loop.close()
