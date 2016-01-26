__all__ = ['Node', 'RoutingTable', 'Contact']

import math
import uuid
import time
import struct
import random
import socket
import asyncio
import marshal

class Contact(object):
    def __init__(self, id=None, local_host=None, local_port=None, remote_host=None, remote_port=None, version=None):
        self.id = id
        self.local_host = local_host
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.version = version
        self.last_seen = None

    def __repr__(self):
        return '<{} id={} local={}:{} remote={}:{} ver={}>'.format(
            self.__class__.__name__,
            self.id,
            self.local_host,
            self.local_port,
            self.remote_host,
            self.remote_port,
            self.version,
        )

    def __getstate__(self):
        return {
            'id': self.id,
            'local_host': self.local_host,
            'local_port': self.local_port,
            'remote_host': self.remote_host,
            'remote_port': self.remote_port,
            'version': self.version,
        }

class RoutingTable(object):
    def __init__(self):
        self.version = 0
        self.contacts = [] # [Contact(), ...]

    def add(self, c):
        c.version = self.version
        self.version += 1
        self.contacts.append(c)

    def get(self, id):
        for c in self.contacts:
            if c.id == id:
                return c

    def remove(self, c_or_id):
        if isinstance(c_or_id, Contact):
            self.contacts.remove(c_or_id)
        else:
            for i, c in enumerate(self.contacts[:]):
                if c.id == c_or_id:
                    del self.contacts[i]
                    break

    def update_or_add(self, c):
        for n in self.contacts:
            if n.id == c.id:
                n.local_host = c.local_host
                n.local_port = c.local_port
                n.remote_host = c.remote_host
                n.remote_port = c.remote_port
                break
            elif n.remote_host == c.remote_host and n.remote_port == c.remote_port:
                n.id = c.id
                n.local_host = c.local_host
                n.local_port = c.local_port
                break
        else:
            self.contacts.append(c)

    def update_by_address(self, remote_host, remote_port, id, local_host, local_port):
        for c in self.contacts:
            if c.remote_host == remote_host and c.remote_port == remote_port:
                c.id = id
                c.local_host = local_host
                c.local_port = local_port
                break

    def random(self, without_id=None):
        if len(self.contacts):
            c = random.choice(self.contacts)
            
            if c.id == without_id:
                c = None
        else:
            c = None

        return c

    def all(self, version=0):
        contacts = []

        for c in self.contacts:
            # if c.version is None:
            #     continue

            # if c.version < version:
            #     continue

            contacts.append(c)

        return contacts

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

    def __init__(self, loop, id=None, listen_host='127.0.0.1', listen_port=6633):
        self.loop = loop
        
        if id == None:
            id = str(uuid.uuid4())

        self.id = id

        self.listen_host = listen_host
        self.listen_port = listen_port

        self.recv_buffer = {} # {(remote_host: remote_port): [socket_data, ...]}
        self.recv_packs = {} # {msg_id: {pack_index: pack_data}}
        self.rt = RoutingTable()
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))
        self.loop.add_reader(self.sock, self.rect_sock_data)

        self.loop.call_soon(self.discover_nodes)
        self.loop.call_soon(self.check_recv_buffer)
        self.loop.call_soon(self.check_last_seen_contacts)

    def check_recv_buffer(self):
        for remote_address in self.recv_buffer:
            self.process_sock_data(b'', remote_address)

        self.loop.call_later(1.0, self.check_recv_buffer)

    def check_last_seen_contacts(self):
        t = time.time()
        
        for c in self.rt.contacts[:]:
            if not c.last_seen:
                continue

            if t - c.last_seen > 30.0:
                print('check_last_seen_contacts removed [CONTACT]:', c)
                self.rt.remove(c)

        self.loop.call_later(
            60 + random.random() * 60.0,
            self.check_last_seen_contacts
        )

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
    # discover_nodes
    #
    def discover_nodes(self):
        # request
        c = self.rt.random(without_id=self.id)
        print('discover_nodes:', c)

        if not c:
            self.loop.call_later(2.0, self.discover_nodes)
            return

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
        for pack in self.build_message(message_data):
            self.sock.sendto(pack, (c.remote_host, c.remote_port))

    def on_req_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
        # print('on_req_discover_nodes:', remote_host, remote_port, args, kwargs)

        # update/add contact which is requesting response
        c = Contact(
            id = kwargs['id'],
            local_host = kwargs['local_host'],
            local_port = kwargs['local_port'],
            remote_host = remote_host,
            remote_port = remote_port,
        )

        if c.id != self.id:
            self.rt.update_or_add(c)
            c.last_seen = time.time()

        # forward to res_discover_nodes
        self.res_discover_nodes(remote_host, remote_port, *args, **kwargs)

    def res_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
        # response
        node_id = self.id
        node_local_host = self.listen_host
        node_local_port = self.listen_port
        node_contacts = self.rt.all(version=kwargs.get('version', 0))
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
        for pack in self.build_message(message_data):
            self.sock.sendto(pack, (remote_host, remote_port))

    def on_res_discover_nodes(self, remote_host, remote_port, res):
        # print('on_res_discover_nodes:', remote_host, remote_port, res)
        print('on_res_discover_nodes len(res[\'contacts\']):', remote_host, remote_port, len(res['contacts']))

        self.rt.update_by_address(
            remote_host,
            remote_port,
            id = res['id'],
            local_host = res['local_host'],
            local_port = res['local_port'],
        )

        for cd in res['contacts']:
            c = Contact(
                id = cd['id'],
                local_host = cd['local_host'],
                local_port = cd['local_port'],
                remote_host = cd['remote_host'],
                remote_port = cd['remote_port'],
            )

            if c.id != self.id:
                self.rt.update_or_add(c)
                c.last_seen = time.time()

        # self.loop.call_later(5.0, self.discover_nodes)
        self.loop.call_later(random.random() * 5.0, self.discover_nodes)

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
