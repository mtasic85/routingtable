__all__ = ['Node']
import math
import struct
import random
import socket
import asyncio
import marshal

class Contact(object):
    def __init__(self, id, local_host, local_port, remote_host, remote_port, version=None):
        self.id = id
        self.local_host = local_host
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.version = version

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
        self.contacts = {} # {node_id: Contact()}

    def add(self, c):
        c.version = self.version
        self.version += 1
        self.contacts[c.id] = c

    def get(self, id):
        c = self.contacts[id]
        return c

    def remove(self, c):
        del self.contacts[c.id]

    def random(self):
        c = random.choice(list(self.contacts.values()))
        return c

    def all(self, version=0):
        contacts = {}

        for c in self.contacts.values():
            if c.version < version:
                continue

            contacts[c.id] = c

        return contacts

class Node(object):
    # version 1.0
    NODE_PROTOCOL_VERSION_MAJOR = 1
    NODE_PROTOCOL_VERSION_MINOR = 0

    NODE_PROTOCOL_REQ = 0
    NODE_PROTOCOL_RES = 1

    NODE_PROTOCOL_KEEP_ALIVE = 0
    NODE_PROTOCOL_PING = 1
    NODE_PROTOCOL_PONG = 2
    NODE_PROTOCOL_DISCOVER_NODES = 3

    def __init__(self, loop, id=None, listen_host='127.0.0.1', listen_port=6633):
        self.loop = loop
        self.id = id
        self.listen_host = listen_host
        self.listen_port = listen_port

        self.recv_buffer = [] # [socket_data, ...]
        self.recv_packs = {} # {msg_id: {pack_index: pack_data}}
        self.rt = RoutingTable()
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))
        self.loop.add_reader(self.sock, self.read_sock)

        self.loop.call_soon(self.req_discover_nodes)

    def read_sock(self):
        data, remote_address = self.sock.recvfrom(1500)
        remote_host, remote_port = remote_address
        print('read_sock [DATA]:', remote_address, len(data), data)

        self.recv_buffer.append(data)
        recv_buffer = b''.join(self.recv_buffer)
        pack_header_size = struct.calcsize('!QIIII')

        if len(recv_buffer) < pack_header_size:
            return

        del self.recv_buffer[:]
        pack_header = recv_buffer[:pack_header_size]
        recv_buffer_rest = recv_buffer[pack_header_size:]
        msg_id, msg_size, msg_n_packs, pack_size, pack_index = struct.unpack('!QIIII', pack_header)

        if pack_size > len(recv_buffer_rest):
            self.recv_buffer.append(pack_header)
            self.recv_buffer.append(recv_buffer_rest)
            return

        pack_data = recv_buffer_rest[:pack_size]
        rest_data = recv_buffer_rest[pack_size:]
        self.recv_buffer.append(rest_data)
        print('read_sock [PACK]:', msg_size, msg_n_packs, pack_size, pack_index, pack_data)

        if msg_id not in self.recv_packs:
            self.recv_packs[msg_id] = {}

        self.recv_packs[msg_id][pack_index] = pack_data

        if len(self.recv_packs[msg_id]) < msg_n_packs:
            return

        msg = b''.join([self.recv_packs[msg_id][i] for i in range(msg_n_packs)])
        print('read_sock [MSG]:', msg)

        self.parse_message(msg, remote_host, remote_port)

    def req_discover_nodes(self):
        c = self.rt.random()
        print('req_discover_nodes', c)

        message_data = struct.pack(
            '!BBBB',
            self.NODE_PROTOCOL_VERSION_MAJOR,
            self.NODE_PROTOCOL_VERSION_MINOR,
            self.NODE_PROTOCOL_REQ,
            self.NODE_PROTOCOL_DISCOVER_NODES,
        )

        for pack in self.build_message(message_data):
            print('pack', pack)
            self.sock.sendto(pack, (c.remote_host, c.remote_port))

    def on_req_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
        self.res_discover_nodes(remote_host, remote_port, *args, **kwargs)

    def res_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
        contacts = self.rt.all(*args, **kwargs)
        contacts = [c.__getstate__() for c in contacts.values()]
        contacts_data = marshal.dumps(contacts)
        remote_host, remote_port = '127.0.0.1', 6633

        message_data = struct.pack(
            '!BBBB',
            self.NODE_PROTOCOL_VERSION_MAJOR,
            self.NODE_PROTOCOL_VERSION_MINOR,
            self.NODE_PROTOCOL_RES,
            self.NODE_PROTOCOL_DISCOVER_NODES,
        )

        message_data += contacts_data

        for pack in self.build_message(message_data):
            print('pack', pack)
            self.sock.sendto(pack, (remote_host, remote_port))

    def on_res_discover_nodes(self, remote_host, remote_port, contacts):
        print('on_res_discover_nodes:', remote_host, remote_port, contacts)

        for cd in contacts:
            c = Contact(**cd) # version ?
            self.rt.add(c)

        self.loop.call_later(2.0, self.req_discover_nodes)

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

        if message_command == self.NODE_PROTOCOL_KEEP_ALIVE:
            pass
        elif message_command == self.NODE_PROTOCOL_PING:
            pass
        elif message_command == self.NODE_PROTOCOL_PONG:
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
                print('!!!', )

if __name__ == '__main__':
    import uuid

    # event loop
    loop = asyncio.get_event_loop()

    node = Node(loop)
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6633, '127.0.0.1', 6633))
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6634, '127.0.0.1', 6633))
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6635, '127.0.0.1', 6633))
    # print(node.rt.all())
    # print(node.rt.all(1))

    # Blocking call interrupted by loop.stop()
    loop.run_forever()
    loop.close()
