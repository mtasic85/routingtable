__all__ = ['Node']
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
        self.version = version\

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

    NODE_PROTOCOL_KEEP_ALIVE = 0
    NODE_PROTOCOL_PING = 1
    NODE_PROTOCOL_PONG = 2
    NODE_PROTOCOL_DISCOVER_NODES = 3

    def __init__(self, loop, id=None, listen_host='127.0.0.1', listen_port=6633):
        self.loop = loop
        self.id = id
        self.listen_host = listen_host
        self.listen_port = listen_port

        self.rt = RoutingTable()
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))
        self.loop.add_reader(self.sock, self.read_sock)

        self.loop.call_soon(self.req_discover_nodes)

    def read_sock(self):
        data = self.sock.recv(1400)
        print('read_sock: len(data) =', len(data), ':', data)

    def req_discover_nodes(self):
        c = self.rt.random()
        print('req_discover_nodes', c)

        message = struct.pack(
            '!BBB',
            self.NODE_PROTOCOL_VERSION_MAJOR,
            self.NODE_PROTOCOL_VERSION_MINOR,
            self.NODE_PROTOCOL_DISCOVER_NODES,
        )

        for pack in self.build_message(message):
            self.sock.sendto(pack, (c.remote_host, c.remote_port))

        self.loop.call_later(2.0, self.req_discover_nodes)

    def build_message(self, message_data):
        step = 1400 - 3 * 4
        pack_index = 0

        for s in range(0, len(message_data), step):
            e = s + step
            pack_data = message_data[s:e]
            pack = self.build_pack(len(message_data), len(pack_data), pack_index, pack_data)
            pack_index += 1
            yield pack

    def build_pack(self, message_size, pack_size, pack_index, pack_data):
        pack = struct.pack('!III', message_size, pack_size, pack_index)
        pack += pack_data
        return pack

if __name__ == '__main__':
    import uuid

    # event loop
    loop = asyncio.get_event_loop()

    node = Node(loop)
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6633, '127.0.0.1', 6633))
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6634, '127.0.0.1', 6634))
    node.rt.add(Contact(str(uuid.uuid4()), '127.0.0.1', 6635, '127.0.0.1', 6635))
    print(node.rt.all())
    print(node.rt.all(1))

    # Blocking call interrupted by loop.stop()
    loop.run_forever()
    loop.close()
