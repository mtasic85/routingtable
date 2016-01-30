__all__ = ['Node']

import math
import uuid
import time
import struct
import random
import socket
import asyncio
import marshal

from print_colors import PrintColors
from contact import Contact
from routing_table import RoutingTable
from protocol_command import ProtocolCommand
from ping_protocol_command import PingProtocolCommand
from discover_protocol_command import DiscoverProtocolCommand

class Node(object):
    def __init__(self, loop, id=None, listen_host='0.0.0.0', listen_port=6633, bootstrap=False):
        self.loop = loop
        
        if id == None:
            id = str(uuid.uuid4())

        self.id = id

        self.listen_host = listen_host
        self.listen_port = listen_port
        self.bootstrap = bootstrap

        # routing table
        self.rt = RoutingTable()

        # default protocol_commands
        self.protocol_commands = {}
        protocol_command = PingProtocolCommand(self, 1, 0, 0)
        self.add_protocol_command(protocol_command)
        protocol_command = DiscoverProtocolCommand(self, 1, 0, 1)
        self.add_protocol_command(protocol_command)
        
        # socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.listen_host, self.listen_port))

        self.recv_buffer = {} # {(remote_host: remote_port): [socket_data, ...]}
        self.recv_packs = {} # {msg_id: {pack_index: pack_data}}
        self.loop.add_reader(self.sock, self.rect_sock_data)

        # tasks
        self.loop.call_soon(self.check_recv_buffer)
        self.loop.call_soon(self.remove_dead_contacts)

    #
    # protocol commands
    #
    def get_protocol_command(self, protocol_major_version, protocol_minor_version, protocol_command_code):
        k = (
            protocol_major_version,
            protocol_minor_version,
            protocol_command_code,
        )

        protocol_command = self.protocol_commands[k]
        return protocol_command

    def add_protocol_command(self, protocol_command):
        k = (
            protocol_command.protocol_major_version,
            protocol_command.protocol_minor_version,
            protocol_command.protocol_command_code,
        )

        self.protocol_commands[k] = protocol_command
        protocol_command.start()

    def remove_protocol_command(self, protocol_command):
        protocol_command.stop()

        k = (
            protocol_command.protocol_major_version,
            protocol_command.protocol_minor_version,
            protocol_command.protocol_command_code,
        )

        del self.protocol_commands[k]

    #
    # tasks
    #
    def remove_dead_contacts(self):
        print(PrintColors.VIOLET, 'remove_dead_contacts:', self, len(self.rt.contacts), len(self.rt.remove_contacts), PrintColors.END)
        t = time.time()

        for c in self.rt.contacts.all():
            if c.id == self.id:
                continue

            if t - c.last_seen > 15.0 + len(self.rt.contacts) + len(self.rt.add_contacts):
                self.rt.contacts.remove(c)
                self.rt.remove_contacts.add(c)
                print(PrintColors.YELLOW, 'remove_dead_contacts:', c, PrintColors.END)

        for c in self.rt.remove_contacts.all():
            if t - c.last_seen > 30.0 + (len(self.rt.contacts) + len(self.rt.add_contacts)) * 2.0:
                self.rt.remove_contacts.remove(c)
                print(PrintColors.RED, 'remove_dead_contacts:', c, PrintColors.END)

        self.loop.call_later(15.0 + random.random() * 15.0, self.remove_dead_contacts)

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

    #
    # message
    #
    def build_message(self, protocol_major_version, protocol_minor_version, protocol_message_type, protocol_command_code, obj):
        obj_data = marshal.dumps(obj)

        message_data = struct.pack(
            '!BBBB',
            protocol_major_version,
            protocol_minor_version,
            protocol_message_type,
            protocol_command_code,
        )

        message_data += obj_data
        return message_data

    def send_message(self, message_data, remote_host, remote_port):
        for pack in self.build_packs(message_data):
            self.sock.sendto(pack, (remote_host, remote_port))

    def build_packs(self, message_data):
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
        protocol_version_major, protocol_version_minor, protocol_message_type, protocol_command_code = struct.unpack('!BBBB', message_header)
        protocol_command = self.get_protocol_command(protocol_version_major, protocol_version_minor, protocol_command_code)

        if protocol_message_type == ProtocolCommand.PROTOCOL_REQ:
            if message_data:
                args, kwargs = marshal.loads(message_data)
            else:
                args, kwargs = (), {}

            protocol_command.on_req(remote_host, remote_port, *args, **kwargs)
        elif protocol_message_type == ProtocolCommand.PROTOCOL_RES:
            obj = marshal.loads(message_data)
            protocol_command.on_res(remote_host, remote_port, obj)
