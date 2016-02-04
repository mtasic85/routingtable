__all__ = ['PingProtocolCommand']

import time
import random

from print_colors import PrintColors
from contact import Contact
from protocol_command import ProtocolCommand

class PingProtocolCommand(ProtocolCommand):
    def start(self):
        self.req()

    def stop(self):
        raise NotImplementedError

    def req(self):
        node_id = self.node.id
        local_host = self.node.listen_host
        local_port = self.node.listen_port

        if random.random() < 0.5:
            # ping contact to be added
            c = self.node.rt.add_contacts.get(0)
        else:
            # ping known contact
            c = self.node.rt.contacts.random(without_id=self.node.id)

        if c:
            # print('ping:', c)
            args = ()

            kwargs = {
                'id': node_id,
                'local_host': local_host,
                'local_port': local_port,
            }

            res = (args, kwargs)
            
            # build message
            message_data = self.node.build_message(
                self.protocol_major_version,
                self.protocol_minor_version,
                self.PROTOCOL_REQ,
                self.protocol_command_code,
                res,
            )

            # send message
            self.node.send_message(message_data, c.remote_host, c.remote_port)

        # schedule next discover
        self.node.loop.call_later(0.0 + random.random() * 0.5, self.req)
    
    def on_req(self, remote_host, remote_port, *args, **kwargs):
        node_id = kwargs['id']
        local_host = kwargs['local_host']
        local_port = kwargs['local_port']
        bootstrap = kwargs.get('bootstrap', False)

        # update contact's `last_seen`, or add contact
        c = self.node.rt.contacts.get(node_id)
        
        if c:
            c.id = node_id
            c.last_seen = time.time()
        else:
            c = self.node.rt.contacts.get((remote_host, remote_port))
        
            if c:
                c.id = node_id
                c.last_seen = time.time()
            else:
                # add_contact
                c = self.node.rt.add_contacts.get(node_id)

                if c:
                    self.node.rt.add_contacts.remove(c)
                    self.node.rt.contacts.add(c)
                    c.id = node_id
                    c.last_seen = time.time()
                    print(PrintColors.GREEN + 'new contact [PING ON REQ]:', self.node, c, PrintColors.END)
                else:
                    c = self.node.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.node.rt.add_contacts.remove(c)
                        self.node.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                        print(PrintColors.GREEN + 'new contact [PING ON REQ]:', self.node, c, PrintColors.END)
                    else:
                        # remove_contact
                        c = self.node.rt.remove_contacts.get(node_id)

                        if c:
                            self.node.rt.remove_contacts.remove(c)
                            self.node.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                            print(PrintColors.GREEN + 'new contact [PING ON REQ]:', self.node, c, PrintColors.END)
                        else:
                            c = self.node.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.node.rt.remove_contacts.remove(c)
                                self.node.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
                                print(PrintColors.GREEN + 'new contact [PING ON REQ]:', self.node, c, PrintColors.END)
                            else:
                                c = Contact(
                                    id = node_id,
                                    local_host = local_host,
                                    local_port = local_port,
                                    remote_host = remote_host,
                                    remote_port = remote_port,
                                    bootstrap = bootstrap,
                                )

                                # because `c` is requesting to discover nodes
                                # put it into known active contacts
                                c.last_seen = time.time()
                                self.node.rt.contacts.add(c)
                                print(PrintColors.GREEN + 'new contact [PING ON REQ]:', self.node, c, PrintColors.END)

        # forward to res_discover_nodes
        self.res(remote_host, remote_port, *args, **kwargs)

    def res(self, remote_host, remote_port, *args, **kwargs):
        # response
        node_id = self.node.id
        local_host = self.node.listen_host
        local_port = self.node.listen_port
        
        res = {
            'id': node_id,
            'local_host': local_host,
            'local_port': local_port,
        }

        # build message
        message_data = self.node.build_message(
            self.protocol_major_version,
            self.protocol_minor_version,
            self.PROTOCOL_RES,
            self.protocol_command_code,
            res,
        )

        # send message
        self.node.send_message(message_data, remote_host, remote_port)

    def on_res(self, remote_host, remote_port, res):
        node_id = res['id']
        local_host = res['local_host']
        local_port = res['local_port']
        bootstrap = res.get('bootstrap', False)

        # update contact's `last_seen`, or add contact
        c = self.node.rt.contacts.get(node_id)
        
        if c:
            c.id = node_id
            c.last_seen = time.time()
        else:
            c = self.node.rt.contacts.get((remote_host, remote_port))
        
            if c:
                c.id = node_id
                c.last_seen = time.time()
            else:
                # add_contact
                c = self.node.rt.add_contacts.get(node_id)

                if c:
                    self.node.rt.add_contacts.remove(c)
                    self.node.rt.contacts.add(c)
                    c.id = node_id
                    c.last_seen = time.time()
                    print(PrintColors.GREEN + 'new contact [PING ON RES]:', self.node, c, PrintColors.END)
                else:
                    c = self.node.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.node.rt.add_contacts.remove(c)
                        self.node.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                        print(PrintColors.GREEN + 'new contact [PING ON RES]:', self.node, c, PrintColors.END)
                    else:
                        # remove_contact
                        c = self.node.rt.remove_contacts.get(node_id)

                        if c:
                            self.node.rt.remove_contacts.remove(c)
                            self.node.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                            print(PrintColors.GREEN + 'new contact [PING ON RES]:', self.node, c, PrintColors.END)
                        else:
                            c = self.node.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.node.rt.remove_contacts.remove(c)
                                self.node.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
                                print(PrintColors.GREEN + 'new contact [PING ON RES]:', self.node, c, PrintColors.END)
                            else:
                                c = Contact(
                                    id = node_id,
                                    local_host = local_host,
                                    local_port = local_port,
                                    remote_host = remote_host,
                                    remote_port = remote_port,
                                    bootstrap = bootstrap,
                                )

                                # because `c` is requesting to discover nodes
                                # put it into known active contacts
                                c.last_seen = time.time()
                                self.node.rt.contacts.add(c)
                                print(PrintColors.GREEN + 'new contact [PING ON RES]:', self.node, c, PrintColors.END)
