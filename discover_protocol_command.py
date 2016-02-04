__all__ = ['DiscoverProtocolCommand']

# import gc
import time
import random

from print_colors import PrintColors
from contact import Contact
from protocol_command import ProtocolCommand

class DiscoverProtocolCommand(ProtocolCommand):
    def start(self):
        self.req()

    def stop(self):
        raise NotImplementedError

    def req(self):
        # request
        c = self.node.rt.contacts.random(without_id=self.node.id)

        if not c or c.id is None:
            self.node.loop.call_later(0.0 + random.random() * 10.0, self.req)
            return

        # print('discover_nodes:', c)
        node_id = self.node.id
        node_local_host = self.node.listen_host
        node_local_port = self.node.listen_port

        args = ()

        kwargs = {
            'id': node_id,
            'local_host': node_local_host,
            'local_port': node_local_port,
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

        # force del
        del args
        del kwargs
        del res

        # send message
        self.node.send_message(message_data, c.remote_host, c.remote_port)

        # schedule next discover
        self.node.loop.call_later(0.0 + random.random() * 10.0, self.req)

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
                    print(PrintColors.GREEN + 'new contact [DISCOVERY REQ]:', self.node, c, PrintColors.END)
                else:
                    c = self.node.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.node.rt.add_contacts.remove(c)
                        self.node.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                        print(PrintColors.GREEN + 'new contact [DISCOVERY REQ]:', self.node, c, PrintColors.END)
                    else:
                        # remove_contact
                        c = self.node.rt.remove_contacts.get(node_id)

                        if c:
                            self.node.rt.remove_contacts.remove(c)
                            self.node.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                            print(PrintColors.GREEN + 'new contact [DISCOVERY REQ]:', self.node, c, PrintColors.END)
                        else:
                            c = self.node.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.node.rt.remove_contacts.remove(c)
                                self.node.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
                                print(PrintColors.GREEN + 'new contact [DISCOVERY REQ]:', self.node, c, PrintColors.END)
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
                                print(PrintColors.GREEN + 'new contact [DISCOVERY REQ]:', self.node, c, PrintColors.END)

        # forward to res_discover_nodes
        self.res(remote_host, remote_port, *args, **kwargs)

    def res(self, remote_host, remote_port, *args, **kwargs):
        # response
        node_id = self.node.id
        local_host = self.node.listen_host
        local_port = self.node.listen_port
        contacts = [c.__getstate__() for c in self.node.rt.contacts]

        res = {
            'id': node_id,
            'local_host': local_host,
            'local_port': local_port,
            'contacts': contacts,
        }

        # build message
        message_data = self.node.build_message(
            self.protocol_major_version,
            self.protocol_minor_version,
            self.PROTOCOL_RES,
            self.protocol_command_code,
            res,
        )

        # force del
        del contacts
        del res

        # send message
        self.node.send_message(message_data, remote_host, remote_port)

        # print('gc.is_tracked(contacts) =', gc.is_tracked(contacts))
        # print('gc.collect() =', gc.collect())

    def on_res(self, remote_host, remote_port, res):
        node_id = res['id']
        local_host = res['local_host']
        local_port = res['local_port']
        contacts = res['contacts']
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
                    print(PrintColors.GREEN + 'new contact [DISCOVERY ON RES]:', self.node, c, PrintColors.END)
                else:
                    c = self.node.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.node.rt.add_contacts.remove(c)
                        self.node.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                        print(PrintColors.GREEN + 'new contact [DISCOVERY ON RES]:', self.node, c, PrintColors.END)
                    else:
                        # remove_contact
                        c = self.node.rt.remove_contacts.get(node_id)

                        if c:
                            self.node.rt.remove_contacts.remove(c)
                            self.node.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                            print(PrintColors.GREEN + 'new contact [DISCOVERY ON RES]:', self.node, c, PrintColors.END)
                        else:
                            c = self.node.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.node.rt.remove_contacts.remove(c)
                                self.node.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
                                print(PrintColors.GREEN + 'new contact [DISCOVERY ON RES]:', self.node, c, PrintColors.END)
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
                                print(PrintColors.GREEN + 'new contact [DISCOVERY ON RES]:', self.node, c, PrintColors.END)

        # update discovered nodes/contacts
        for cd in contacts:
            node_id = cd['id']
            local_host = cd['local_host']
            local_port = cd['local_port']
            remote_host = cd['remote_host']
            remote_port = cd['remote_port']
            bootstrap = cd.get('bootstrap', False)

            # update contact's `last_seen`, or add contact
            c = self.node.rt.contacts.get(node_id)
            
            if c:
                c.id = node_id
            else:
                c = self.node.rt.contacts.get((remote_host, remote_port))
            
                if c:
                    c.id = node_id
                else:
                    # add_contact
                    c = self.node.rt.add_contacts.get(node_id)

                    if c:
                        c.id = node_id
                    else:
                        c = self.node.rt.add_contacts.get((remote_host, remote_port))
                    
                        if c:
                            c.id = node_id
                        else:
                            # remove_contact
                            c = self.node.rt.remove_contacts.get(node_id)

                            if c:
                                self.node.rt.remove_contacts.remove(c)
                                self.node.rt.add_contacts.add(c)
                                c.id = node_id
                            else:
                                c = self.node.rt.remove_contacts.get((remote_host, remote_port))
                            
                                if c:
                                    self.node.rt.remove_contacts.remove(c)
                                    self.node.rt.add_contacts.add(c)
                                    c.id = node_id
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
                                    self.node.rt.add_contacts.add(c)
