__all__ = ['PingProtocolCommand']

from protocol_command import ProtocolCommand

class PingProtocolCommand(ProtocolCommand):
    def ping(self):
        node_id = self.id
        local_host = self.listen_host
        local_port = self.listen_port

        if random.random() < 0.5:
            # ping contact to be added
            c = self.rt.add_contacts.get(0)
        else:
            # ping known contact
            c = self.rt.contacts.random(without_id=self.id)

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
            message_data = self.build_message(
                self.NODE_PROTOCOL_VERSION_MAJOR,
                self.NODE_PROTOCOL_VERSION_MINOR,
                self.NODE_PROTOCOL_REQ,
                self.NODE_PROTOCOL_DISCOVER_NODES,
                res,
            )

            # send message
            self.send_message(message_data, c.remote_host, c.remote_port)

        # schedule next discover
        self.loop.call_later(0.0 + random.random() * 0.5, self.ping)
    
    def on_req_ping(self, remote_host, remote_port, *args, **kwargs):
        node_id = kwargs['id']
        local_host = kwargs['local_host']
        local_port = kwargs['local_port']
        bootstrap = kwargs.get('bootstrap', False)

        # update contact's `last_seen`, or add contact
        c = self.rt.contacts.get(node_id)
        
        if c:
            c.id = node_id
            c.last_seen = time.time()
        else:
            c = self.rt.contacts.get((remote_host, remote_port))
        
            if c:
                c.id = node_id
                c.last_seen = time.time()
            else:
                # add_contact
                c = self.rt.add_contacts.get(node_id)

                if c:
                    self.rt.add_contacts.remove(c)
                    self.rt.contacts.add(c)
                    c.id = node_id
                    c.last_seen = time.time()
                    # print(PrintColors.GREEN, 'on_req_ping [0]:', c, PrintColors.END)
                else:
                    c = self.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.rt.add_contacts.remove(c)
                        self.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                        # print(PrintColors.GREEN, 'on_req_ping [1]:', c, PrintColors.END)
                    else:
                        # remove_contact
                        c = self.rt.remove_contacts.get(node_id)

                        if c:
                            self.rt.remove_contacts.remove(c)
                            self.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                            # print(PrintColors.GREEN, 'on_req_ping [2]:', c, PrintColors.END)
                        else:
                            c = self.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.rt.remove_contacts.remove(c)
                                self.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
                                # print(PrintColors.GREEN, 'on_req_ping [3]:', c, PrintColors.END)
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
                                self.rt.contacts.add(c)
                                # print(PrintColors.GREEN, 'on_req_ping [4]:', c, PrintColors.END)

        # forward to res_discover_nodes
        self.res_ping(remote_host, remote_port, *args, **kwargs)

    def res_ping(self, remote_host, remote_port, *args, **kwargs):
        # response
        node_id = self.id
        local_host = self.listen_host
        local_port = self.listen_port
        
        res = {
            'id': node_id,
            'local_host': local_host,
            'local_port': local_port,
        }

        # build message
        message_data = self.build_message(
            self.NODE_PROTOCOL_VERSION_MAJOR,
            self.NODE_PROTOCOL_VERSION_MINOR,
            self.NODE_PROTOCOL_RES,
            self.NODE_PROTOCOL_DISCOVER_NODES,
            res,
        )

        # send message
        self.send_message(message_data, remote_host, remote_port)

    def on_res_ping(self, remote_host, remote_port, res):
        # print('on_res_ping:', remote_host, remote_port, (len(self.rt.contacts), len(self.rt.add_contacts), len(self.rt.remove_contacts)))

        node_id = res['id']
        local_host = res['local_host']
        local_port = res['local_port']
        bootstrap = res.get('bootstrap', False)

        # update contact's `last_seen`, or add contact
        c = self.rt.contacts.get(node_id)
        
        if c:
            c.id = node_id
            c.last_seen = time.time()
        else:
            c = self.rt.contacts.get((remote_host, remote_port))
        
            if c:
                c.id = node_id
                c.last_seen = time.time()
            else:
                # add_contact
                c = self.rt.add_contacts.get(node_id)

                if c:
                    self.rt.add_contacts.remove(c)
                    self.rt.contacts.add(c)
                    c.id = node_id
                    c.last_seen = time.time()
                    # print(PrintColors.GREEN, 'on_res_ping [0]:', c, PrintColors.END)
                else:
                    c = self.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.rt.add_contacts.remove(c)
                        self.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                        # print(PrintColors.GREEN, 'on_res_ping [1]:', c, PrintColors.END)
                    else:
                        # remove_contact
                        c = self.rt.remove_contacts.get(node_id)

                        if c:
                            self.rt.remove_contacts.remove(c)
                            self.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                            # print(PrintColors.GREEN, 'on_res_ping [2]:', c, PrintColors.END)
                        else:
                            c = self.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.rt.remove_contacts.remove(c)
                                self.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
                                # print(PrintColors.GREEN, 'on_res_ping [3]:', c, PrintColors.END)
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
                                self.rt.contacts.add(c)
                                # print(PrintColors.GREEN, 'on_res_ping [4]:', c, PrintColors.END)