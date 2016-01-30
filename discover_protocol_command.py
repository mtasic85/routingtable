__all__ = ['DiscoverProtocolCommand']

from protocol_command import ProtocolCommand

class DiscoverProtocolCommand(ProtocolCommand):
    def discover_nodes(self):
        # request
        c = self.rt.contacts.random(without_id=self.id)

        if not c or c.id is None:
            self.loop.call_later(0.0 + random.random() * 10.0, self.discover_nodes)
            return

        # print('discover_nodes:', c)
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
        self.loop.call_later(0.0 + random.random() * 10.0, self.discover_nodes)

    def on_req_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
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
                else:
                    c = self.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.rt.add_contacts.remove(c)
                        self.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                    else:
                        # remove_contact
                        c = self.rt.remove_contacts.get(node_id)

                        if c:
                            self.rt.remove_contacts.remove(c)
                            self.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                        else:
                            c = self.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.rt.remove_contacts.remove(c)
                                self.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
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
                                # print(PrintColors.GREEN, 'on_req_discover_nodes:', c, PrintColors.END)

        # forward to res_discover_nodes
        self.res_discover_nodes(remote_host, remote_port, *args, **kwargs)

    def res_discover_nodes(self, remote_host, remote_port, *args, **kwargs):
        # response
        node_id = self.id
        local_host = self.listen_host
        local_port = self.listen_port
        contacts = [c.__getstate__() for c in self.rt.contacts.all()]

        res = {
            'id': node_id,
            'local_host': local_host,
            'local_port': local_port,
            'contacts': contacts,
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

    def on_res_discover_nodes(self, remote_host, remote_port, res):
        node_id = res['id']
        local_host = res['local_host']
        local_port = res['local_port']
        contacts = res['contacts']
        bootstrap = res.get('bootstrap', False)

        # print('on_res_discover_nodes:', remote_host, remote_port, len(contacts), (len(self.rt.contacts), len(self.rt.add_contacts), len(self.rt.remove_contacts)))

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
                else:
                    c = self.rt.add_contacts.get((remote_host, remote_port))
                
                    if c:
                        self.rt.add_contacts.remove(c)
                        self.rt.contacts.add(c)
                        c.id = node_id
                        c.last_seen = time.time()
                    else:
                        # remove_contact
                        c = self.rt.remove_contacts.get(node_id)

                        if c:
                            self.rt.remove_contacts.remove(c)
                            self.rt.contacts.add(c)
                            c.id = node_id
                            c.last_seen = time.time()
                        else:
                            c = self.rt.remove_contacts.get((remote_host, remote_port))
                        
                            if c:
                                self.rt.remove_contacts.remove(c)
                                self.rt.contacts.add(c)
                                c.id = node_id
                                c.last_seen = time.time()
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
                                # print(PrintColors.GREEN, 'on_res_discover_nodes:', c, PrintColors.END)

        # update discovered nodes/contacts
        for cd in contacts:
            node_id = cd['id']
            local_host = cd['local_host']
            local_port = cd['local_port']
            remote_host = cd['remote_host']
            remote_port = cd['remote_port']
            bootstrap = cd.get('bootstrap', False)

            # update contact's `last_seen`, or add contact
            c = self.rt.contacts.get(node_id)
            
            if c:
                c.id = node_id
                # c.last_seen = time.time()
            else:
                c = self.rt.contacts.get((remote_host, remote_port))
            
                if c:
                    c.id = node_id
                    # c.last_seen = time.time()
                else:
                    # add_contact
                    c = self.rt.add_contacts.get(node_id)

                    if c:
                        c.id = node_id
                        # c.last_seen = time.time()
                        # print(PrintColors.GREEN, 'on_res_discover_nodes [0]:', c, PrintColors.END)
                    else:
                        c = self.rt.add_contacts.get((remote_host, remote_port))
                    
                        if c:
                            c.id = node_id
                            # c.last_seen = time.time()
                            # print(PrintColors.GREEN, 'on_res_discover_nodes [1]:', c, PrintColors.END)
                        else:
                            # remove_contact
                            c = self.rt.remove_contacts.get(node_id)

                            if c:
                                self.rt.remove_contacts.remove(c)
                                self.rt.add_contacts.add(c)
                                c.id = node_id
                                # c.last_seen = time.time()
                                # print(PrintColors.GREEN, 'on_res_discover_nodes [2]:', c, PrintColors.END)
                            else:
                                c = self.rt.remove_contacts.get((remote_host, remote_port))
                            
                                if c:
                                    self.rt.remove_contacts.remove(c)
                                    self.rt.add_contacts.add(c)
                                    c.id = node_id
                                    # c.last_seen = time.time()
                                    # print(PrintColors.GREEN, 'on_res_discover_nodes [3]:', c, PrintColors.END)
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
                                    self.rt.add_contacts.add(c)
                                    # print(PrintColors.GREEN, 'on_res_discover_nodes [4]:', c, PrintColors.END)
