__all__ = ['ContactList']

import random

from contact import Contact

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

    def get(self, id_or_remote_address_or_idx):
        c = None

        if isinstance(id_or_remote_address_or_idx, (str, bytes)):
            c_id = id_or_remote_address_or_idx
            
            try:
                c = self.items_id_map[c_id]
            except KeyError as e:
                pass
        elif isinstance(id_or_remote_address_or_idx, (tuple, list)):
            remote_host, remote_port = id_or_remote_address_or_idx

            try:
                c = self.items_raddr_map[remote_host, remote_port]
            except KeyError as e:
                pass
        elif isinstance(id_or_remote_address_or_idx, int):
            i = id_or_remote_address_or_idx

            try:
                c = self.items[i]
            except IndexError as e:
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

        if len(contacts):
            c = random.choice(contacts)
        else:
            c = None

        return c

    def all(self, version=0, max_old=None):
        contacts = []

        for c in self.items:
            if c.bootstrap:
                contacts.append(c)
                continue

            # FIXME: use version and max_old
            contacts.append(c)

        return contacts
