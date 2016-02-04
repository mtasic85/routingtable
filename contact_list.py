__all__ = ['ContactList']

import random

from contact import Contact
from print_colors import PrintColors

class ContactList(object):
    def __init__(self):
        self.items = []
        self.items_id_map = {}
        self.items_raddr_map = {}

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def add(self, c):
        if c.id is None and not c.bootstrap:
            raise ValueError('Contact it cannot be None, it its is not bootstrap node')

        if c.id is None and c.id in self.items_id_map:
            raise ValueError('Bootstrap contact with id=None is already known')

        self.items.append(c)
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

    def random(self, without_id=None):
        if not len(self.items):
            return None

        # filter contacts
        i = random.randint(0, len(self.items) - 1)
        c = self.items[i]

        if c.id == without_id:
            return None

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
