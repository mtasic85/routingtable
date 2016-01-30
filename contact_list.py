__all__ = ['ContactList']

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

        # # random contact
        # if random.randint(0, 10) == 5:
        #     # chance is 10% to return boostrap contact
        #     # this is good because if all nodes fail, bootstrap should be
        #     # the most reliable node
        #     c = None

        #     for n in self.items:
        #         if n.bootstrap:
        #             c = n
        #             break
        # elif len(contacts):
        #     c = random.choice(contacts)
        # else:
        #     c = None

        if len(contacts):
            c = random.choice(contacts)
        else:
            c = None

        return c

    def all(self, version=0, max_old=None, max_contacts=None):
        contacts = []

        for c in self.items:
            if c.bootstrap:
                contacts.append(c)
                continue

            # if max_old and c.last_seen and time.time() - c.last_seen > max_old:
            #     continue

            contacts.append(c)

        # # sort by last seen time
        # t = time.time()
        # contacts.sort(key=lambda c: t - c.last_seen)

        # if max_contacts is not None:
        #     if isinstance(max_contacts, int):
        #         contacts = contacts[:max_contacts]
        #     elif isinstance(max_contacts, float):
        #         e = int(len(contacts) * max_contacts)
        #         contacts = contacts[:e]

        return contacts

    def remove_older_than(self, max_old):
        t = time.time()

        for c in self.items[:]:
            if t - c.last_seen > max_old:
                print(PrintColors.YELLOW, 'remove_older_than', self, max_old, c, PrintColors.END)
                self.contacts.remove(c)
                del self.items_id_map[c.id]
                del self.items_raddr_map[c.remote_host, c.remote_port]
