__all__ = ['RoutingTable']

from contact_list import ContactList

class RoutingTable(object):
    def __init__(self):
        self.version = 0 # FIXME: not used, but should be
        self.contacts = ContactList()           # healthy "green" contacts
        self.add_contacts = ContactList()       # to be checked "blue" contacts
        self.remove_contacts = ContactList()    # missing "yellow" contacts
