__all__ = ['Contact']

class Contact(object):
    def __init__(self, id=None, local_host=None, local_port=None, remote_host=None, remote_port=None, bootstrap=False, version=None):
        self.id = id
        self.local_host = local_host
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.bootstrap = bootstrap
        self.version = version
        self.last_seen = None

    def __repr__(self):
        return '<{}:{} local={}:{} remote={}:{} bootstrap={}>'.format(
            self.__class__.__name__,
            self.id,
            self.local_host,
            self.local_port,
            self.remote_host,
            self.remote_port,
            self.bootstrap,
        )

    def __getstate__(self):
        return {
            'id': self.id,
            'local_host': self.local_host,
            'local_port': self.local_port,
            'remote_host': self.remote_host,
            'remote_port': self.remote_port,
            'bootstrap': self.bootstrap,
        }
