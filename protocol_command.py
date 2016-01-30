__all__ = ['ProtocolCommand']

class ProtocolCommand(object):
    # protocol version 1.0
    DEFAULT_NODE_PROTOCOL_VERSION_MAJOR = 1
    DEFAULT_NODE_PROTOCOL_VERSION_MINOR = 0
    
    # protocol message types
    NODE_PROTOCOL_REQ = 0
    NODE_PROTOCOL_RES = 1

    def __init__(self, node, protocol_major_version, protocol_minor_version, protocol_command_code):
        self.node = node
        self.protocol_major_version = protocol_major_version
        self.protocol_minor_version = protocol_minor_version
        self.protocol_command_code = protocol_command_code
    
    def req(self):
        raise NotImplementedError
    
    def on_req(self, remote_host, remote_port, *args, **kwargs):
        raise NotImplementedError

    def res(self, remote_host, remote_port, *args, **kwargs):
        raise NotImplementedError

    def on_res(self, remote_host, remote_port, res):
        raise NotImplementedError
