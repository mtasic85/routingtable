__all__ = ['DateTimeProtocolCommand']

import time
import random
import datetime

from print_colors import PrintColors
from contact import Contact
from protocol_command import ProtocolCommand

class DateTimeProtocolCommand(ProtocolCommand):
    def start(self):
        self.req()

    def stop(self):
        raise NotImplementedError

    def req(self):
        c = self.node.rt.contacts.random(without_id=self.node.id)

        if not c:
            # schedule next discover
            self.node.loop.call_later(0.0 + random.random() * 10.0, self.req)
            return

        args = ()
        kwargs = {}
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
        self.node.loop.call_later(0.0 + random.random() * 10.0, self.req)
    
    def on_req(self, remote_host, remote_port, *args, **kwargs):
        # forward to res
        self.res(remote_host, remote_port, *args, **kwargs)

    def res(self, remote_host, remote_port, *args, **kwargs):
        # response
        res = {
            'datetime': datetime.datetime.utcnow().isoformat(),
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
        dt = res['datetime']
        print('datetime {}:{} {}'.format(remote_host, remote_port, dt))
