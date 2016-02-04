import os
import sys
sys.path.append(os.path.abspath('..'))

import json
import asyncio

from node import Node
from contact import Contact

from datetime_protocol_command import DateTimeProtocolCommand

# event loop
loop = asyncio.get_event_loop()

with open('nodeN.json', 'r') as f:
    node_config = json.load(f)

for i in range(100):
    node = Node(
        loop,
        id = node_config['id'],
        listen_host = node_config['listen_host'],
        listen_port = node_config['listen_port'] + i,
        bootstrap = node_config.get('bootstrap', False),
    )

    pc = DateTimeProtocolCommand(node, 1, 0, 10)
    node.add_protocol_command(pc)

    for cd in node_config['contacts']:
        c = Contact(**cd)
        node.rt.add_contacts.add(c)

# run loop
loop.run_forever()
loop.close()
