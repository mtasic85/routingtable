import json
import asyncio

from routingtable import Node, Contact

# event loop
loop = asyncio.get_event_loop()

with open('node0.json', 'r') as f:
    node_config = json.load(f)

node = Node(
    loop,
    id = node_config['id'],
    listen_host = node_config['listen_host'],
    listen_port = node_config['listen_port'],
)

for cd in node_config['contacts']:
    c = Contact(**cd)
    node.rt.add(c)

# run loop
loop.run_forever()
loop.close()
