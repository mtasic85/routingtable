import time
import asyncio

# event loop
loop = asyncio.get_event_loop()

def f(a):
	# print('a:', a, loop.time(), time.time())
	print('a:', a)

	if a == 1000000:
		loop.close()

	loop.call_later(0.0, f, a + 1)

loop.call_soon(f, 0)

# run loop
loop.run_forever()
loop.close()