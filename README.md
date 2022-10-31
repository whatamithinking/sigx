# sigx
Async in-memory topic-pattern signal exchange

## Preamble

[blinker](https://github.com/pallets-eco/blinker) is the old pro when it comes to signal exchange in a python program, but it does not have topic-pattern subscriptions.

This package solves the topic-pattern issue with an efficient/performant topic-pattern subscription interface with indexing for performance so that topic patterns are compared to each topic only once.

Documentation consists of what you see here and the docs in the code.

## Table of Contents
<!-- TOC -->

- [sigx](#sigx)
	- [Preamble](#preamble)
	- [Table of Contents](#table-of-contents)
	- [Inspiration](#inspiration)
	- [Technologies](#technologies)
	- [Example](#example)
		- [Code](#code)
		- [Output](#output)

<!-- /TOC -->

## Inspiration
- [blinker](https://github.com/pallets-eco/blinker)

## Technologies
- python >=3.5
- [refutil](https://github.com/ConnorSMaynes/refutil)

## Example

### Code
```python
import time
from typing import *
import logging

import anyio
from sigx import SignalExchange, Message


logging.basicConfig()  # just for showing logs from package normally turned off


def handler(sub_id: str, source: Any, msg: Message):
	# sub id is the subscription id the message is getting sent for
	# this can be useful if one callback handles messages for multiple subscriptions
	# source is the publisher object sending the signal/message
	print(time.time(), 'callback', sub_id, source, msg)


async def async_handler(sub_id: str, source: Any, msg: Message):
	# sub id is the subscription id the message is getting sent for
	# this can be useful if one callback handles messages for multiple subscriptions
	# source is the publisher object sending the signal/message
	await anyio.sleep(0.1)
	print(time.time(), 'callback', sub_id, source, msg)


async def main():
	sx = SignalExchange()

	sx.subscribe(
			topic_pattern=r'parent/.*',
			handler=handler,
		)
	sx.subscribe(
			topic_pattern=r'parent/child.*',
			handler=async_handler,
		)
	await sx.publish(
			publisher=None, 
			message=Message(
					publisher_name=None, 
					topic='parent/child',
					value='my message',
				),
		)
	await sx.publish(
			publisher=None, 
			message=Message(
					publisher_name=None, 
					topic='parent/chipmunk',
					value='my message',
					previous_value='banana',
				),
		)


anyio.run(main)

```

### Output

```text
1667237629.7093604 callback 2e31f3a6594211ed8f48b88a60aec5d0 None Message(publisher_name=None, topic='parent/child', value='my message', previous_value=None, created=datetime.datetime(2022, 10, 31, 17, 33, 49, 609258, tzinfo=datetime.timezone.utc), message_id='0000172337ca6d062cd82792ba09504c')

1667237629.7102206 callback 2e31cc7b594211edbbf1b88a60aec5d0 None Message(publisher_name=None, topic='parent/child', value='my message', previous_value=None, created=datetime.datetime(2022, 10, 31, 17, 33, 49, 609258, tzinfo=datetime.timezone.utc), message_id='0000172337ca6d062cd82792ba09504c')

1667237629.7112095 callback 2e31cc7b594211edbbf1b88a60aec5d0 None Message(publisher_name=None, topic='parent/chipmunk', value='my message', previous_value='banana', created=datetime.datetime(2022, 10, 31, 17, 33, 49, 711209, tzinfo=datetime.timezone.utc), message_id='0000172337ca7319d480c8a36afc5827') 
```
