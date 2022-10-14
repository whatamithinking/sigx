# sigx
Topic-pattern signal exchange with in-memory caching for late joiners

## Preamble

[blinker](https://github.com/pallets-eco/blinker) is the old pro when it comes to signal exchange in a python program, but it does not have topic-pattern subscriptions or do anything about late joiners itself.

This package solves the topic-pattern issue with an efficient/performant topic-pattern subscription interface with indexing for performance so that topic patterns are compared to each topic only once.

This package also solves the late joiner problem by way of a built-in cache which can optionally be used each time a signal/message/event is published. When new subscriptions are created, the subscriber has the option to `initialize` itself with cached values for that topic.

Async and sync handlers & filters are supported.

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

from sigx import SignalExchange, Message


logging.basicConfig()  # just for showing logs from package normally turned off


def handler(sub_id: str, source: Any, msg: Message):
	# sub id is the subscription id the message is getting sent for
	# this can be useful if one callback handles messages for multiple subscriptions
	# source is the publisher object sending the signal/message
	print(time.time(), 'callback', sub_id, source, msg)


sx = SignalExchange()

sx.publish(
		publisher=None, 
		message=Message(
				publisher_name=None, 
				topic='parent/child',
				value='my message',
			),
		skip_no_change=True,
		cache=True,
	)

# example of late joiner
# the handler can subscribe late and still receive the most recent message
# because the publisher used the cache
sx.subscribe(
		topic_pattern=r'parent/.*',
		handler=handler,
		initialize=True,
	)

# subscriber does not receive message because the cache is being used
# and skip no change is True and the value is not changing from the last one
# for the topic
sx.publish(
		publisher=None, 
		message=Message(
				publisher_name=None, 
				topic='parent/child',
				value='my message',
			),
		skip_no_change=True,
		cache=True,
	)
```

### Output

A warning is shown because the publisher cannot be weakly referenced and the cache is being used, so the publisher will be kept alive until the SignalExchange reference is lost.

In most cases, the publisher is an object which can be weakly referenced so the data in the cache will be garbage collected soon after the object is.

```text
WARNING:sigx:The publisher cannot be weakly referenced. A hard ref will be held by the cache indefinitely.
1654608794.8128855 callback 62132bc6e66611eca2b7b88a60aec5d0 None Message(publisher_name=None, topic='parent/child', value='my message', previous_value=None, created=datetime.datetime(2022, 6, 7, 9, 33, 14, 807888), message_id='62128f29e66611ec8fd0b88a60aec5d0')
```
