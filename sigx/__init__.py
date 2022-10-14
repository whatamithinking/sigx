from dataclasses import dataclass, field
from enum import Enum
from typing import *
import re
import uuid
import threading
import weakref
import logging
import datetime
import asyncio
import inspect
from collections import namedtuple

import refutil

__all__ = [
	'Message',
	'InvalidTopicError',
	'InvalidTopicPatternError',
	'SignalExchange',
]


__version__ = '1.1.0'

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


@dataclass
class Message:
	"""A standard message envelope for all signals/events/messages."""
	publisher_name: Optional[str]
	"""Name of the publisher sending the message."""
	topic: str
	"""Topic on which the signal is being published."""
	value: Any
	"""The current value being sent for the signal."""
	previous_value: Any = None
	"""Optional previous value for this topic and publisher."""
	created: datetime.datetime = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))
	"""Datetime when the message was created."""
	message_id: str = field(default_factory=lambda: uuid.uuid1().hex)
	"""A uuid for the message."""


class InvalidTopicError(ValueError):
	"""Raised when a topic is of an invalid format."""


class InvalidTopicPatternError(ValueError):
	"""Raised when a topic pattern is of an invalid format
	(i.e. it is not a valid regular expression)."""


class _TopicPattern:
	__slots__ = '__weakref__', '_topic_pattern', '_regex', '_topics', '_subscription_ids'

	def __init__(self, topic_pattern: str) -> None:
		self._topic_pattern = topic_pattern
		self._regex: Optional[re.Pattern] = re.compile(topic_pattern)
		self._topics: Set[_Topic] = set()
		self._subscription_ids: Set[str] = set()

	def __repr__(self) -> str:
		return f'{self.__class__.__name__}(topic_pattern="{self._topic_pattern}")'

	@classmethod
	def is_valid(cls, topic_pattern: str) -> bool:
		try:
			re.compile(topic_pattern)
		except re.error:
			return False
		else:
			return True

	@classmethod
	def check_valid(cls, topic_pattern: str) -> None:
		if not cls.is_valid(topic_pattern):
			raise InvalidTopicPatternError(f'Invalid topic pattern: {topic_pattern}')

	def __hash__(self) -> int:
		return hash(self._topic_pattern)

	def __eq__(self, other: '_TopicPattern') -> bool:
		return hash(self) == hash(other)

	@property
	def topic_pattern(self) -> str:
		return self._topic_pattern

	def matches(self, topic: Union[str, '_Topic']) -> bool:
		if isinstance(topic, _Topic):
			topic = topic.topic
		return self._regex.match(topic) is not None
	
	def _add_topic(self, topic: '_Topic') -> None:
		self._topics.add(topic)

	@property
	def topics(self) -> Set['_Topic']:
		return self._topics

	def _add_subscription(self, subscription_id: str) -> None:
		self._subscription_ids.add(subscription_id)
	
	def _remove_subscription(self, subscription_id: str) -> None:
		self._subscription_ids.remove(subscription_id)
		if not self._subscription_ids: self._subscription_ids.clear()  # release mem
	
	@property
	def subscription_ids(self) -> Set[str]:
		return self._subscription_ids


class _TopicPatterns:
	__slots__ = '__weakref__', '_topic_patterns', '_topics'

	def __init__(self) -> None:
		# weak refs to topic patterns so that they live only so long
		# as there is at least one subscription for them
		# other alternative was to list subscription ids, but weak refs
		# should be more memory efficient
		self._topic_patterns: Dict[str, _TopicPattern] = weakref.WeakValueDictionary()
		self._topics: Optional[weakref.ReferenceType[_Topics]] = None

	def __getitem__(self, topic_pattern: str) -> _TopicPattern:
		return self._topic_patterns[topic_pattern]

	def __bool__(self) -> bool:
		return bool(self._topic_patterns)

	def _add(self, topic_pattern: str) -> None:
		try:
			tp = self._topic_patterns[topic_pattern]
		except KeyError:
			# when new topic pattern created, create an index of all topics
			# that pattern matches out of known topics
			# topics stick around in memory so long as there are subscriptions / topic patterns
			# for them, so each topic pattern has a hard ref to all topics it matches
			self._topic_patterns[topic_pattern] = tp = _TopicPattern(topic_pattern)
			for topic in self._topics():
				if not tp.matches(topic): continue
				tp._add_topic(topic)
		return tp
	
	def __iter__(self) -> Iterable[_TopicPattern]:
		return iter(self._topic_patterns.values())


def _run_sync(func, *args, **kwargs):
	if inspect.iscoroutinefunction(func):
		return asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))
	else:
		return func(*args, **kwargs)


_Subscription = namedtuple('_Subscription', 
	['topic_pattern', 'subscription_id', 'handler', 'filter', 'publisher_name'])
_SubscriptionWeakRef = refutil.reftype('_SubscriptionWeakRef', ['subscription_id'])


class _Subscriptions:
	
	def __init__(self, cache: '_Cache', lock: threading.RLock) -> None:
		self._cache = cache
		self._lock = lock
		# subscription id: _Subscription
		self._subscriptions: Dict[str, _Subscription] = {}
		self._topic_patterns: Optional[weakref.ReferenceType[_TopicPatterns]] = None
		self._topics: Optional[weakref.ReferenceType[_Topics]] = None
	
	def __len__(self) -> int:
		return len(self._subscriptions)

	def __bool__(self) -> bool:
		return bool(self._subscriptions)

	def __getitem__(self, subscription_id: str) -> _Subscription:
		return self._subscriptions[subscription_id]

	def _weak_subscription_cleanup(self, ref: _SubscriptionWeakRef) -> None:
		# auto-delete subscription when weakref to handler or filter function is gc'ed
		try:
			self._remove(ref.subscription_id)
		except KeyError:  # if weakref hangs around after subscription
			# already removed, which can happen when things are not properly
			# shutdown, then the subscription may no longer be present
			# this can be safely ignored.
			log.warning('Signal exchange subscription weakref lingering ' + \
				'after subscription deleted. This is likely because the app ' + \
				'failed to close properly.', extra=dict(subscription_id=ref.subscription_id))

	def _add(self, topic_pattern: Union[str, Enum], handler: Callable[[str, Any, Message], None], 
		publisher_name: str = 'any', filter: Optional[Callable[[str, Any, Message], bool]] = None, 
		subscription_id: Optional[str] = None,
		weak_handler: bool = True, weak_filter: bool = True, initialize: bool = False) -> str:
		"""Create a subscription and return the subscription id.
		
		Args:
			topic_pattern: A regular expression string or enum to match topics against.
			handler: Async/sync callable to call with new messages for the subscription, which
				takes subscription id, publisher, and the message as arguments.
			publisher_name: Optional. The name of the publisher to receive messages
				from. Defaults to "any" which listens for messages from any publisher.
			filter: Async/sync callable to call with new messages for the subscription, which
				takes subscription id, publisher, and the message as arguments.
				If it returns True, the message will be given to the handler callable;
				otherwise, the message will be dropped.
			subscription_id: Optional. UUID for the subscription. Defaults to uuid1 hex.
			weak_handler: Optional. True if a weakref to the handler callable should
				be used and False otherwise. Defaults to True.
			weak_filter: Optional. True if a weakref to the filter callable should
				be used and False otherwise. Defaults to True.
			initialize: Optional. True if the initial latest values for all matching
				topics and publishers should be sent to the subscription when it is created,
				handling the late-joiner problem. Defaults to False.
		
		Returns:
			subscription_id: uuid hex of the subscription.
		"""
		topic_pattern = topic_pattern.value if isinstance(topic_pattern, Enum) else topic_pattern
		with self._lock:
			if subscription_id in self._subscriptions:
				raise KeyError(f'Subscription id already exists: {subscription_id}')
			
			subscription_id = subscription_id or uuid.uuid1().hex
			
			sub_handler = handler
			if weak_handler:
				sub_handler = _SubscriptionWeakRef(handler, callback=self._weak_subscription_cleanup, 
					subscription_id=subscription_id)
			
			sub_filter = filter
			if filter and weak_filter:
				sub_filter = _SubscriptionWeakRef(filter, callback=self._weak_subscription_cleanup, 
					subscription_id=subscription_id)
			
			topic_pattern = self._topic_patterns()._add(topic_pattern)
			topic_pattern._add_subscription(subscription_id)
			
			subscription = _Subscription(
					topic_pattern=topic_pattern,
					subscription_id=subscription_id,
					handler=sub_handler,
					filter=sub_filter,
					publisher_name=publisher_name,
				)
			self._subscriptions[subscription_id] = subscription

			for topic in topic_pattern.topics:
				
				if initialize:
					# before creating the subscription, push the latest values for all publishers
					# matching the subscription to the handler so it has the initial state
					# done to avoid late joiner problems
					pub_names = []
					if publisher_name == 'any':
						pub_names += self._cache.publishers(topic.topic)
					else:
						pub_names.append(publisher_name)
					previous_messages = []
					for pub_name in pub_names:
						pub, previous_message = self._cache.get(pub_name, topic.topic)
						if not previous_message: continue
						previous_messages.append((pub, previous_message))
					# order the messages so the handler gets them in the order they happened
					# relative to one another. this can be important in some cases.
					previous_messages.sort(key=lambda _: _[1].created)
					for pub, previous_message in previous_messages:
						if filter:
							try:
								if not _run_sync(filter, subscription_id, pub, previous_message):
									continue
							except:
								log.exception('Exception raised by signal filter.')
								continue
						try:
							_run_sync(handler, subscription_id, pub, previous_message)
						except:
							log.exception('Exception raised by signal handler.')

				self._topics()._add_subscription(topic, publisher_name, subscription_id)
			
		return subscription_id
	
	def _remove(self, subscription_id: str) -> None:
		"""Remove a subscription.
		
		Args:
			subscription_id: The uuid of the subscription to remove.
		"""
		with self._lock:
			subscription = self._subscriptions.pop(subscription_id)
			if not self._subscriptions: self._subscriptions.clear()  # release mem
			publisher_name = subscription.publisher_name
			topic_pattern = subscription.topic_pattern
			topic_pattern._remove_subscription(subscription_id)
			for topic in topic_pattern.topics:
				self._topics()._remove_subscription(topic, publisher_name, subscription_id)


class _TopicPublisher:
	"""Set-like collection of subscription ids for a topic publisher.
	
	This is used to keep track of all the subscriptions for a topic for
	a publisher so that they can be looked up in O(1) time during publishing.
	"""
	__slots__ = '_publisher', '_subscription_ids'

	def __init__(self, publisher: str) -> None:
		self._publisher = publisher
		self._subscription_ids: Set[str] = set()

	@property
	def publisher(self) -> str:
		return self._publisher

	def _add(self, subscription_id: str) -> None:
		self._subscription_ids.add(subscription_id)
		
	def _remove(self, subscription_id: str) -> None:
		self._subscription_ids.remove(subscription_id)
		if not self._subscription_ids: self._subscription_ids.clear()  # release mem
	
	@property
	def subscription_ids(self) -> Set[str]:
		return self._subscription_ids


class _TopicPublishers:
	"""Set-like collection of topic publishers."""
	__slots__ = '_publishers'

	def __init__(self) -> None:
		self._publishers: Dict[str, _TopicPublisher] = {}
	
	def __getitem__(self, publisher: str) -> _TopicPublisher:
		return self._publishers[publisher]

	def __bool__(self) -> bool:
		return bool(self._publishers)

	def _add(self, publisher: str) -> _TopicPublisher:
		try:
			topic_publisher = self._publishers[publisher]
		except KeyError:
			topic_publisher = _TopicPublisher(publisher)
			self._publishers[publisher] = topic_publisher
		return topic_publisher
	
	def _remove(self, publisher: str) -> _TopicPublisher:
		topic_publisher = self._publishers.pop(publisher)
		if not self._publishers: self._publishers.clear()  # release mem
		return topic_publisher

	def __iter__(self) -> Iterator[_TopicPublisher]:
		return iter(self._publishers.values())


class _Topic:
	__slots__ = '_topic', 'publishers'
	# using hyphens because of https://stackoverflow.com/questions/10302179/hyphen-underscore-or-camelcase-as-word-delimiter-in-uris
	_REGEX = re.compile(r'^(?:(?:[a-z]+(?:-[a-z]+)*)(?:/(?=[a-z]))?)*$')

	def __init__(self, topic: str) -> None:
		self.check_valid(topic)
		self._topic = topic
		self.publishers = _TopicPublishers()

	def __hash__(self) -> int:
		return hash(self._topic)

	def __repr__(self) -> str:
		return f'{self.__class__.__name__}(topic="{self._topic}")'

	@classmethod
	def is_valid(cls, topic: str) -> bool:
		return cls._REGEX.match(topic) is not None

	@classmethod
	def check_valid(self, topic: str) -> None:
		if not self.is_valid(topic):
			raise InvalidTopicError(f'Invalid topic: {topic}')

	@property
	def topic(self) -> str:
		return self._topic


class _Topics:
	"""Index of topics, publishers, and subscriptions for efficient lookup
	during publishing."""

	def __init__(self, lock: threading.RLock) -> None:
		self._lock = lock
		self._topics: Dict[str, _Topic] = {}
		self._topic_patterns: Optional[weakref.ReferenceType[_TopicPatterns]] = None
		self._subscriptions: Optional[weakref.ReferenceType[_Subscriptions]] = None

	def __getitem__(self, topic: str) -> _Topic:
		return self._topics[topic]

	def __bool__(self) -> bool:
		return bool(self._topics)

	def _add(self, topic: str) -> _Topic:
		with self._lock:
			try:
				topic = self._topics[topic]
			except KeyError:
				self._topics[topic] = topic = _Topic(topic)
				# on creation of a new topic we need to check all topic patterns
				# to see which ones match and update indices
				for topic_pattern in self._topic_patterns():
					if not topic_pattern.matches(topic): continue
					topic_pattern._add_topic(topic)
					# map topic to different publishers and then to different
					# subscription ids for O(1) lookup when a publisher sends a message for a topic
					for subscription_id in topic_pattern.subscription_ids:
						publisher_name = self._subscriptions()[subscription_id].publisher_name
						self._add_subscription(topic, publisher_name, subscription_id)
			return topic

	def _add_subscription(self, topic: Union[str, _Topic], publisher: str, subscription_id: str) -> None:
		with self._lock:
			if isinstance(topic, _Topic):
				topic = topic.topic
			topic = self._topics[topic]
			topic_publisher = topic.publishers._add(publisher)
			topic_publisher._add(subscription_id)
	
	def _remove_subscription(self, topic: Union[str, _Topic], publisher: str, subscription_id: str) -> None:
		with self._lock:
			if isinstance(topic, _Topic):
				topic = topic.topic
			topic = self._topics[topic]
			topic_publisher = topic.publishers[publisher]
			topic_publisher._remove(subscription_id)
			if not topic_publisher.subscription_ids:
				topic.publishers._remove(publisher)

	def __iter__(self) -> Iterator[_Topic]:
		return iter(self._topics.values())
		

_PublisherWeakRef = refutil.reftype('_PublisherWeakRef', ['publisher_name'])


class _Cache:
	"""Cache of last messages for every topic/publisher pairing where
	caching is enabled.
	
	Used to solve the late joiner problem, where a subscriber will join
	after signals have already been sent and still need to know the current
	state of every publisher it is subscribed to.

	Weak references to publishers are maintained so that all the data
	for a publisher can be dumped when the object is garbage collected. 
	This must be done, otherwise as many ephemeral publishers come and go,
	memory will leak. A side effect of this design is that a publisher with the same name 
	may be destroyed and recreated at different points and the last value
	will not be available each time the publisher is recreated.
	"""
	
	def __init__(self, lock: threading.RLock) -> None:
		self._lock = lock
		# topic: publisher name: message
		self._msgcache: Dict[str, Dict[str, Message]] = {}
		# publisher name: (weakref, topics)
		self._r: Dict[str, Tuple[weakref.ref, List[str]]] = {}

	def _weak_cleanup(self, ref: _PublisherWeakRef) -> None:
		with self._lock:
			topics = self._r.pop(ref.publisher_name)[1]
			for topic in topics:
				tdict = self._msgcache[topic]
				del tdict[ref.publisher_name]
				if not tdict:
					del self._msgcache[topic]

	def set(self, publisher: Any, message: Message) -> None:
		with self._lock:
			try:
				rtup = self._r[message.publisher_name]
			except KeyError:
				try:
					pub = _PublisherWeakRef(publisher, self._weak_cleanup, 
						publisher_name=message.publisher_name)
				except TypeError:  # if publisher object cannot be weakly referenced
					# just have to go with hard ref and accept it will live forever in the cache
					pub = publisher
					log.warning('The publisher cannot be weakly referenced. ' + \
						'A hard ref will be held by the cache indefinitely.',
						extra=dict(publisher_name=message.publisher_name))
				self._r[message.publisher_name] = rtup = (pub, set())
			rtup[1].add(message.topic)
			try:
				tdict = self._msgcache[message.topic]
			except KeyError:
				self._msgcache[message.topic] = tdict = {}
			tdict[message.publisher_name] = message

	def get(self, publisher_name: str, topic: str, default = None) -> Tuple[Any, Message]:
		try:
			message = self._msgcache[topic][publisher_name]
		except KeyError:
			return default, default
		else:
			pub = self._r[publisher_name][0]
			if isinstance(pub, weakref.ref):
				pub = pub()
			return pub, message

	def publishers(self, topic: str) -> KeysView[str]:
		"""Get all publisher names on the topic, for use with late joiners."""
		return self._msgcache[topic].keys()


class SignalExchange:
	"""Central thread-safe signal exchange for pub/sub pattern."""

	def __init__(self) -> None:
		self._lock = threading.RLock()
		self._cache = _Cache(lock=self._lock)
		self._subscriptions = _Subscriptions(cache=self._cache, lock=self._lock)
		self._topics = _Topics(lock=self._lock)
		self._topic_patterns = _TopicPatterns()
		self._topic_patterns._topics = weakref.ref(self._topics)
		self._subscriptions._topic_patterns = weakref.ref(self._topic_patterns)
		self._subscriptions._topics = weakref.ref(self._topics)
		self._topics._topic_patterns = weakref.ref(self._topic_patterns)
		self._topics._subscriptions = weakref.ref(self._subscriptions)

	def subscribe(self, *args, **kwargs) -> str:
		"""Create a subscription and return the subscription id.
		
		Args:
			topic_pattern: A regular expression string or enum to match topics against.
			handler: Async/sync callable to call with new messages for the subscription, which
				takes subscription id, publisher, and the message as arguments.
			publisher_name: Optional. The name of the publisher to receive messages
				from. Defaults to "any" which listens for messages from any publisher.
			filter: Async/sync callable to call with new messages for the subscription, which
				takes subscription id, publisher, and the message as arguments.
				If it returns True, the message will be given to the handler callable;
				otherwise, the message will be dropped.
			subscription_id: Optional. UUID for the subscription. Defaults to uuid1 hex.
			weak_handler: Optional. True if a weakref to the handler callable should
				be used and False otherwise. Defaults to True.
			weak_filter: Optional. True if a weakref to the filter callable should
				be used and False otherwise. Defaults to True.
			initialize: Optional. True if the initial latest values for all matching
				topics and publishers should be sent to the subscription when it is created,
				handling the late-joiner problem. Defaults to False.
		
		Returns:
			subscription_id: uuid hex of the subscription.
		"""
		return self._subscriptions._add(*args, **kwargs)

	def unsubscribe(self, *args, **kwargs) -> None:
		"""Remove a subscription.
		
		Args:
			subscription_id: The uuid of the subscription to remove.
		"""
		return self._subscriptions._remove(*args, **kwargs)

	def publish(self, publisher: Any, message: Message,
		skip_no_change: bool = True, cache: bool = True) -> None:
		"""Publish a message to all subscriptions for the given topic and publisher.
		
		Subscription handler uniqueness will be checked before sending any messages
		to avoid sending the same handler function the same message more than once.

		Args:
			publisher: The publisher object publishing the message.
			message: The SignalModel object being published. Note that the 
				previous_value will be filled in automatically.
			skip_no_change: Optional. True if you want to skip publishing
				when the previous value and current value are the same.
				Defaults to True.
			cache: Optional. True if you want the latest value to be cached
				for this topic and False otherwise. Defaults to True.
				Note that the cache holds hard refs, which will prevent gc.
		"""
		
		if not cache and skip_no_change:
			raise ValueError('skip_no_change cannot be True when cache is False.') 

		if cache:
			_, previous_message = self._cache.get(message.publisher_name, message.topic)
			if previous_message:
				message.previous_value = previous_message.value
			self._cache.set(publisher, message)
		
		if skip_no_change and message.value == message.previous_value:
			return

		topic = self._topics._add(message.topic)
		if not topic.publishers:
			return

		# each subscription can only have one publisher mapped to it,
		# so a set is not needed
		subscription_ids = []
		try:
			subscription_ids += topic.publishers[message.publisher_name].subscription_ids
		except KeyError:  # no subscriptions for this topic/publisher
			pass
		try:
			subscription_ids += topic.publishers['any'].subscription_ids
		except KeyError:
			pass
		if not subscription_ids:
			return

		# collect all the handlers across all subscriptions into a unique
		# set to avoid sending the same message more than once to a handler
		handlers = {}
		for subscription_id in subscription_ids:
			subscription = self._subscriptions[subscription_id]
			filter = subscription.filter
			if filter:
				if isinstance(subscription.filter, weakref.ref):
					filter = subscription.filter()
				try:
					if not _run_sync(filter, subscription_id, publisher, message):
						continue
				except:
					log.exception('Exception raised by signal filter.',
						extra=dict(subscription_id=subscription_id))
					continue
			if isinstance(subscription.handler, weakref.ref):
				handler = subscription.handler()
			else:
				handler = subscription.handler
			handlers[handler] = subscription_id
		
		for handler, subscription_id in handlers.items():
			try:
				_run_sync(handler, subscription_id, publisher, message)
			except:
				log.exception('Exception raised by signal handler.',
					extra=dict(subscription_id=subscription_id))
