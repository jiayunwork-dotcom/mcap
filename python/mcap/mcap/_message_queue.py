import heapq
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple, Union

from .records import Channel, ChunkIndex, Message, Schema

MessageTuple = Tuple[Tuple[Optional[Schema], Channel, Message], int, int]


@dataclass
class ChunkCandidate:
    """A chunk that may contain messages matching a query.

    Unlike a bare :py:class:`~mcap.records.ChunkIndex`, this carries the time envelope that
    should be used to position the chunk in a message priority queue.  For publish-time queries
    backed by the publish-time index, ``min_time`` and ``max_time`` are the minimum and maximum
    publish times of the *selected* channels in this chunk, so an unrelated channel with an
    earlier or later publish time cannot make the queue hand out a winner too early (or hold it
    back).
    """

    chunk_index: ChunkIndex
    min_time: int
    max_time: int


QueueItem = Union[ChunkIndex, ChunkCandidate, MessageTuple]


class _Orderable:
    def __init__(self, item: QueueItem, reverse: bool, time_basis: str = "log"):
        self.item: QueueItem = item
        self.reverse = reverse
        self.time_basis = time_basis

    def __lt__(self, other: "_Orderable") -> bool:
        if self.log_time() == other.log_time():
            return self._position_less_than(other)
        return self._compare(self.log_time(), other.log_time())

    def _compare(self, a: int, b: int) -> bool:
        if self.reverse:
            return a > b
        return a < b

    def _position_less_than(self, other: "_Orderable") -> bool:
        this_chunk_offset, this_message_offset = self.position()
        other_chunk_offset, other_message_offset = other.position()
        if this_message_offset is None or other_message_offset is None:
            return self._compare(this_chunk_offset, other_chunk_offset)
        if this_chunk_offset == other_chunk_offset:
            return self._compare(this_message_offset, other_message_offset)
        return self._compare(this_chunk_offset, other_chunk_offset)

    def log_time(self) -> int:
        raise NotImplementedError(
            "do not instantiate _Orderable directly, use a subclass"
        )

    def position(self) -> Tuple[int, Optional[int]]:
        raise NotImplementedError(
            "do not instantiate _Orderable directly, use a subclass"
        )


def _chunk_index_of(item: Union[ChunkIndex, ChunkCandidate]) -> ChunkIndex:
    if isinstance(item, ChunkCandidate):
        return item.chunk_index
    return item


class _ChunkIndexWrapper(_Orderable):
    def _chunk_item(self) -> Union[ChunkIndex, ChunkCandidate]:
        item = self.item
        assert isinstance(item, (ChunkIndex, ChunkCandidate))
        return item

    def _envelope(self) -> Tuple[int, int]:
        item = self._chunk_item()
        if isinstance(item, ChunkCandidate):
            return (item.min_time, item.max_time)
        return (item.message_start_time, item.message_end_time)

    def log_time(self) -> int:
        min_time, max_time = self._envelope()
        if self.reverse:
            return max_time
        return min_time

    def position(self) -> Tuple[int, Optional[int]]:
        # The chunk's start offset is used in both directions. In reverse mode the descending
        # comparison then puts later chunks ahead of earlier ones for equal timestamps, which is
        # exactly the reverse of the ascending (physical) tie order.
        chunk_index = _chunk_index_of(self._chunk_item())
        return (chunk_index.chunk_start_offset, None)


class _MessageTupleWrapper(_Orderable):
    def _message_item(self) -> MessageTuple:
        item = self.item
        assert not isinstance(item, (ChunkIndex, ChunkCandidate))
        return item

    def log_time(self) -> int:
        message = self._message_item()[0][2]
        if self.time_basis == "publish":
            return message.publish_time
        return message.log_time

    def position(self) -> Tuple[int, Optional[int]]:
        item = self._message_item()
        return (item[1], item[2])


def _make_orderable(
    item: QueueItem, reverse: bool, time_basis: str = "log"
) -> _Orderable:
    if isinstance(item, (ChunkIndex, ChunkCandidate)):
        return _ChunkIndexWrapper(item, reverse, time_basis)
    return _MessageTupleWrapper(item, reverse, time_basis)


class _MessageQueue(ABC):
    @abstractmethod
    def push(self, item: QueueItem):
        raise NotImplementedError()

    @abstractmethod
    def pop(self) -> QueueItem:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()


class LogTimeOrderQueue(_MessageQueue):
    def __init__(self, reverse: bool = False, time_basis: str = "log"):
        self._q: List[_Orderable] = []
        self._reverse = reverse
        self._time_basis = time_basis

    def push(self, item: QueueItem):
        orderable = _make_orderable(item, self._reverse, self._time_basis)
        heapq.heappush(self._q, orderable)

    def pop(self) -> QueueItem:
        return heapq.heappop(self._q).item

    def __len__(self) -> int:
        return len(self._q)


class InsertOrderQueue(_MessageQueue):
    def __init__(self):
        self._q: Deque[QueueItem] = deque()

    def push(self, item: QueueItem):
        self._q.append(item)

    def pop(self) -> QueueItem:
        return self._q.popleft()  # cspell:disable-line

    def __len__(self) -> int:
        return len(self._q)


def make_message_queue(
    log_time_order: bool = True, reverse: bool = False, time_basis: str = "log"
) -> _MessageQueue:
    """Create a queue of MCAP messages and chunk indices.

    :param log_time_order: if True, this queue acts as a priority queue, ordered by log time.
        if False, ``pop()`` returns elements in insert order.
    :param reverse: if True, order elements in descending log time order rather than ascending.
        only valid if ``log_time_order`` is True, otherwise throws a ValueError.
    :param time_basis: the message clock used to order messages: ``"log"`` (the message
        ``log_time``) or ``"publish"`` (the message ``publish_time``).  Chunks may be pushed as
        :py:class:`ChunkCandidate` instances to supply an explicit time envelope.
    """
    if time_basis not in ("log", "publish"):
        raise ValueError(
            f"invalid time_basis {time_basis!r}, expected 'log' or 'publish'"
        )
    if log_time_order:
        return LogTimeOrderQueue(reverse, time_basis)
    if reverse:
        raise ValueError("reverse is only valid with log_time_order=True")
    return InsertOrderQueue()
