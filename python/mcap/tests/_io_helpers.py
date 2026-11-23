"""Shared test helpers for short-read and record-boundary tests.

These helpers deliberately avoid the production :class:`mcap.writer.Writer` where noted: the
``hand_*`` builders emit individual MCAP records from explicit byte operations, with independently
stated expectations, so round-trip tests are not the only source of truth.
"""

import struct
from io import BytesIO
from typing import Callable, List, Optional, Tuple, Union

MAGIC = bytes([137, 77, 67, 65, 80, 48, 13, 10])


def record(opcode: int, body: bytes) -> bytes:
    """Pack an MCAP record envelope (opcode + u64 body length + body) by hand."""
    return bytes([opcode]) + struct.pack("<Q", len(body)) + body


def pstr(value: str) -> bytes:
    """Pack a length-prefixed UTF-8 string by hand."""
    raw = value.encode("utf-8")
    return struct.pack("<I", len(raw)) + raw


def u8(value: int) -> bytes:
    return struct.pack("<B", value)


def u16(value: int) -> bytes:
    return struct.pack("<H", value)


def u32(value: int) -> bytes:
    return struct.pack("<I", value)


def u64(value: int) -> bytes:
    return struct.pack("<Q", value)


class RequestProbe:
    """Records every ``read`` request and the byte interval actually consumed.

    A plain (non-:class:`io.RawIOBase`) object is used on purpose, so :class:`ReadDataStream`
    talks to it directly and the logged request sizes are exactly those the library made.
    """

    def __init__(self, data: bytes, cap: Union[int, Callable[[], int]] = 1024):
        self._data = data
        self._pos = 0
        self._cap = cap
        self.requests: List[Tuple[int, int, int]] = []
        self.closed = False

    def _next_cap(self) -> int:
        cap = self._cap() if callable(self._cap) else self._cap
        if cap <= 0:
            raise AssertionError("test cap must be a positive integer")
        return cap

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def read(self, size: Optional[int] = -1) -> bytes:
        if size is None or size < 0:
            raise AssertionError("the library must always issue explicit positive reads")
        start = self._pos
        n = min(size, self._next_cap(), len(self._data) - self._pos)
        out = self._data[self._pos : self._pos + n]
        self._pos += n
        self.requests.append((size, start, self._pos))
        return out

    def close(self) -> None:  # the reader must never call this
        self.closed = True

    @property
    def position(self) -> int:
        return self._pos

    @property
    def max_request(self) -> int:
        return max((req[0] for req in self.requests), default=0)


class SegmentedSeekable(BytesIO):
    """A seekable buffer whose ``read`` never returns more than ``cap`` bytes per call."""

    def __init__(self, data: bytes, cap: Union[int, Callable[[], int]] = 1024):
        super().__init__(data)
        self._cap = cap
        self.requests: List[Tuple[int, int]] = []

    def _next_cap(self) -> int:
        cap = self._cap() if callable(self._cap) else self._cap
        if cap <= 0:
            raise AssertionError("test cap must be a positive integer")
        return cap

    def read(self, size: Optional[int] = -1) -> bytes:
        if size is None or size < 0:
            raise AssertionError("the library must always issue explicit positive reads")
        start = self.tell()
        out = super().read(min(size, self._next_cap()))
        self.requests.append((size, start))
        return out


def alternating_caps(values: Tuple[int, ...] = (1, 3, 7, 2, 5)) -> Callable[[], int]:
    state = {"i": 0}

    def cap() -> int:
        value = values[state["i"] % len(values)]
        state["i"] += 1
        return value

    return cap


class RaisingStream:
    """A source whose reads fail with an OSError, which the library must propagate unchanged."""

    def __init__(self, error: OSError):
        self._error = error
        self.closed = False

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def read(self, size: Optional[int] = -1) -> bytes:
        raise self._error

    def close(self) -> None:
        self.closed = True


class LazyLargeBodyStream:
    """A non-seekable source that materializes a huge unknown-record body lazily.

    The body is generated one fixed block at a time; the full ``body_length`` bytes are never
    allocated. This lets skip tests prove that working memory does not grow with the skipped
    record. The stream ends with a recognizable legal record so the test can confirm parsing
    continues correctly afterwards.
    """

    BLOCK = 64 * 1024

    def __init__(self, opcode: int, body_length: int, tail: bytes):
        self._header = bytes([opcode]) + struct.pack("<Q", body_length)
        self._body_length = body_length
        self._tail = tail
        self._block = bytes((i % 251 for i in range(self.BLOCK)))
        # Logical layout: header | body | tail, addressed with one absolute cursor.
        self._body_start = len(self._header)
        self._tail_start = self._body_start + body_length
        self._end = self._tail_start + len(tail)
        self._pos = 0
        self.requests: List[int] = []

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def read(self, size: Optional[int] = -1) -> bytes:
        if size is None or size <= 0:
            raise AssertionError("the library must always issue explicit positive reads")
        self.requests.append(size)
        if self._pos >= self._end:
            return b""
        out = b""
        while size > 0 and self._pos < self._end:
            if self._pos < self._body_start:
                end = min(self._body_start, self._pos + size)
                out += self._header[self._pos : end]
                size -= end - self._pos
                self._pos = end
            elif self._pos < self._tail_start:
                body_off = self._pos - self._body_start
                n = min(size, self.BLOCK, self._body_length - body_off)
                # Reuse the same block object; never build a body-sized buffer.
                out += self._block[:n]
                size -= n
                self._pos += n
            else:
                tail_off = self._pos - self._tail_start
                n = min(size, len(self._tail) - tail_off)
                out += self._tail[tail_off : tail_off + n]
                size -= n
                self._pos += n
        return out


def walk_records(data: bytes, start: int = 8) -> List[Tuple[int, int, int]]:
    """Return ``(opcode, record_start_offset, body_length)`` tuples from a hand-built MCAP."""
    out: List[Tuple[int, int, int]] = []
    pos = start
    while pos < len(data) - len(MAGIC):
        opcode = data[pos]
        (length,) = struct.unpack_from("<Q", data, pos + 1)
        out.append((opcode, pos, length))
        pos += 9 + length
    return out
