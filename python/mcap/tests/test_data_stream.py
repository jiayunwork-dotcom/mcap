"""Tests for the low-level read contract: short reads, bounded ranges and error mapping."""

import struct
from io import BytesIO

import pytest

from mcap.data_stream import MAX_SKIP_READ, ReadDataStream
from mcap.exceptions import EndOfFile, McapFormatError

from ._io_helpers import RaisingStream, RequestProbe


class _Short:
    """A blocking binary source that hands out at most one byte per read."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def read(self, size=-1):
        if self._pos >= len(self._data):
            return b""
        out = self._data[self._pos : self._pos + 1]
        self._pos += 1
        return out


def test_read_loops_until_full_amount():
    stream = ReadDataStream(_Short(b"abcdefgh"))
    assert stream.read(8) == b"abcdefgh"
    assert stream.count == 8


def test_read_zero_returns_empty_without_touching_source():
    probe = RequestProbe(b"abc")
    stream = ReadDataStream(probe)
    assert stream.read(0) == b""
    assert probe.requests == []


def test_empty_read_is_end_of_file():
    stream = ReadDataStream(BytesIO(b"abc"))
    with pytest.raises(EndOfFile):
        stream.read(6)
    # The three physical bytes were still counted before the EOF.
    assert stream.count == 3


def test_eof_mid_multibyte_value_stays_end_of_file():
    stream = ReadDataStream(_Short(b"ab"))
    with pytest.raises(EndOfFile):
        stream.read8()


def test_source_oserror_propagates_unchanged():
    error = OSError("device reset")
    stream = ReadDataStream(RaisingStream(error))
    with pytest.raises(OSError) as exc:
        stream.read(4)
    assert exc.value is error
    assert not isinstance(exc.value, EndOfFile)


def test_bounded_range_blocks_cross_boundary_read():
    stream = ReadDataStream(BytesIO(b"abcdefgh"))
    bounded = stream.bounded(4)
    assert bounded.read(4) == b"abcd"
    with pytest.raises(McapFormatError):
        bounded.read(1)
    # The parent stream is positioned exactly at the boundary; the next bytes still belong to the
    # following field/record and have not been borrowed.
    assert stream.read(4) == b"efgh"


def test_bounded_range_rejects_oversized_request_upfront():
    stream = ReadDataStream(BytesIO(b"abcdefgh"))
    bounded = stream.bounded(2)
    with pytest.raises(McapFormatError):
        bounded.read(3)


def test_nested_bounded_range_cannot_exceed_parent():
    stream = ReadDataStream(BytesIO(b"abcdefgh"))
    bounded = stream.bounded(4)
    with pytest.raises(McapFormatError):
        bounded.bounded(5)


def test_bounded_short_reads_are_fully_satisfied():
    stream = ReadDataStream(_Short(b"abcdefgh"))
    bounded = stream.bounded(6)
    assert bounded.read(6) == b"abcdef"
    with pytest.raises(McapFormatError):
        bounded.read(1)


def test_discard_caps_each_request_at_64kib():
    size = 200_000
    probe = RequestProbe(b"\x00" * size)
    stream = ReadDataStream(probe)
    stream.discard(size)
    assert stream.count == size
    assert probe.max_request <= MAX_SKIP_READ
    # ceil(size / 64KiB) chunked requests rather than one giant request.
    assert max((req[0] for req in probe.requests)) == MAX_SKIP_READ


def test_bounded_discard_cannot_cross_boundary():
    stream = ReadDataStream(BytesIO(b"abcdefgh"))
    bounded = stream.bounded(3)
    with pytest.raises(McapFormatError):
        bounded.discard(4)
    assert bounded.remaining == 3
    bounded.discard(3)
    assert bounded.remaining == 0
    assert stream.read(1) == b"d"


def test_negative_read_is_format_error():
    stream = ReadDataStream(BytesIO(b"ab"))
    with pytest.raises(McapFormatError):
        stream.read(-1)


def test_invalid_utf8_is_format_error():
    # length prefix 2 followed by bytes that are not valid UTF-8.
    stream = ReadDataStream(BytesIO(struct.pack("<I", 2) + b"\xff\xfe"))
    with pytest.raises(McapFormatError):
        stream.read_prefixed_string()


def test_zero_length_prefixed_string_is_legal():
    stream = ReadDataStream(BytesIO(struct.pack("<I", 0) + b"tail"))
    assert stream.read_prefixed_string() == ""
    assert stream.read(4) == b"tail"


def test_count_and_crc_are_owned_by_parent_through_bounds():
    probe = RequestProbe(b"abcdefgh")
    stream = ReadDataStream(probe, calculate_crc=True)
    bounded = stream.bounded(4)
    bounded.read(2)
    nested = bounded.bounded(2)
    nested.read(2)
    assert stream.count == 4
    # CRC over the same four bytes is computed exactly once, despite the nested views.
    assert stream.checksum() == __import__("zlib").crc32(b"abcd")
    assert stream.read(4) == b"efgh"


def test_multishot_short_read_request_intervals_are_contiguous():
    probe = RequestProbe(b"abcdefgh", cap=3)
    stream = ReadDataStream(probe)
    assert stream.read(8) == b"abcdefgh"
    # Consumed intervals must tile [0, 8) with no overlap and no gap.
    intervals = [(start, end) for _, start, end in probe.requests]
    flattened = [pos for interval in intervals for pos in interval]
    assert flattened[0] == 0
    assert flattened[-1] == 8
    for (_, end), (start, _) in zip(intervals, intervals[1:]):
        assert end == start
