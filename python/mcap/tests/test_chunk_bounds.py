"""Corruption must stay contained inside chunk inner-record boundaries."""

import zlib
from io import BytesIO

import pytest

from mcap.exceptions import McapFormatError
from mcap.records import Chunk
from mcap.stream_reader import breakup_chunk

from ._io_helpers import pstr, record, u16, u32, u64


def _chunk_with_inner(inner: bytes) -> Chunk:
    return Chunk(
        compression="",
        data=inner,
        message_end_time=0,
        message_start_time=0,
        uncompressed_crc=0,  # disable CRC so the boundary logic is what is under test
        uncompressed_size=len(inner),
    )


def test_inner_record_short_declared_length_does_not_borrow_next_inner_record():
    # A Channel whose declared body ends after its id, leaving the following inner record's bytes
    # to be misread as the channel's strings if bounds were not enforced.
    short_channel = record(0x04, u16(1))
    follower = record(0x04, u16(2) + u16(0) + pstr("t") + pstr("e") + u32(0))
    chunk = _chunk_with_inner(short_channel + follower)
    with pytest.raises(McapFormatError):
        breakup_chunk(chunk, record_size_limit=None)


def test_inner_message_with_under_length_header_is_format_error():
    bad_message = record(0x05, b"\x01\x02")
    follower = record(0x04, u16(1) + u16(0) + pstr("t") + pstr("e") + u32(0))
    chunk = _chunk_with_inner(bad_message + follower)
    with pytest.raises(McapFormatError):
        breakup_chunk(chunk, record_size_limit=None)


def test_inner_map_truncated_inside_chunk_is_format_error():
    # A Channel whose metadata map declares entries that do not fit in the remaining inner bytes.
    body = u16(1) + u16(0) + pstr("t") + pstr("e") + u32(12) + u32(0)
    chunk = _chunk_with_inner(record(0x04, body))
    with pytest.raises(McapFormatError):
        breakup_chunk(chunk, record_size_limit=None)


def test_inner_records_parse_when_all_bounds_are_legal():
    channel = record(0x04, u16(1) + u16(0) + pstr("t") + pstr("e") + u32(0))
    message_body = u16(1) + u32(0) + u64(5) + u64(5) + b"abc"
    message = record(0x05, message_body)
    chunk = _chunk_with_inner(channel + message)
    records = breakup_chunk(chunk, record_size_limit=None)
    assert len(records) == 2
    assert records[1].data == b"abc"


def test_chunk_crc_is_still_computed_over_full_decompressed_payload():
    inner = record(
        0x05, u16(1) + u32(0) + u64(5) + u64(5) + b"abc"
    )
    chunk = Chunk(
        compression="",
        data=inner,
        message_end_time=0,
        message_start_time=0,
        uncompressed_crc=zlib.crc32(inner),
        uncompressed_size=len(inner),
    )
    records = breakup_chunk(chunk, validate_crc=True, record_size_limit=None)
    assert records[0].data == b"abc"


def test_unknown_inner_record_skipped_inside_its_boundary():
    unknown = record(0x77, b"opaque-inner-bytes")
    message = record(0x05, u16(1) + u32(0) + u64(5) + u64(5) + b"z")
    chunk = _chunk_with_inner(unknown + message)
    records = breakup_chunk(chunk, record_size_limit=None)
    assert len(records) == 1
    assert records[0].data == b"z"
