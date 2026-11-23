"""Record parsing must stay inside each record's and each field's declared range."""

import struct
from io import BytesIO

import pytest

from mcap.data_stream import ReadDataStream
from mcap.exceptions import EndOfFile, McapFormatError
from mcap.opcode import Opcode
from mcap.records import (
    Channel,
    Chunk,
    Header,
    Message,
    Metadata,
    Schema,
    Statistics,
)
from mcap.stream_reader import StreamReader

from ._io_helpers import pstr, record, u16, u32, u64


def _bounded(body: bytes):
    """A bounded stream that hands the parser exactly one record body."""
    return ReadDataStream(BytesIO(body)).bounded(len(body))


def test_header_zero_length_string_does_not_borrow_neighbour_bytes():
    # Declared Header body length 4: one zero-length string (4 bytes), then four bytes that belong
    # to a hypothetical following record. The old reader used those bytes to invent a second empty
    # string; the bounded parser must reject the under-length Header instead.
    neighbour = b"\x00\x00\x00\x00"
    data = record(Opcode.HEADER, u32(0)) + neighbour
    with pytest.raises(McapFormatError):
        list(StreamReader(BytesIO(data), skip_magic=True).records)


def test_header_legal_pair_of_empty_strings():
    body = u32(0) + u32(0)
    header = Header.read(_bounded(body))
    assert header == Header("", "")


def test_header_unknown_extension_tail_is_skipped_within_boundary():
    body = u32(0) + u32(0) + b"extension-bytes"
    header = Header.read(_bounded(body))
    assert header == Header("", "")


def test_message_shorter_than_fixed_header_is_format_error():
    body = b"\x01\x02\x03"
    with pytest.raises(McapFormatError):
        Message.read(_bounded(body), len(body))


def test_message_zero_length_payload_is_legal():
    body = u16(1) + u32(0) + u64(10) + u64(10)
    message = Message.read(_bounded(body), len(body))
    assert message.data == b""


def test_channel_map_tail_truncated_is_format_error():
    # Map declares 8 bytes but only contains a complete 4-byte empty key and nothing else.
    map_body = u32(0)  # one zero-length key, missing the value
    body = (
        u16(1) + u16(0) + pstr("t") + pstr("e") + u32(len(map_body) + 4) + map_body
    )
    with pytest.raises(McapFormatError):
        Channel.read(_bounded(body))


def test_channel_map_declared_longer_than_record_is_format_error():
    # The map range claims bytes the record does not have.
    body = u16(1) + u16(0) + pstr("t") + pstr("e") + u32(100)
    with pytest.raises(McapFormatError):
        Channel.read(_bounded(body))


def test_metadata_map_entry_cannot_borrow_next_record():
    map_body = u32(0)  # a key with no value, truncated inside the map
    body = pstr("name") + u32(len(map_body) + 4) + map_body
    with pytest.raises(McapFormatError):
        Metadata.read(_bounded(body))


def test_statistics_truncated_count_entry_is_format_error():
    fixed = (
        u64(0) + u16(0) + u32(0) + u32(0) + u32(0) + u32(0) + u64(0) + u64(0)
    )
    # Map declares 10 bytes (one entry) but only provides 3.
    body = fixed + u32(10) + b"\x01\x02\x03"
    with pytest.raises(McapFormatError):
        Statistics.read(_bounded(body))


def test_chunk_data_cannot_run_past_record_boundary():
    # compression "" (4 length bytes), data declared 8 bytes, only 2 are present in the body.
    body = (
        u64(0) + u64(0) + u64(10) + u32(0) + u32(0) + u64(8) + b"\x00\x01"
    )
    with pytest.raises(McapFormatError):
        Chunk.read(_bounded(body))


def test_chunk_legal_empty_data_is_readable():
    body = (
        u64(0)
        + u64(0)
        + u64(0)
        + u32(0)
        + pstr("")
        + u64(0)
    )
    chunk = Chunk.read(_bounded(body))
    assert chunk.data == b""
    assert chunk.compression == ""


def test_schema_zero_length_data_is_legal():
    body = u16(1) + pstr("n") + pstr("") + u32(0)
    schema = Schema.read(_bounded(body))
    assert schema.data == b""


def test_physical_truncation_inside_record_is_end_of_file():
    # The Header envelope declares a 10-byte body, but the source ends after only 6 of them. The
    # bounded range is not yet exhausted when the physical input ends, so this is EndOfFile rather
    # than a structural error.
    data = (
        bytes([Opcode.HEADER]) + struct.pack("<Q", 10) + u32(0) + b"\x01\x02"
    )
    with pytest.raises(EndOfFile):
        list(StreamReader(BytesIO(data), skip_magic=True).records)


def test_struct_errors_never_leak():
    # One byte where a u8 value is expected is fine; two needed where one byte remains must surface
    # as an McapError-family error rather than struct.error.
    stream = ReadDataStream(BytesIO(b"\x00")).bounded(1)
    stream.read1()
    with pytest.raises(McapFormatError):
        stream.read2()
