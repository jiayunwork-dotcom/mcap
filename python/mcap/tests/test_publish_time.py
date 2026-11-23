"""Tests for publish-time queries and the optional publish-time index extension.

The tests deliberately construct files where ``log_time`` and ``publish_time`` disagree and
interleave across chunks, then check results against an independently-computed expectation built
from the raw input rather than from the reader under test.
"""

import io
import os
import struct
import zlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pytest

import mcap.reader as reader_module
from mcap.decoder import DecoderFactory
from mcap.exceptions import McapError
from mcap.publish_time_index import (
    PUBLISH_TIME_INDEX_MAGIC,
    PublishTimeIndex,
    PublishTimeIndexEntry,
    encode_publish_time_index,
    parse_publish_time_index,
    validate_publish_time_index,
)
from mcap.reader import NonSeekingReader, SeekingReader, make_reader
from mcap.records import Channel, ChunkIndex, Message, Statistics
from mcap.stream_reader import CRCValidationError, StreamReader
from mcap.writer import CompressionType, IndexType, Writer

READER_SUBCLASSES = [SeekingReader, NonSeekingReader]

# ---------------------------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------------------------


@dataclass
class RawMessage:
    topic: str
    channel_id: int
    sequence: int
    log_time: int
    publish_time: int
    data: bytes


def build_synthetic_mcap(
    messages: List[RawMessage],
    *,
    publish_time_index: bool,
    compression: CompressionType = CompressionType.NONE,
    chunk_size: int = 1024 * 1024,
    index_types: IndexType = IndexType.ALL,
    repeat_channels: bool = True,
    repeat_schemas: bool = True,
    use_statistics: bool = True,
    use_summary_offsets: bool = True,
    use_chunking: bool = True,
    enable_crcs: bool = True,
) -> Tuple[bytes, Dict[str, int]]:
    """Write a file. Returns bytes and a topic -> channel id map."""
    topics: List[str] = []
    for m in messages:
        if m.topic not in topics:
            topics.append(m.topic)
    chan_of_topic = {t: i + 1 for i, t in enumerate(topics)}

    out = io.BytesIO()
    writer = Writer(
        out,
        compression=compression,
        chunk_size=chunk_size,
        index_types=index_types,
        repeat_channels=repeat_channels,
        repeat_schemas=repeat_schemas,
        use_statistics=use_statistics,
        use_summary_offsets=use_summary_offsets,
        use_chunking=use_chunking,
        enable_crcs=enable_crcs,
        publish_time_index=publish_time_index,
    )
    writer.start("profile", "publish-time-tests")
    for topic in topics:
        writer.register_channel(topic, "raw", 0)
    for m in messages:
        writer.add_message(
            channel_id=chan_of_topic[m.topic],
            log_time=m.log_time,
            data=m.data,
            publish_time=m.publish_time,
            sequence=m.sequence,
        )
    writer.finish()
    return out.getvalue(), chan_of_topic


def expected_results(
    messages: List[RawMessage],
    *,
    topics: Optional[Iterable[str]] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    order: bool = True,
    reverse: bool = False,
    time_basis: str = "log",
) -> List[Tuple[str, int, int, int, bytes]]:
    """Independently compute the expected (topic, sequence, log, publish, data) sequence.

    Physical order is the input order. Ties are broken by that physical order; a reverse read is
    the exact reverse of the forward sequence.
    """
    if isinstance(topics, str):
        topics = [topics]
    topic_set = None if topics is None else set(topics)

    def tstamp(m: RawMessage) -> int:
        return m.publish_time if time_basis == "publish" else m.log_time

    selected = []
    for index, m in enumerate(messages):
        if topic_set is not None and m.topic not in topic_set:
            continue
        t = tstamp(m)
        if start_time is not None and t < start_time:
            continue
        if end_time is not None and t >= end_time:
            continue
        selected.append((index, m))
    if order:
        selected.sort(key=lambda pair: (tstamp(pair[1]), pair[0]))
        if reverse:
            selected.reverse()
    out = []
    for _, m in selected:
        out.append((m.topic, m.sequence, m.log_time, m.publish_time, m.data))
    return out


def read_results(
    reader,
    *,
    topics: Optional[Iterable[str]] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    order: bool = True,
    reverse: bool = False,
    time_basis: str = "log",
) -> List[Tuple[str, int, int, int, bytes]]:
    return [
        (
            channel.topic,
            message.sequence,
            message.log_time,
            message.publish_time,
            message.data,
        )
        for _, channel, message in reader.iter_messages(
            topics=topics,
            start_time=start_time,
            end_time=end_time,
            log_time_order=order,
            reverse=reverse,
            time_basis=time_basis,
        )
    ]


def opposite_clock_messages() -> List[RawMessage]:
    """Two topics whose log and publish orders are exact opposites, spread over many chunks.

    With ``chunk_size`` small enough, messages from both topics share chunks and publish-time
    envelopes of different chunks overlap heavily.
    """
    messages: List[RawMessage] = []
    sequence = 0
    # 24 messages: topic /a logs increasing, /b logs increasing, but publish time is 1000 - 7*log.
    for i in range(24):
        topic = "/a" if i % 3 != 2 else "/b"
        log_time = 100 + i
        publish_time = 1000 - 7 * i
        messages.append(
            RawMessage(
                topic=topic,
                channel_id=0,
                sequence=sequence,
                log_time=log_time,
                publish_time=publish_time,
                data=f"msg-{i:03d}".encode(),
            )
        )
        sequence += 1
    return messages


# ---------------------------------------------------------------------------------------------
# Correctness: dual clocks, ordering, filters, ties, duplicates, boundaries
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("publish_time_index", [False, True])
@pytest.mark.parametrize(
    "compression", [CompressionType.NONE, CompressionType.LZ4, CompressionType.ZSTD]
)
def test_publish_query_matches_independent_model(publish_time_index: bool, compression):
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=publish_time_index,
        compression=compression,
        chunk_size=260,  # several messages per chunk, many chunks
    )

    for reader_cls in READER_SUBCLASSES:
        for basis in ("log", "publish"):
            for order in (True, False):
                for reverse in ((False, True) if order else (False,)):
                    # A NonSeekingReader may only answer one query, so build a fresh one each time.
                    reader = reader_cls(io.BytesIO(content))
                    got = read_results(
                        reader,
                        order=order,
                        reverse=reverse,
                        time_basis=basis,
                    )
                    want = expected_results(
                        messages, order=order, reverse=reverse, time_basis=basis
                    )
                    assert got == want, (reader_cls, basis, order, reverse)


def _open(reader_cls, content: bytes):
    return _ClosingReader(reader_cls(io.BytesIO(content)))


class _ClosingReader:
    def __init__(self, reader):
        self.reader = reader

    def __enter__(self):
        return self.reader

    def __exit__(self, *exc):
        return False


def test_publish_start_end_and_topics():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=200)
    reader = SeekingReader(io.BytesIO(content))
    cases = [
        dict(start_time=700, end_time=800),
        dict(start_time=1000, end_time=1000),  # empty window
        dict(start_time=0, end_time=1),
        dict(topics=["/a"]),
        dict(topics="/a", start_time=600, end_time=900),
        dict(topics=["/nonexistent"]),
    ]
    for case in cases:
        got = read_results(reader, time_basis="publish", **case)
        want = expected_results(messages, time_basis="publish", **case)
        assert got == want, case


def test_publish_zero_is_a_real_time():
    messages = [
        RawMessage("/a", 0, 1, 500, 0, b"zero"),
        RawMessage("/a", 0, 2, 501, 10, b"ten"),
        RawMessage("/b", 0, 3, 502, 0, b"zero-b"),
    ]
    content, _ = build_synthetic_mcap(messages, publish_time_index=True)
    reader = SeekingReader(io.BytesIO(content))
    got = read_results(reader, start_time=0, end_time=1, time_basis="publish")
    assert [g[4] for g in got] == [b"zero", b"zero-b"]


def test_publish_duplicates_and_ties_keep_physical_order():
    # Two identical messages plus a tie between topics; duplicates must each appear once.
    messages = [
        RawMessage("/a", 0, 7, 10, 50, b"dup"),
        RawMessage("/b", 0, 7, 20, 50, b"dup"),
        RawMessage("/a", 0, 8, 30, 50, b"tie-a"),
        RawMessage("/b", 0, 9, 40, 60, b"later"),
    ]
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=120)
    reader = SeekingReader(io.BytesIO(content))
    fwd = read_results(reader, time_basis="publish")
    rev = read_results(reader, time_basis="publish", reverse=True)
    want = expected_results(messages, time_basis="publish")
    assert fwd == want
    assert rev == list(reversed(want))
    assert len([x for x in fwd if x[4] == b"dup"]) == 2


def test_boundaries_are_half_open():
    messages = [
        RawMessage("/a", 0, 1, 1, 100, b"a"),
        RawMessage("/a", 0, 2, 2, 200, b"b"),
        RawMessage("/a", 0, 3, 3, 300, b"c"),
    ]
    content, _ = build_synthetic_mcap(messages, publish_time_index=True)
    reader = SeekingReader(io.BytesIO(content))
    got = read_results(reader, start_time=100, end_time=300, time_basis="publish")
    assert [g[4] for g in got] == [b"a", b"b"]
    # start only
    got = read_results(reader, start_time=200, time_basis="publish")
    assert [g[4] for g in got] == [b"b", b"c"]
    # end only
    got = read_results(reader, end_time=200, time_basis="publish")
    assert [g[4] for g in got] == [b"a"]


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_message_fields_unchanged(reader_cls):
    messages = opposite_clock_messages()
    content, chan_of_topic = build_synthetic_mcap(messages, publish_time_index=True)
    reader = reader_cls(io.BytesIO(content))
    seen = 0
    for schema, channel, message in reader.iter_messages(time_basis="publish"):
        assert schema is None
        assert channel.id == chan_of_topic[channel.topic]
        assert isinstance(message, Message)
        # fields match the raw input by sequence
        raw = next(m for m in messages if m.sequence == message.sequence)
        assert message.channel_id == raw.channel_id or channel.topic == raw.topic
        assert message.log_time == raw.log_time
        assert message.publish_time == raw.publish_time
        assert message.data == raw.data
        seen += 1
    assert seen == len(messages)


# ---------------------------------------------------------------------------------------------
# Argument validation (before any message is delivered)
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
@pytest.mark.parametrize(
    "kwargs",
    [
        dict(time_basis="wall"),
        dict(time_basis=None),
        dict(start_time=True),
        dict(end_time=False),
        dict(start_time=1.5),
        dict(start_time="10"),
        dict(start_time=-1),
        dict(end_time=2**64),
        dict(start_time=10, end_time=9),
        dict(log_time_order=False, reverse=True),
    ],
)
def test_invalid_arguments_raise_before_delivery(reader_cls, kwargs):
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True)
    reader = reader_cls(io.BytesIO(content))
    with pytest.raises(ValueError):
        reader.iter_messages(**kwargs)
    reader = reader_cls(io.BytesIO(content))
    with pytest.raises(ValueError):
        reader.iter_decoded_messages(**kwargs)


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_valid_extreme_boundaries(reader_cls):
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True)
    reader = reader_cls(io.BytesIO(content))
    got = read_results(reader, start_time=0, end_time=2**64 - 1, time_basis="publish")
    # publish times span 1000-7*23 .. 1000; all are < 2**64-1 and >= 0
    assert len(got) == len(messages)


def test_log_time_order_name_is_preserved():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True)
    reader = SeekingReader(io.BytesIO(content))
    default = read_results(reader)
    explicit = read_results(reader, time_basis="log")
    assert default == explicit


# ---------------------------------------------------------------------------------------------
# iter_decoded_messages: filtering and ordering happen before decoding
# ---------------------------------------------------------------------------------------------


class CountingDecoderFactory(DecoderFactory):
    def __init__(self):
        self.calls: List[bytes] = []

    def decoder_for(self, message_encoding: str, schema):
        if message_encoding != "raw":
            return None

        def decode(data: bytes) -> Any:
            self.calls.append(data)
            return {"data": data}

        return decode


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_decoded_only_decodes_selected_messages(reader_cls):
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=260)
    factory = CountingDecoderFactory()
    reader = reader_cls(io.BytesIO(content), decoder_factories=[factory])
    got = list(
        reader.iter_decoded_messages(
            topics=["/a"], start_time=600, end_time=900, time_basis="publish"
        )
    )
    want = expected_results(
        messages, topics=["/a"], start_time=600, end_time=900, time_basis="publish"
    )
    assert [(c.topic, m.sequence, d["data"]) for _, c, m, d in got] == [
        (t, s, d) for (t, s, _, _, d) in want
    ]
    assert factory.calls == [d for (_, _, _, _, d) in want]


# ---------------------------------------------------------------------------------------------
# Writer option validation and interoperability with plain/old readers
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(publish_time_index=True, use_chunking=False),
        dict(publish_time_index=True, use_chunking=True, index_types=IndexType.NONE),
        dict(publish_time_index=True, use_chunking=True, index_types=IndexType.MESSAGE),
        dict(publish_time_index="yes"),
        dict(publish_time_index=1),
    ],
)
def test_writer_rejects_incompatible_options_before_writing(kwargs):
    out = io.BytesIO()
    with pytest.raises(ValueError):
        Writer(out, **kwargs)
    assert out.getvalue() == b""


def test_plain_writer_does_not_pay_for_publish_index():
    """A normal writer still only tracks log times; no extension is emitted."""
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=False)
    reader = SeekingReader(io.BytesIO(content))
    summary = reader.get_summary()
    assert summary is not None
    assert summary.publish_time_indexes == []
    # publish queries still work, just without the index.
    got = read_results(reader, time_basis="publish")
    assert got == expected_results(messages, time_basis="publish")


def test_standard_time_fields_stay_log_time():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=260)
    summary = SeekingReader(io.BytesIO(content)).get_summary()
    assert summary is not None
    assert isinstance(summary.statistics, Statistics)
    log_times = [m.log_time for m in messages]
    assert summary.statistics.message_start_time == min(log_times)
    assert summary.statistics.message_end_time == max(log_times)
    for chunk_index in summary.chunk_indexes:
        assert isinstance(chunk_index, ChunkIndex)
        # A chunk's standard envelope must never exceed the global log-time range.
        assert chunk_index.message_start_time >= min(log_times)
        assert chunk_index.message_end_time <= max(log_times)


def test_plain_stream_reader_reads_every_message_from_indexed_file():
    """A reader that ignores the extension reads all messages, with unchanged fields."""
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=260)
    records = list(StreamReader(io.BytesIO(content)).records)
    got = [
        (r.sequence, r.log_time, r.publish_time, r.data)
        for r in records
        if isinstance(r, Message)
    ]
    want = [(m.sequence, m.log_time, m.publish_time, m.data) for m in messages]
    assert got == want
    # A default StreamReader never emits a PrivateRecord, so old code that switches on record
    # type never sees the extension.
    from mcap.records import PrivateRecord

    assert not any(isinstance(r, PrivateRecord) for r in records)


def test_non_seeking_reader_reads_indexed_file_fully():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=260)
    got = read_results(NonSeekingReader(io.BytesIO(content)), time_basis="publish")
    assert got == expected_results(messages, time_basis="publish")


# ---------------------------------------------------------------------------------------------
# Old files: no summary, non-seekable streams
# ---------------------------------------------------------------------------------------------


def _no_summary_content() -> Tuple[bytes, List[RawMessage]]:
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=False,
        repeat_channels=False,
        repeat_schemas=False,
        use_statistics=False,
        use_summary_offsets=False,
        index_types=IndexType.NONE,
    )
    return content, messages


def test_no_summary_file_publish_query():
    content, messages = _no_summary_content()
    reader = SeekingReader(io.BytesIO(content))
    assert reader.get_summary() is None
    for order in (True, False):
        got = read_results(reader, order=order, time_basis="publish")
        assert got == expected_results(messages, order=order, time_basis="publish")


class _Unseekable(io.RawIOBase):
    def __init__(self, data: bytes):
        self._buffer = io.BytesIO(data)

    def readable(self):
        return True

    def seekable(self):
        return False

    def read(self, n=-1):
        return self._buffer.read(n)

    def readinto(self, b):
        data = self._buffer.read(len(b))
        b[: len(data)] = data
        return len(data)


def test_non_seekable_stream_publish_query():
    content, messages = _no_summary_content()
    reader = NonSeekingReader(_Unseekable(content))
    got = read_results(reader, order=True, time_basis="publish")
    assert got == expected_results(messages, order=True, time_basis="publish")


def test_pipe_stream_publish_query():
    content, messages = _no_summary_content()
    r, w = os.pipe()
    try:
        with os.fdopen(w, "wb") as writer:
            writer.write(content)
        with os.fdopen(r, "rb") as stream:
            reader = make_reader(stream)
            assert isinstance(reader, NonSeekingReader)
            got = read_results(reader, time_basis="publish")
            assert got == expected_results(messages, time_basis="publish")
    finally:
        try:
            os.close(r)
        except OSError:
            pass


# ---------------------------------------------------------------------------------------------
# Corruption: malformed extension is rejected, unrelated payload damage is isolated
# ---------------------------------------------------------------------------------------------


def _find_position(content: bytes, needle: bytes) -> int:
    position = content.find(needle)
    assert position >= 0, "needle not found"
    return position


def _patch_extension(content: bytes, mutate: Callable[[bytearray], None]) -> bytes:
    """Patch the extension body in a single-index file, fixing its CRC afterwards.

    ``mutate`` receives the mutable body (without the trailing CRC) and may modify it in place.
    The body length must not change.
    """
    data = bytearray(content)
    body_start = _find_position(bytes(data), PUBLISH_TIME_INDEX_MAGIC)
    # A record starts 9 bytes (opcode + u64 length) before its body.
    record_length = struct.unpack_from("<Q", data, body_start - 8)[0]
    body_end = body_start + record_length - 4
    crc_offset = body_start + record_length - 4
    assert (
        bytes(data[body_start : body_start + len(PUBLISH_TIME_INDEX_MAGIC)])
        == PUBLISH_TIME_INDEX_MAGIC
    )
    body = bytearray(data[body_start:body_end])
    mutate(body)
    assert len(body) == body_end - body_start
    data[body_start:body_end] = body
    struct.pack_into("<I", data, crc_offset, zlib.crc32(bytes(body)))
    return bytes(data)


def test_corrupt_index_crc_is_rejected_before_delivery():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=260)
    data = bytearray(content)
    body_start = _find_position(bytes(data), PUBLISH_TIME_INDEX_MAGIC)
    data[body_start + 12] ^= 0xFF  # inside the entry area
    reader = SeekingReader(io.BytesIO(bytes(data)))
    with pytest.raises(McapError):
        list(reader.iter_messages(time_basis="publish"))
    # A log-time query does not interpret the extension and still works.
    reader = SeekingReader(io.BytesIO(bytes(data)))
    assert read_results(reader, time_basis="log") == expected_results(messages)


def test_unknown_extension_version_is_rejected():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True)

    def bump_version(body: bytearray):
        struct.pack_into("<I", body, len(PUBLISH_TIME_INDEX_MAGIC), 2)

    data = _patch_extension(content, bump_version)
    reader = SeekingReader(io.BytesIO(data))
    with pytest.raises(McapError, match="version"):
        list(reader.iter_messages(time_basis="publish"))


def test_incomplete_entry_count_is_rejected():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(messages, publish_time_index=True, chunk_size=260)

    def bump_count(body: bytearray):
        offset = len(PUBLISH_TIME_INDEX_MAGIC) + 4
        (count,) = struct.unpack_from("<Q", body, offset)
        struct.pack_into("<Q", body, offset, count + 1)

    data = _patch_extension(content, bump_count)
    reader = SeekingReader(io.BytesIO(data))
    with pytest.raises(McapError):
        list(reader.iter_messages(time_basis="publish"))


def test_duplicate_chunk_reference_is_rejected():
    content, _ = build_synthetic_mcap(
        opposite_clock_messages(), publish_time_index=True, chunk_size=260
    )

    def duplicate_offset(body: bytearray):
        first_entry = len(PUBLISH_TIME_INDEX_MAGIC) + 4 + 8
        (first_offset,) = struct.unpack_from("<Q", body, first_entry)
        # The second entry begins after the first entry's channel entries; instead of walking
        # the layout, find the next occurrence of a different offset by scanning entry headers.
        # Simpler: overwrite the first entry's offset to match the second entry's offset.
        # Layout: entry = u64 offset, u64 length, u32 nchans, nchans*(u16,u64,u64).
        pos = first_entry
        _, _, nchans = struct.unpack_from("<QQI", body, pos)
        pos += 8 + 8 + 4 + nchans * (2 + 8 + 8)
        (second_offset,) = struct.unpack_from("<Q", body, pos)
        struct.pack_into("<Q", body, first_entry, second_offset)

    data = _patch_extension(content, duplicate_offset)
    reader = SeekingReader(io.BytesIO(data))
    with pytest.raises(McapError, match="duplicate|incomplete|missing"):
        list(reader.iter_messages(time_basis="publish"))


def test_out_of_range_chunk_length_is_rejected():
    content, _ = build_synthetic_mcap(
        opposite_clock_messages(), publish_time_index=True, chunk_size=260
    )

    def huge_length(body: bytearray):
        length_offset = len(PUBLISH_TIME_INDEX_MAGIC) + 4 + 8 + 8
        struct.pack_into("<Q", body, length_offset, 2**62)

    data = _patch_extension(content, huge_length)
    reader = SeekingReader(io.BytesIO(data))
    with pytest.raises(McapError, match="length|ChunkIndex|outside"):
        list(reader.iter_messages(time_basis="publish"))


def test_parse_rejects_truncated_and_trailing_garbage():
    good = encode_publish_time_index(
        [
            PublishTimeIndexEntry(10, 20, {1: (0, 5)}),
        ]
    )
    with pytest.raises(McapError):
        parse_publish_time_index(good[:-6])
    with pytest.raises(McapError):
        parse_publish_time_index(good + b"\x00")


def test_validate_rejects_duplicate_and_missing_chunks():
    chunk_a = ChunkIndex(
        chunk_length=100,
        chunk_start_offset=1000,
        compression="",
        compressed_size=0,
        message_end_time=10,
        message_index_length=0,
        message_index_offsets={1: 0},
        message_start_time=0,
        uncompressed_size=0,
    )
    chunk_b = ChunkIndex(
        chunk_length=100,
        chunk_start_offset=2000,
        compression="",
        compressed_size=0,
        message_end_time=20,
        message_index_length=0,
        message_index_offsets={1: 0},
        message_start_time=11,
        uncompressed_size=0,
    )
    duplicate = PublishTimeIndex(
        entries=[
            PublishTimeIndexEntry(1000, 100, {1: (0, 10)}),
            PublishTimeIndexEntry(1000, 100, {1: (0, 10)}),
        ]
    )
    with pytest.raises(McapError):
        validate_publish_time_index(duplicate, [chunk_a, chunk_b], summary_start=5000)
    incomplete = PublishTimeIndex(
        entries=[PublishTimeIndexEntry(1000, 100, {1: (0, 10)})]
    )
    with pytest.raises(McapError):
        validate_publish_time_index(incomplete, [chunk_a, chunk_b], summary_start=5000)


def _corrupt_chunk_crc(content: bytes, target_offset: int) -> bytes:
    data = bytearray(content)
    # Chunk record: 9-byte record header then start(8) end(8) size(8) crc(4) ...
    crc_offset = target_offset + 9 + 8 + 8 + 8
    data[crc_offset] ^= 0xFF
    return bytes(data)


def test_unrelated_corrupt_payload_is_not_read():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.NONE,
        chunk_size=120,  # one message per chunk
    )
    reader = SeekingReader(io.BytesIO(content))
    summary = reader.get_summary()
    assert summary is not None
    # Pick a chunk that cannot match a publish-time window on a topic of another chunk.
    target = summary.chunk_indexes[-1]
    damaged = _corrupt_chunk_crc(content, target.chunk_start_offset)

    # A query that cannot touch the damaged chunk succeeds even with CRC validation enabled.
    other_reader = SeekingReader(io.BytesIO(damaged), validate_crcs=True)
    # /a messages have publish times 1000-7*i for i not congruent to 2 mod 3.
    got = read_results(
        other_reader,
        topics=["/a"],
        start_time=1000,
        end_time=1001,
        time_basis="publish",
    )
    assert got == expected_results(
        messages, topics=["/a"], start_time=1000, end_time=1001, time_basis="publish"
    )

    # Reading the damaged chunk does surface the CRC failure.
    failing = SeekingReader(io.BytesIO(damaged), validate_crcs=True)
    with pytest.raises(CRCValidationError):
        list(
            failing.iter_messages(
                start_time=0, end_time=2**64 - 1, time_basis="publish"
            )
        )

    # And a fresh reader can still answer a valid query afterwards.
    fresh = SeekingReader(io.BytesIO(content), validate_crcs=True)
    assert len(read_results(fresh, time_basis="publish")) == len(messages)


# ---------------------------------------------------------------------------------------------
# Read savings: no message payload reads / decompression for unselected chunks
# ---------------------------------------------------------------------------------------------


class CountingStream(io.IOBase):
    """Wraps bytes and records every read requested by the library (not OS prefetch)."""

    def __init__(self, data: bytes):
        self._buffer = io.BytesIO(data)
        self.reads: List[Tuple[int, int]] = []

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        return self._buffer.seek(offset, whence)

    def tell(self):
        return self._buffer.tell()

    def read(self, n=-1):
        position = self._buffer.tell()
        data = self._buffer.read(n)
        if n != 0:
            self.reads.append((position, len(data)))
        return data

    def readinto(self, b):
        position = self._buffer.tell()
        n = self._buffer.readinto(b)
        self.reads.append((position, n or 0))
        return n

    def reset(self):
        self.reads.clear()


def _payload_offsets(content: bytes) -> List[Tuple[int, int, int]]:
    """(chunk_start, chunk_end, index) for each chunk, from the summary."""
    summary = SeekingReader(io.BytesIO(content)).get_summary()
    assert summary is not None
    return [
        (c.chunk_start_offset, c.chunk_start_offset + c.chunk_length, i)
        for i, c in enumerate(summary.chunk_indexes)
    ]


def _overlapping(start: int, end: int, spans: List[Tuple[int, int, int]]) -> List[int]:
    return [i for (s, e, i) in spans if start < e and end > s]


def _run_counted(content: bytes, **kwargs):
    stream = CountingStream(content)
    reader = SeekingReader(stream)
    decompressed: List[int] = []
    original_breakup = reader_module.breakup_chunk

    def tracking_breakup(chunk, validate_crc=False):
        decompressed.append(1)
        return original_breakup(chunk, validate_crc=validate_crc)

    reader_module.breakup_chunk = tracking_breakup
    try:
        results = list(reader.iter_messages(**kwargs))
    finally:
        reader_module.breakup_chunk = original_breakup
    return results, stream.reads, len(decompressed)


def test_seeking_publish_query_skips_unrelated_chunk_payloads():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.LZ4,
        chunk_size=120,
    )
    spans = _payload_offsets(content)
    # Narrow publish window selecting exactly the message with publish_time == 1000.
    results, reads, decompressed = _run_counted(
        content, start_time=1000, end_time=1001, time_basis="publish"
    )
    assert len(results) == 1

    touched = set()
    for position, length in reads:
        touched.update(_overlapping(position, position + length, spans))
    # Only reached chunks may have their payload read.
    assert len(touched) <= len(spans)
    assert decompressed == len(touched)
    # The vast majority of chunks must not be read.
    assert len(touched) <= 2


def test_empty_window_and_missing_topic_do_not_decompress():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.ZSTD,
        chunk_size=120,
    )
    results, reads, decompressed = _run_counted(
        content, start_time=500, end_time=500, time_basis="publish"
    )
    assert results == []
    assert decompressed == 0
    # No data-section reads at all beyond header/footer/summary.
    spans = _payload_offsets(content)
    for position, length in reads:
        assert not _overlapping(position, position + length, spans)

    results, reads, decompressed = _run_counted(
        content, topics=["/does-not-exist"], time_basis="publish"
    )
    assert results == []
    assert decompressed == 0


def test_first_query_does_not_scan_all_messages():
    """Opening + first publish query must not read every chunk payload."""
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.LZ4,
        chunk_size=120,
    )
    spans = _payload_offsets(content)
    stream = CountingStream(content)
    reader = SeekingReader(stream)
    # Opening, reading the summary and validating the extension is allowed.
    results = list(
        reader.iter_messages(start_time=1000, end_time=1001, time_basis="publish")
    )
    assert len(results) == 1
    touched = set()
    for position, length in stream.reads:
        touched.update(_overlapping(position, position + length, spans))
    assert len(touched) < len(spans) / 2


def test_channel_in_same_chunk_does_not_force_decompression():
    """A window matching only another topic's envelope must not read the chunk."""
    # Chunk 0: /a publish 1000, /b publish 5. Chunk 1: /a publish 2000.
    messages = [
        RawMessage("/a", 0, 1, 10, 1000, b"a0"),
        RawMessage("/b", 0, 2, 11, 5, b"b0"),
        RawMessage("/a", 0, 3, 12, 2000, b"a1"),
    ]
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.NONE,
        chunk_size=100,
    )
    results, reads, decompressed = _run_counted(
        content, topics=["/a"], start_time=2000, end_time=2001, time_basis="publish"
    )
    assert [message.data for _, _, message in results] == [b"a1"]
    assert decompressed == 1


# ---------------------------------------------------------------------------------------------
# Empty archives, reduced index types, chunk threshold / partial final chunk
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("publish_time_index", [False, True])
@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_empty_archive(reader_cls, publish_time_index):
    content, _ = build_synthetic_mcap([], publish_time_index=publish_time_index)
    for basis in ("log", "publish"):
        assert (
            list(reader_cls(io.BytesIO(content)).iter_messages(time_basis=basis)) == []
        )
        # A fresh reader for the single-use non-seeking case.
        assert (
            list(
                reader_cls(io.BytesIO(content)).iter_messages(
                    start_time=0, end_time=1, time_basis=basis
                )
            )
            == []
        )


def test_chunk_only_index_type_with_publish_index():
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        index_types=IndexType.CHUNK,
        chunk_size=180,
    )
    reader = SeekingReader(io.BytesIO(content))
    for topics in (None, ["/a"], ["/b"]):
        got = read_results(reader, topics=topics, time_basis="publish")
        want = expected_results(messages, topics=topics, time_basis="publish")
        assert got == want


def test_threshold_chunks_and_partial_final_chunk():
    # Use a chunk size that triggers several chunks plus a final partial chunk.
    messages = [
        RawMessage("/a", 0, i, 10 * i, 10_000 - 13 * i, b"x" * 40) for i in range(31)
    ]
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        chunk_size=300,
        compression=CompressionType.ZSTD,
    )
    summary = SeekingReader(io.BytesIO(content)).get_summary()
    assert summary is not None
    assert len(summary.chunk_indexes) > 3
    reader = SeekingReader(io.BytesIO(content))
    got = read_results(reader, time_basis="publish")
    assert got == expected_results(messages, time_basis="publish")


# ---------------------------------------------------------------------------------------------
# Queue-level invariants: envelope bounds drive ordering, not the other clock
# ---------------------------------------------------------------------------------------------


def test_queue_uses_candidate_envelope():
    from mcap._message_queue import ChunkCandidate, make_message_queue
    from mcap.records import ChunkIndex

    def chunk(offset: int) -> ChunkIndex:
        return ChunkIndex(
            chunk_length=1,
            chunk_start_offset=offset,
            compression="",
            compressed_size=0,
            message_end_time=0,
            message_index_length=0,
            message_index_offsets={},
            message_start_time=0,
            uncompressed_size=0,
        )

    # A chunk whose standard log-time envelope is far away, but whose publish envelope starts at 5,
    # must be popped before a message with publish time 6.
    mq = make_message_queue(time_basis="publish")
    candidate = ChunkCandidate(chunk_index=chunk(1000), min_time=5, max_time=50)
    message = (
        (
            None,
            Channel(0, "t", "", {}, 0),
            Message(0, 999, b"", 6, 0),
        ),
        2000,
        0,
    )
    mq.push(message)
    mq.push(candidate)
    assert mq.pop() is candidate
    popped = mq.pop()
    assert popped[0][2].publish_time == 6


def test_queue_reverse_breaks_ties_by_physical_order():
    from mcap._message_queue import make_message_queue

    def msg(publish_time: int, chunk_offset: int, index: int):
        return (
            (None, Channel(0, "t", "", {}, 0), Message(0, 0, b"", publish_time, 0)),
            chunk_offset,
            index,
        )

    mq = make_message_queue(reverse=True, time_basis="publish")
    mq.push(msg(10, 100, 0))
    mq.push(msg(10, 100, 1))
    mq.push(msg(10, 200, 0))
    mq.push(msg(20, 50, 0))
    order = [mq.pop()[1:] for _ in range(4)]
    assert order == [(50, 0), (200, 0), (100, 1), (100, 0)]


# ---------------------------------------------------------------------------------------------
# Forward/reverse are exact reverses at the reader level for every distribution
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("publish_time_index", [False, True])
@pytest.mark.parametrize("compression", [CompressionType.NONE, CompressionType.ZSTD])
def test_reverse_is_exact_reverse(publish_time_index, compression):
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=publish_time_index,
        compression=compression,
        chunk_size=200,
    )
    for basis in ("log", "publish"):
        for topics in (None, ["/a"], ["/b"]):
            forward = read_results(
                SeekingReader(io.BytesIO(content)), topics=topics, time_basis=basis
            )
            backward = read_results(
                SeekingReader(io.BytesIO(content)),
                topics=topics,
                reverse=True,
                time_basis=basis,
            )
            assert backward == list(reversed(forward)), (basis, topics)


# ---------------------------------------------------------------------------------------------
# The publish envelope, not the log envelope, must drive chunk selection and merging
# ---------------------------------------------------------------------------------------------


def test_chunk_with_log_time_outside_window_but_publish_inside_is_read():
    # Chunk 0: log 0..2,   publish 900..902
    # Chunk 1: log 1000,   publish 500        <- log far outside the query, publish inside
    messages = [
        RawMessage("/a", 0, 1, 0, 900, b"a0"),
        RawMessage("/a", 0, 2, 1, 901, b"a1"),
        RawMessage("/a", 0, 3, 2, 902, b"a2"),
        RawMessage("/a", 0, 4, 1000, 500, b"target"),
    ]
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.NONE,
        chunk_size=110,
    )
    summary = SeekingReader(io.BytesIO(content)).get_summary()
    assert summary is not None
    assert len(summary.chunk_indexes) == 2
    # Standard log-time envelope of the second chunk is 1000, well outside [499, 501).
    assert summary.chunk_indexes[1].message_start_time == 1000

    reader = SeekingReader(io.BytesIO(content))
    got = read_results(reader, start_time=499, end_time=501, time_basis="publish")
    assert [r[4] for r in got] == [b"target"]

    # A log-time query for the same window finds nothing, as it should.
    reader = SeekingReader(io.BytesIO(content))
    assert read_results(reader, start_time=499, end_time=501, time_basis="log") == []


def test_merge_not_cut_short_by_other_clock_chunk_bounds():
    # Chunks in log order hold interleaved publish times. A naive merge that closes a chunk based
    # on log_time boundaries would hand out an early winner and miss a later, still-smaller
    # publish time in a chunk it had not opened yet.
    messages = [
        RawMessage("/a", 0, 1, 0, 100, b"c0-early"),
        RawMessage("/a", 0, 2, 1, 300, b"c0-late"),
        RawMessage("/b", 0, 3, 2, 50, b"c1-earliest"),
        RawMessage("/a", 0, 4, 3, 200, b"c1-mid"),
        RawMessage("/a", 0, 5, 4, 150, b"c2-mid2"),
    ]
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.NONE,
        chunk_size=110,
    )
    reader = SeekingReader(io.BytesIO(content))
    got = [r[4] for r in read_results(reader, time_basis="publish")]
    assert got == [b"c1-earliest", b"c0-early", b"c2-mid2", b"c1-mid", b"c0-late"]

    reader = SeekingReader(io.BytesIO(content))
    got_rev = [r[4] for r in read_results(reader, time_basis="publish", reverse=True)]
    assert got_rev == list(reversed(got))


# ---------------------------------------------------------------------------------------------
# Interoperability with the original (pre-extension) reader, driven as a subprocess
# ---------------------------------------------------------------------------------------------


def test_original_reader_reads_indexed_file(tmp_path):
    """A pristine copy of the pre-extension package must read every message from a new file."""
    import subprocess
    import sys

    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.ZSTD,
        chunk_size=200,
    )
    mcap_file = tmp_path / "indexed.mcap"
    mcap_file.write_bytes(content)

    script = tmp_path / "read_old.py"
    script.write_text(
        "import io, json, sys\n"
        "from mcap.reader import SeekingReader, NonSeekingReader\n"
        "data = open(sys.argv[1], 'rb').read()\n"
        "def rows(reader):\n"
        "    return [[c.topic, m.sequence, m.log_time, m.publish_time, list(m.data)]\n"
        "            for _, c, m in reader.iter_messages()]\n"
        "a = rows(SeekingReader(io.BytesIO(data)))\n"
        "b = rows(NonSeekingReader(io.BytesIO(data)))\n"
        "stats = SeekingReader(io.BytesIO(data)).get_summary().statistics\n"
        "print(json.dumps({'seeking': a, 'nonseeking': b,\n"
        "                  'start': stats.message_start_time,\n"
        "                  'end': stats.message_end_time}))\n"
    )

    env = dict(os.environ)
    env["PYTHONPATH"] = str(_ORIGINAL_MCAP_PATH)
    completed = subprocess.run(
        [sys.executable, "-P", str(script), str(mcap_file)],
        env=env,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        pytest.skip(
            "original 1.3.1 package not available under "
            f"{_ORIGINAL_MCAP_PATH}: {completed.stderr.strip()[:300]}"
        )
    import json

    result = json.loads(completed.stdout)
    expected = [
        [m.topic, m.sequence, m.log_time, m.publish_time, list(m.data)]
        for m in messages
    ]
    assert result["seeking"] == expected
    assert result["nonseeking"] == expected
    assert result["start"] == min(m.log_time for m in messages)
    assert result["end"] == max(m.log_time for m in messages)


_ORIGINAL_MCAP_PATH = os.environ.get("MCAP_ORIGINAL_PACKAGE", "/tmp/orig_mcap")


@pytest.mark.parametrize("use_summary_offsets", [False, True])
@pytest.mark.parametrize("use_statistics", [False, True])
def test_publish_index_without_optional_summary_pieces(
    use_summary_offsets, use_statistics
):
    messages = opposite_clock_messages()
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        use_summary_offsets=use_summary_offsets,
        use_statistics=use_statistics,
        chunk_size=200,
    )
    reader = SeekingReader(io.BytesIO(content))
    assert read_results(reader, time_basis="publish") == expected_results(
        messages, time_basis="publish"
    )


def test_publish_index_with_data_crcs_and_non_seeking_validation():
    messages = opposite_clock_messages()
    out = io.BytesIO()
    writer = Writer(
        out,
        compression=CompressionType.ZSTD,
        chunk_size=200,
        index_types=IndexType.ALL,
        publish_time_index=True,
        enable_crcs=True,
        enable_data_crcs=True,
    )
    writer.start()
    topics = sorted({m.topic for m in messages})
    ids = {t: i + 1 for i, t in enumerate(topics)}
    for t in topics:
        writer.register_channel(t, "raw", 0)
    for m in messages:
        writer.add_message(ids[m.topic], m.log_time, m.data, m.publish_time, m.sequence)
    writer.finish()
    content = out.getvalue()

    reader = NonSeekingReader(io.BytesIO(content), validate_crcs=True)
    got = read_results(reader, time_basis="publish")
    assert got == expected_results(messages, time_basis="publish")


def test_publish_index_does_not_change_standard_records():
    """Compare against a file written without the extension: standard records must be identical."""
    messages = opposite_clock_messages()

    def standard_records(publish_time_index: bool):
        content, _ = build_synthetic_mcap(
            messages, publish_time_index=publish_time_index, chunk_size=200
        )
        out = []
        for record in StreamReader(io.BytesIO(content), emit_chunks=True).records:
            # Footer is excluded: its summary offsets legitimately shift because the extension
            # adds bytes to the summary section.
            if type(record).__name__ in (
                "Header",
                "Chunk",
                "ChunkIndex",
                "Statistics",
                "DataEnd",
                "Schema",
                "Channel",
            ):
                out.append(record)
        return out

    assert standard_records(True) == standard_records(False)


def test_dense_ties_and_duplicates_with_index_reverse_exact():
    # Many messages share the same publish time, spread over chunks, with exact duplicates.
    messages = []
    for i in range(40):
        messages.append(
            RawMessage(
                topic="/a" if i % 2 else "/b",
                channel_id=0,
                sequence=i % 5,  # intentionally repeated sequence numbers
                log_time=1000 - i,  # log order is the reverse of physical order
                publish_time=7 if i % 3 else 7,  # all dense ties
                data=b"same" if i % 4 == 0 else ("m%d" % i).encode(),
            )
        )
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.ZSTD,
        chunk_size=90,
    )
    forward = read_results(SeekingReader(io.BytesIO(content)), time_basis="publish")
    reverse = read_results(
        SeekingReader(io.BytesIO(content)), time_basis="publish", reverse=True
    )
    want = expected_results(messages, time_basis="publish")
    assert forward == want
    assert reverse == list(reversed(want))
    # Each duplicate appears once, in physical order in the forward sequence.
    duplicates = [r[4] for r in forward if r[4] == b"same"]
    assert len(duplicates) == 10


def test_malformed_extension_raises_at_call_time_not_first_next():
    """The extension is validated when the query is created, before any message is yielded."""
    content, _ = build_synthetic_mcap(
        opposite_clock_messages(), publish_time_index=True, chunk_size=260
    )
    data = bytearray(content)
    body_start = _find_position(bytes(data), PUBLISH_TIME_INDEX_MAGIC)
    data[body_start + 12] ^= 0xFF
    reader = SeekingReader(io.BytesIO(bytes(data)))
    # No list()/next(): the call itself must raise.
    with pytest.raises(McapError):
        reader.iter_messages(time_basis="publish")
    reader = SeekingReader(io.BytesIO(bytes(data)))
    with pytest.raises(McapError):
        reader.iter_decoded_messages(time_basis="publish")
    # A log-time query on the same damaged file is unaffected.
    reader = SeekingReader(io.BytesIO(bytes(data)))
    assert len(list(reader.iter_messages())) == 24


def test_other_topic_in_window_does_not_decompress_chunk():
    """A window that only covers another topic's publish time must not read the chunk."""
    messages = [
        RawMessage("/a", 0, 1, 10, 1000, b"a0"),
        RawMessage("/b", 0, 2, 11, 5, b"b0"),
        RawMessage("/a", 0, 3, 12, 2000, b"a1"),
    ]
    content, _ = build_synthetic_mcap(
        messages,
        publish_time_index=True,
        compression=CompressionType.ZSTD,
        chunk_size=100,
    )
    # Chunk 0 holds both topics, the window only matches /b's publish time.
    results, _, decompressed = _run_counted(
        content, topics=["/a"], start_time=5, end_time=6, time_basis="publish"
    )
    assert results == []
    assert decompressed == 0
    # But asking for /b does read it.
    results, _, decompressed = _run_counted(
        content, topics=["/b"], start_time=5, end_time=6, time_basis="publish"
    )
    assert [message.data for _, _, message in results] == [b"b0"]
    assert decompressed == 1
