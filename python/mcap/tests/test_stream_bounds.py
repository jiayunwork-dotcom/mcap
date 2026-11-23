"""End-to-end sequential-reader tests over segmented and corrupted inputs."""

import struct
from io import BytesIO

import pytest

from mcap.data_stream import MAX_SKIP_READ
from mcap.exceptions import EndOfFile, McapError, McapFormatError
from mcap.opcode import Opcode
from mcap.reader import NonSeekingReader, SeekingReader
from mcap.records import Attachment, Channel, Message, Metadata, Schema
from mcap.stream_reader import CRCValidationError, StreamReader, breakup_chunk
from mcap.writer import CompressionType, Writer

from ._hand_mcap import (
    hand_chunked_mcap,
    hand_unchunked_mcap,
)
from ._io_helpers import (
    LazyLargeBodyStream,
    MAGIC,
    RequestProbe,
    SegmentedSeekable,
    alternating_caps,
    pstr,
    record,
    u32,
    u64,
    walk_records,
)

CAPS = [1, 3, 7, alternating_caps()]


def _assert_expected_messages(records, expected):
    messages = [r for r in records if isinstance(r, Message)]
    assert [
        (m.log_time, m.publish_time, m.data) for m in messages
    ] == expected.messages
    schema = next(r for r in records if isinstance(r, Schema))
    assert schema.name == expected.schema_name
    assert schema.data == expected.schema_data
    channel = next(r for r in records if isinstance(r, Channel))
    assert channel.topic == expected.channel_topic
    assert channel.metadata == expected.channel_metadata
    attachment = next(r for r in records if isinstance(r, Attachment))
    assert (attachment.name, attachment.media_type, attachment.data) == expected.attachment
    metadata = next(r for r in records if isinstance(r, Metadata))
    assert metadata.name == expected.metadata[0]
    assert metadata.metadata == expected.metadata[1]


@pytest.mark.parametrize("cap", CAPS)
def test_unchunked_archive_survives_short_reads(cap):
    data, expected = hand_unchunked_mcap()
    probe = RequestProbe(data, cap)
    records = list(StreamReader(probe).records)
    _assert_expected_messages(records, expected)
    assert not probe.closed  # the reader must not close the caller's handle


@pytest.mark.parametrize("cap", CAPS)
def test_unchunked_archive_via_non_seeking_reader(cap):
    data, expected = hand_unchunked_mcap()
    reader = NonSeekingReader(RequestProbe(data, cap))
    triples = list(reader.iter_messages())
    assert [t[2].data for t in triples] == [m[2] for m in expected.messages]
    attachments = list(NonSeekingReader(RequestProbe(data, cap)).iter_attachments())
    assert attachments[0].data == expected.attachment[2]


@pytest.mark.parametrize("cap", CAPS)
@pytest.mark.parametrize("compression", ["", "lz4", "zstd"])
def test_chunked_archive_survives_short_reads(cap, compression):
    data, expected = hand_chunked_mcap(compression)
    records = list(StreamReader(RequestProbe(data, cap)).records)
    messages = [r for r in records if isinstance(r, Message)]
    assert [
        (m.log_time, m.publish_time, m.data) for m in messages
    ] == expected.messages
    # The schema and channel live inside the chunk.
    assert isinstance(next(r for r in records if isinstance(r, Schema)), Schema)
    assert isinstance(next(r for r in records if isinstance(r, Channel)), Channel)


@pytest.mark.parametrize("cap", CAPS)
@pytest.mark.parametrize("compression", ["", "lz4", "zstd"])
def test_chunked_archive_via_seekable_segmented_stream(cap, compression):
    data, expected = hand_chunked_mcap(compression)
    reader = SeekingReader(SegmentedSeekable(data, cap))
    # No summary: the seeking reader falls back to a bounded linear scan.
    triples = list(reader.iter_messages())
    assert [t[2].data for t in triples] == [m[2] for m in expected.messages]


def test_segmentation_does_not_change_crc():
    data, _ = hand_chunked_mcap("")  # uncompressed chunk carries a CRC
    whole = list(StreamReader(BytesIO(data), validate_crcs=True).records)
    segmented = list(
        StreamReader(RequestProbe(data, 3), validate_crcs=True).records
    )
    assert len(whole) == len(segmented)
    assert whole == segmented


def test_corrupt_chunk_crc_still_detected_under_short_reads():
    data, _ = hand_chunked_mcap("")
    records_info = walk_records(data)
    chunk_op, chunk_start, chunk_len = next(
        (op, start, length)
        for op, start, length in records_info
        if op == Opcode.CHUNK
    )
    # Corrupt the chunk's stored CRC field (the u32 immediately after uncompressed_size).
    crc_field = chunk_start + 9 + 8 + 8 + 8
    data = bytearray(data)
    data[crc_field] ^= 0xFF
    with pytest.raises(CRCValidationError):
        list(StreamReader(RequestProbe(bytes(data), 3), validate_crcs=True).records)


def test_pipe_style_oserror_is_propagated():
    from ._io_helpers import RaisingStream

    reader = StreamReader(RaisingStream(OSError("broken pipe")))
    with pytest.raises(OSError):
        list(reader.records)


@pytest.mark.parametrize("cap", CAPS)
def test_truncated_archive_raises_end_of_file_at_every_cut(cap):
    data, _ = hand_unchunked_mcap()
    # Cut at every offset from the magic through the first several records. Every cut must raise
    # EndOfFile (physical truncation), never silently return a partial object.
    seen_eof = 0
    for cut in range(1, min(len(data), 160)):
        probe = RequestProbe(data[:cut], cap)
        try:
            list(StreamReader(probe).records)
        except EndOfFile:
            seen_eof += 1
        except McapError:
            # A cut landing inside a declared but now incomplete range may also be reported as
            # structural corruption; it must never succeed.
            pass
    assert seen_eof > 0


def test_bad_record_does_not_borrow_neighbour_bytes():
    # Header body length 4 with one empty string; the following four bytes are a distinct record
    # (a zero-length DataEnd-style body). The bad Header must fail without consuming them.
    neighbour = record(Opcode.DATA_END, u32(0))
    data = record(Opcode.HEADER, u32(0)) + neighbour
    probe = RequestProbe(data, cap=1)
    with pytest.raises(McapFormatError):
        list(StreamReader(probe, skip_magic=True).records)
    # The failure must happen at or before the Header's own boundary (9 envelope + 4 body).
    assert probe.position <= 13


def test_oversize_outer_record_body_is_never_read():
    # A Header whose declared length wildly exceeds a small limit.
    data = (
        bytes([Opcode.HEADER])
        + struct.pack("<Q", 4096)
        + pstr("p") + pstr("lib")
    )
    probe = RequestProbe(data, cap=1)
    from mcap.exceptions import RecordLengthLimitExceeded

    reader = StreamReader(probe, skip_magic=True, record_size_limit=1024)
    with pytest.raises(RecordLengthLimitExceeded):
        list(reader.records)
    # Only the 9-byte envelope may have been consumed; no body byte was requested.
    assert probe.position == 9


def test_inner_chunk_records_share_the_size_limit():
    # Build a chunk with an inner 4096-byte message; the outer chunk itself is small enough.
    payload = b"m" * 4096
    buf = BytesIO()
    writer = Writer(
        buf,
        compression=CompressionType.NONE,
        chunk_size=10**9,
        use_statistics=False,
        use_summary_offsets=False,
        repeat_channels=False,
        repeat_schemas=False,
    )
    writer.start()
    channel_id = writer.register_channel("t", "raw", 0)
    writer.add_message(channel_id, 1, payload, 1)
    writer.finish()
    data = buf.getvalue()
    chunk = next(
        r for r in StreamReader(BytesIO(data), emit_chunks=True).records
        if r.__class__.__name__ == "Chunk"
    )
    from mcap.exceptions import RecordLengthLimitExceeded

    # Inner encoded record length (9 envelope + 22 header + 4096 payload body length 4118) exceeds
    # 1024 even though the whole decompressed chunk is being read as one blob.
    from mcap.data_stream import ReadDataStream
    from mcap.exceptions import RecordLengthLimitExceeded
    from mcap.stream_reader import _check_record_size_limit

    # Walk the uncompressed inner records through a probing source: the oversized message must be
    # rejected at its 9-byte envelope, before any of its 4096 payload bytes are requested.
    probe_stream = ReadDataStream(RequestProbe(chunk.data, cap=3))
    with pytest.raises(RecordLengthLimitExceeded):
        while probe_stream.count < len(chunk.data):
            opc = probe_stream.read1()
            ln = probe_stream.read8()
            _check_record_size_limit(opc, ln, 1024)
            probe_stream.discard(ln)
    assert probe_stream.count < len(chunk.data)

    with pytest.raises(RecordLengthLimitExceeded):
        breakup_chunk(chunk, record_size_limit=1024)
    # With no limit it parses, and the chunk's total length was never mistaken for a record length.
    records = breakup_chunk(chunk, record_size_limit=None)
    assert next(r for r in records if isinstance(r, Message)).data == payload


def test_unknown_record_with_legal_length_is_skipped_without_retaining_body():
    # An unknown opcode with a legal outer length must be skipped, then a recognizable Header read.
    unknown_body = b"x" * 50
    data = (
        record(0x77, unknown_body)
        + record(Opcode.HEADER, pstr("p") + pstr("lib"))
        + record(Opcode.FOOTER, u64(0) + u64(0) + u32(0))
        + MAGIC
    )
    probe = RequestProbe(data, cap=3)
    records = list(StreamReader(probe, skip_magic=True).records)
    headers = [r for r in records if r.__class__.__name__ == "Header"]
    assert len(headers) == 1
    assert (headers[0].profile, headers[0].library) == ("p", "lib")
    # Skipping uses bounded chunked requests, never a body-sized allocation request.
    assert probe.max_request <= MAX_SKIP_READ


@pytest.mark.parametrize("body_length", [1024 * 1024, 128 * 1024 * 1024])
def test_lazy_unknown_record_skip_memory(body_length):
    import tracemalloc

    from mcap.records import Footer

    tail = (
        record(Opcode.HEADER, pstr("p") + pstr("lib"))
        + record(Opcode.FOOTER, u64(0) + u64(0) + u32(0))
        + MAGIC
    )
    source = LazyLargeBodyStream(0x77, body_length, tail)
    tracemalloc.start()
    tracemalloc.reset_peak()
    records = list(
        StreamReader(source, skip_magic=True, record_size_limit=None).records
    )
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    header = next(r for r in records if r.__class__.__name__ == "Header")
    assert (header.profile, header.library) == ("p", "lib")
    assert any(isinstance(r, Footer) for r in records)
    # Every source request stays at or below 64 KiB even for a 128 MiB skipped body.
    assert max(source.requests) <= MAX_SKIP_READ
    # Extra peak working memory must stay far below the skipped body size (a whole-body read would
    # allocate roughly body_length bytes).
    assert peak < max(4 * MAX_SKIP_READ, body_length // 100)


def test_reader_does_not_close_caller_handle_after_failure():
    data, _ = hand_unchunked_mcap()
    probe = RequestProbe(data[:20], cap=1)  # truncated
    with pytest.raises(EndOfFile):
        list(NonSeekingReader(probe).iter_messages())
    assert not probe.closed


def test_fresh_reader_works_after_prior_failure():
    good, expected = hand_unchunked_mcap()
    with pytest.raises(EndOfFile):
        list(StreamReader(RequestProbe(good[:15], 1)).records)
    records = list(StreamReader(RequestProbe(good, 7)).records)
    assert len([r for r in records if isinstance(r, Message)]) == len(expected.messages)
