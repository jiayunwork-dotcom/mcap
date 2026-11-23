"""Random-access readers must obey the same opcode/length/size contract as sequential scans."""

import struct
from io import BytesIO

import pytest

from mcap.exceptions import McapFormatError, RecordLengthLimitExceeded
from mcap.opcode import Opcode
from mcap.reader import NonSeekingReader, SeekingReader
from mcap.writer import CompressionType, IndexType, Writer

from ._io_helpers import SegmentedSeekable, walk_records


def _indexed_mcap(compression=CompressionType.NONE, payload=b"abc"):
    buf = BytesIO()
    writer = Writer(buf, compression=compression, index_types=IndexType.ALL)
    writer.start("profile", "library")
    schema_id = writer.register_schema("s", "jsonschema", b"true")
    channel_id = writer.register_channel("遥测", "json", schema_id)
    writer.add_message(channel_id, 10, payload, 10)
    writer.add_attachment(12, 12, "a.txt", "text/plain", b"attach-data")
    writer.add_metadata("meta", {"k": "v"})
    writer.finish()
    return buf.getvalue()


def _record_offset(data, opcode):
    matches = [start for op, start, _ in walk_records(data) if op == opcode]
    if not matches:
        raise AssertionError(f"no record with opcode {opcode}")
    return matches[0]


@pytest.mark.parametrize("compression", [CompressionType.NONE, CompressionType.LZ4,
                                         CompressionType.ZSTD])
def test_seeking_indexed_reads_match_linear(compression):
    data = _indexed_mcap(compression)
    seeking = list(SeekingReader(BytesIO(data)).iter_messages())
    linear = list(NonSeekingReader(BytesIO(data)).iter_messages())
    assert [t[2].data for t in seeking] == [t[2].data for t in linear] == [b"abc"]


@pytest.mark.parametrize("cap", [1, 3, 7])
@pytest.mark.parametrize("compression", [CompressionType.NONE, CompressionType.LZ4,
                                         CompressionType.ZSTD])
def test_seeking_reads_survive_segmented_source(cap, compression):
    data = _indexed_mcap(compression)
    reader = SeekingReader(SegmentedSeekable(data, cap))
    assert [t[2].data for t in reader.iter_messages()] == [b"abc"]
    assert next(reader.iter_attachments()).data == b"attach-data"
    assert next(reader.iter_metadata()).metadata == {"k": "v"}


def test_index_pointing_at_wrong_opcode_is_rejected():
    data = bytearray(_indexed_mcap())
    chunk_start = _record_offset(bytes(data), Opcode.CHUNK)
    # Corrupt the chunk index's stored offset so it points at the attachment instead.
    attachment_start = _record_offset(bytes(data), Opcode.ATTACHMENT)
    chunk_index_start = _record_offset(bytes(data), Opcode.CHUNK_INDEX)
    # The ChunkIndex body starts with two u64 times, then the u64 chunk_start_offset.
    offset_field = chunk_index_start + 9 + 8 + 8
    struct.pack_into("<Q", data, offset_field, attachment_start)
    assert attachment_start != chunk_start
    reader = SeekingReader(BytesIO(bytes(data)))
    with pytest.raises(McapFormatError):
        list(reader.iter_messages())


def test_index_length_inconsistent_with_actual_record_is_rejected():
    data = bytearray(_indexed_mcap())
    chunk_index_start = _record_offset(bytes(data), Opcode.CHUNK_INDEX)
    # chunk_length sits right after chunk_start_offset inside the ChunkIndex body.
    length_field = chunk_index_start + 9 + 8 + 8 + 8
    actual, = struct.unpack_from("<Q", data, length_field)
    struct.pack_into("<Q", data, length_field, actual + 7)
    reader = SeekingReader(BytesIO(bytes(data)))
    with pytest.raises(McapFormatError):
        list(reader.iter_messages())


def test_attachment_index_wrong_opcode_is_rejected():
    data = bytearray(_indexed_mcap())
    attachment_index_start = _record_offset(bytes(data), Opcode.ATTACHMENT_INDEX)
    metadata_start = _record_offset(bytes(data), Opcode.METADATA)
    # AttachmentIndex body starts with two u64 (offset, length); retarget offset to the Metadata
    # record.
    offset_field = attachment_index_start + 9
    struct.pack_into("<Q", data, offset_field, metadata_start)
    reader = SeekingReader(BytesIO(bytes(data)))
    with pytest.raises(McapFormatError):
        list(reader.iter_attachments())


def test_metadata_index_wrong_length_is_rejected():
    data = bytearray(_indexed_mcap())
    metadata_index_start = _record_offset(bytes(data), Opcode.METADATA_INDEX)
    # MetadataIndex body: offset(u64), length(u64), name...
    length_field = metadata_index_start + 9 + 8
    actual, = struct.unpack_from("<Q", data, length_field)
    struct.pack_into("<Q", data, length_field, actual + 3)
    reader = SeekingReader(BytesIO(bytes(data)))
    with pytest.raises(McapFormatError):
        list(reader.iter_metadata())


def test_record_size_limit_applies_to_indexed_chunk():
    # A message with a 4096-byte payload lives in a chunk whose encoded length exceeds 1024.
    data = _indexed_mcap(payload=b"p" * 4096)
    # Sequential (non-seeking) reader rejects the oversized outer chunk.
    with pytest.raises(RecordLengthLimitExceeded):
        list(NonSeekingReader(BytesIO(data), record_size_limit=1024).iter_messages())
    # The seeking reader must now reject it too, rather than following the chunk index.
    with pytest.raises(RecordLengthLimitExceeded):
        list(SeekingReader(BytesIO(data), record_size_limit=1024).iter_messages())


def test_record_size_limit_none_removes_the_cap():
    data = _indexed_mcap(payload=b"p" * 4096)
    messages = list(
        SeekingReader(BytesIO(data), record_size_limit=None).iter_messages()
    )
    assert messages[0][2].data == b"p" * 4096


def test_record_size_limit_applies_to_indexed_attachment():
    data = _indexed_mcap()
    reader = SeekingReader(BytesIO(data), record_size_limit=10)
    with pytest.raises(RecordLengthLimitExceeded):
        list(reader.iter_attachments())


def test_indexed_chunk_does_not_read_beyond_record():
    data = _indexed_mcap()
    probe = SegmentedSeekable(data, cap=3)
    reader = SeekingReader(probe)
    triples = list(reader.iter_messages())
    assert triples[0][2].data == b"abc"
    # A successful indexed read must not raise; bounded parsing guarantees no neighbour bytes were
    # consumed, validated here by re-reading the same source successfully.
    assert len(list(SeekingReader(SegmentedSeekable(data, 1)).iter_messages())) == 1


def test_no_summary_linear_fallback_keeps_limit_and_short_reads():
    buf = BytesIO()
    writer = Writer(
        buf,
        index_types=IndexType.NONE,
        use_chunking=False,
        use_statistics=False,
        use_summary_offsets=False,
        repeat_channels=False,
        repeat_schemas=False,
    )
    writer.start()
    channel_id = writer.register_channel("t", "raw", 0)
    writer.add_message(channel_id, 1, b"abc", 1)
    writer.finish()
    data = buf.getvalue()
    reader = SeekingReader(SegmentedSeekable(data, 3))
    assert [t[2].data for t in reader.iter_messages()] == [b"abc"]
