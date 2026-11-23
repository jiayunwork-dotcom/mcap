"""A private MCAP extension that indexes message publish times.

MCAP's standard ``ChunkIndex``, ``MessageIndex`` and ``Statistics`` records describe message
*log* times.  A message's ``publish_time`` is allowed to be in a completely different order, so
those standard records cannot be used to answer a publish-time query without reading (and
decompressing) every message.

When the :py:class:`~mcap.writer.Writer` is constructed with ``publish_time_index=True`` it
writes one private record into the summary section of the file.  The record contains, for every
chunk, the minimum and maximum ``publish_time`` observed for every channel that has messages in
that chunk.  This "per chunk, per channel envelope" is enough to:

* skip a chunk entirely when none of the selected channels can contain a message in the queried
  publish-time interval, and
* use the envelope as the merge boundary in the existing message priority queue.

The record is a private record (opcode ``0x80``, in the range reserved by the MCAP spec for
application extensions), so readers that do not know about it skip it and still read every
message from the file.  The record starts with a magic string so that unrelated private records
using the same opcode are never mistaken for this extension.

Layout (little endian, all integers unsigned)::

    magic           8 bytes   b"MCAPPTI\\x00"
    version         u32       1
    entry_count     u64
    entries         entry_count times:
        chunk_start_offset  u64
        chunk_length        u64
        channel_count       u32       (>= 1)
        channels            channel_count times:
            channel_id          u16
            publish_min_time    u64
            publish_max_time    u64   (>= publish_min_time)
    crc32           u32       CRC-32 of every preceding byte of the record

The record's CRC only protects against accidental damage to the index itself.  The index is also
bound to the standard summary section: every entry must reference exactly one ``ChunkIndex`` with
the same ``chunk_start_offset`` and ``chunk_length``, the number of entries must match the number
of ``ChunkIndex`` records, and channel ids must be unique within an entry.  When the file also
contains standard ``MessageIndex`` records, their channel set is required to match the extension's
channel set.  This makes an extension/standard-index desynchronisation detectable without reading
any message payload.  A producer that consistently rewrites every checksum and every index is out
of scope, as is validating that the stored envelope really matches the (unread) chunk payload.
"""

import struct
import zlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .data_stream import RecordBuilder
from .exceptions import McapError
from .opcode import Opcode
from .records import ChunkIndex

PUBLISH_TIME_INDEX_MAGIC = b"MCAPPTI\x00"
PUBLISH_TIME_INDEX_VERSION = 1
PUBLISH_TIME_INDEX_OPCODE = 0x80

#: Smallest possible serialized Chunk record: opcode + length + the fixed chunk header + an empty
#: compression string + an empty data length.  Used to reject obviously out-of-range lengths.
MIN_CHUNK_RECORD_LENGTH = 1 + 8 + 8 + 8 + 8 + 4 + 4 + 8

_ENTRY_HEADER_SIZE = 8 + 8 + 4
_CHANNEL_ENTRY_SIZE = 2 + 8 + 8


@dataclass
class PublishTimeIndexEntry:
    """Publish-time envelope for one chunk, keyed by channel id."""

    chunk_start_offset: int
    chunk_length: int
    #: channel id -> (minimum publish_time, maximum publish_time) for messages of that channel in
    #: this chunk.  Every channel with at least one message in the chunk must be present.
    channel_publish_times: Dict[int, Tuple[int, int]]


@dataclass
class PublishTimeIndex:
    """Parsed contents of the private publish-time index record."""

    entries: List[PublishTimeIndexEntry]
    version: int = PUBLISH_TIME_INDEX_VERSION


def encode_publish_time_index(entries: List[PublishTimeIndexEntry]) -> bytes:
    """Serialize the body (magic, header, entries and CRC) of a publish-time index record."""
    parts = [
        PUBLISH_TIME_INDEX_MAGIC,
        struct.pack("<IQ", PUBLISH_TIME_INDEX_VERSION, len(entries)),
    ]
    for entry in entries:
        parts.append(
            struct.pack(
                "<QQI",
                entry.chunk_start_offset,
                entry.chunk_length,
                len(entry.channel_publish_times),
            )
        )
        for channel_id in sorted(entry.channel_publish_times):
            publish_min, publish_max = entry.channel_publish_times[channel_id]
            parts.append(struct.pack("<HQQ", channel_id, publish_min, publish_max))
    body = b"".join(parts)
    return body + struct.pack("<I", zlib.crc32(body))


def write_publish_time_index(
    stream: RecordBuilder, entries: List[PublishTimeIndexEntry]
) -> None:
    """Write the private publish-time index record into ``stream``."""
    stream.start_record(Opcode.PUBLISH_TIME_INDEX)
    stream.write(encode_publish_time_index(entries))
    stream.finish_record()


def parse_publish_time_index(data: bytes) -> PublishTimeIndex:
    """Parse and structurally check the body of a publish-time index record.

    Raises :py:class:`~mcap.exceptions.McapError` for an unknown version, a truncated or
    over-long record, a duplicate channel entry within a chunk, or a bad CRC.
    """
    magic_size = len(PUBLISH_TIME_INDEX_MAGIC)
    if len(data) < magic_size + 4 + 8 + 4:
        raise McapError("publish time index record is truncated")
    if data[:magic_size] != PUBLISH_TIME_INDEX_MAGIC:
        raise McapError("not a publish time index record")

    body = data[:-4]
    stored_crc = struct.unpack_from("<I", data, len(data) - 4)[0]
    if zlib.crc32(body) != stored_crc:
        raise McapError("publish time index record failed CRC validation")

    version, entry_count = struct.unpack_from("<IQ", body, magic_size)
    if version != PUBLISH_TIME_INDEX_VERSION:
        raise McapError(f"unsupported publish time index version {version}")

    position = magic_size + 4 + 8
    entries: List[PublishTimeIndexEntry] = []
    for _ in range(entry_count):
        if len(body) - position < _ENTRY_HEADER_SIZE:
            raise McapError("publish time index record is truncated")
        chunk_start_offset, chunk_length, channel_count = struct.unpack_from(
            "<QQI", body, position
        )
        position += _ENTRY_HEADER_SIZE
        if channel_count == 0:
            raise McapError(
                "publish time index entry with no channels (incomplete index)"
            )
        if len(body) - position < channel_count * _CHANNEL_ENTRY_SIZE:
            raise McapError(
                "publish time index channel entries exceed the record length"
            )
        channel_publish_times: Dict[int, Tuple[int, int]] = {}
        for _ in range(channel_count):
            channel_id, publish_min, publish_max = struct.unpack_from(
                "<HQQ", body, position
            )
            position += _CHANNEL_ENTRY_SIZE
            if channel_id in channel_publish_times:
                raise McapError(
                    f"duplicate channel {channel_id} in publish time index entry"
                )
            if publish_min > publish_max:
                raise McapError(
                    f"publish time index channel {channel_id} has min > max"
                )
            channel_publish_times[channel_id] = (publish_min, publish_max)
        entries.append(
            PublishTimeIndexEntry(
                chunk_start_offset=chunk_start_offset,
                chunk_length=chunk_length,
                channel_publish_times=channel_publish_times,
            )
        )

    if position != len(body):
        raise McapError(
            "publish time index record has trailing bytes beyond its declared entries"
        )
    return PublishTimeIndex(entries=entries, version=version)


def validate_publish_time_index(
    index: PublishTimeIndex,
    chunk_indexes: List[ChunkIndex],
    summary_start: Optional[int] = None,
) -> Dict[int, PublishTimeIndexEntry]:
    """Cross-check an extension against the standard summary and return a map by offset.

    ``summary_start`` is the absolute offset of the summary section when known; data section
    chunk offsets and lengths are then required to stay below it.  Returns a mapping from
    ``chunk_start_offset`` to extension entry.
    """
    entries = index.entries
    if len(entries) != len(chunk_indexes):
        raise McapError(
            "publish time index does not have exactly one entry per ChunkIndex "
            f"({len(entries)} entries, {len(chunk_indexes)} chunks)"
        )

    by_offset: Dict[int, PublishTimeIndexEntry] = {}
    for entry in entries:
        if entry.chunk_start_offset in by_offset:
            raise McapError(
                "duplicate chunk reference in publish time index: "
                f"offset {entry.chunk_start_offset}"
            )
        by_offset[entry.chunk_start_offset] = entry

    result: Dict[int, PublishTimeIndexEntry] = {}
    for chunk_index in chunk_indexes:
        found = by_offset.get(chunk_index.chunk_start_offset)
        if found is None:
            raise McapError(
                "publish time index is incomplete: no entry for chunk at offset "
                f"{chunk_index.chunk_start_offset}"
            )
        entry = found
        if entry.chunk_length != chunk_index.chunk_length:
            raise McapError(
                "publish time index chunk length does not match ChunkIndex for offset "
                f"{chunk_index.chunk_start_offset}"
            )
        if entry.chunk_length < MIN_CHUNK_RECORD_LENGTH:
            raise McapError(
                "publish time index has out-of-range chunk length "
                f"{entry.chunk_length} for offset {entry.chunk_start_offset}"
            )
        if summary_start is not None and (
            entry.chunk_start_offset + entry.chunk_length > summary_start
        ):
            raise McapError(
                "publish time index chunk reference is outside the data section: offset "
                f"{entry.chunk_start_offset}, length {entry.chunk_length}"
            )
        if not entry.channel_publish_times:
            raise McapError(
                "publish time index entry has no channel entries (incomplete index)"
            )
        if chunk_index.message_index_offsets:
            if set(chunk_index.message_index_offsets) != set(
                entry.channel_publish_times
            ):
                raise McapError(
                    "publish time index channel set does not match MessageIndex at offset "
                    f"{chunk_index.chunk_start_offset}"
                )
        result[chunk_index.chunk_start_offset] = entry

    if len(result) != len(entries):
        raise McapError(
            "publish time index references chunks that are not present in the summary"
        )
    return result
