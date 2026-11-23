"""MCAP archives built byte-by-byte without the production writer.

Every record is emitted with the explicit little-endian field layouts from the MCAP specification,
and the expected parsed objects are stated independently. These archives are the source of truth
that does not depend on :class:`mcap.writer.Writer` round-tripping.
"""

import zlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import lz4.frame
import zstandard

from ._io_helpers import MAGIC, pstr, record, u16, u32, u64

OP_HEADER = 0x01
OP_FOOTER = 0x02
OP_SCHEMA = 0x03
OP_CHANNEL = 0x04
OP_MESSAGE = 0x05
OP_CHUNK = 0x06
OP_DATA_END = 0x0F
OP_ATTACHMENT = 0x09
OP_METADATA = 0x0C


@dataclass
class Expected:
    header_profile: str
    header_library: str
    schema_id: int
    schema_name: str
    schema_encoding: str
    schema_data: bytes
    channel_id: int
    channel_topic: str
    channel_encoding: str
    channel_metadata: Dict[str, str]
    messages: List[Tuple[int, int, bytes]] = field(default_factory=list)
    attachment: Optional[Tuple[str, str, bytes]] = None
    metadata: Optional[Tuple[str, Dict[str, str]]] = None


def _map_bytes(entries: Dict[str, str]) -> bytes:
    body = b"".join(pstr(k) + pstr(v) for k, v in entries.items())
    return u32(len(body)) + body


def _schema_body(schema_id: int, name: str, encoding: str, data: bytes) -> bytes:
    return u16(schema_id) + pstr(name) + pstr(encoding) + u32(len(data)) + data


def _channel_body(
    channel_id: int,
    schema_id: int,
    topic: str,
    encoding: str,
    metadata: Dict[str, str],
) -> bytes:
    return (
        u16(channel_id)
        + u16(schema_id)
        + pstr(topic)
        + pstr(encoding)
        + _map_bytes(metadata)
    )


def _message_body(
    channel_id: int, sequence: int, log_time: int, publish_time: int, data: bytes
) -> bytes:
    return u16(channel_id) + u32(sequence) + u64(log_time) + u64(publish_time) + data


def _attachment_body(
    log_time: int, create_time: int, name: str, media_type: str, data: bytes
) -> bytes:
    without_crc = (
        u64(log_time)
        + u64(create_time)
        + pstr(name)
        + pstr(media_type)
        + u64(len(data))
        + data
    )
    return without_crc + u32(zlib.crc32(without_crc))


def _metadata_body(name: str, entries: Dict[str, str]) -> bytes:
    return pstr(name) + _map_bytes(entries)


def _expected() -> Expected:
    return Expected(
        header_profile="",  # legal empty string
        header_library="hand-builder/1.0",
        schema_id=1,
        schema_name="手動 schema",
        schema_encoding="jsonschema",
        schema_data=b'{"type":"object"}',
        channel_id=1,
        channel_topic="遥测",
        channel_encoding="raw",
        # A multi-byte value and a legal zero-length value.
        channel_metadata={"unit": "米/秒", "note": ""},
        messages=[
            (10, 10, b"abc"),
            (20, 21, "中文载荷".encode("utf-8")),
            (30, 30, b"x" * 5000),
        ],
        attachment=("scene.txt", "text/plain", b"attachment-bytes-" + "附件".encode()),
        metadata=("run", {"status": "ok", "empty": ""}),
    )


def hand_unchunked_mcap() -> Tuple[bytes, Expected]:
    exp = _expected()
    parts = [MAGIC]
    parts.append(record(OP_HEADER, pstr(exp.header_profile) + pstr(exp.header_library)))
    parts.append(
        record(
            OP_SCHEMA,
            _schema_body(
                exp.schema_id, exp.schema_name, exp.schema_encoding, exp.schema_data
            ),
        )
    )
    parts.append(
        record(
            OP_CHANNEL,
            _channel_body(
                exp.channel_id,
                exp.schema_id,
                exp.channel_topic,
                exp.channel_encoding,
                exp.channel_metadata,
            ),
        )
    )
    for sequence, (log_time, publish_time, data) in enumerate(exp.messages):
        parts.append(
            record(
                OP_MESSAGE,
                _message_body(
                    exp.channel_id, sequence, log_time, publish_time, data
                ),
            )
        )
    assert exp.attachment is not None
    name, media, data = exp.attachment
    parts.append(record(OP_ATTACHMENT, _attachment_body(15, 15, name, media, data)))
    assert exp.metadata is not None
    meta_name, entries = exp.metadata
    parts.append(record(OP_METADATA, _metadata_body(meta_name, entries)))
    parts.append(record(OP_DATA_END, u32(0)))
    parts.append(record(OP_FOOTER, u64(0) + u64(0) + u32(0)))
    parts.append(MAGIC)
    return b"".join(parts), exp


def _compress(compression: str, data: bytes) -> bytes:
    if compression == "":
        return data
    if compression == "lz4":
        return lz4.frame.compress(data)
    if compression == "zstd":
        return zstandard.compress(data)
    raise ValueError(compression)


def hand_chunked_mcap(compression: str) -> Tuple[bytes, Expected]:
    exp = _expected()
    inner_parts = []
    inner_parts.append(
        record(
            OP_SCHEMA,
            _schema_body(
                exp.schema_id, exp.schema_name, exp.schema_encoding, exp.schema_data
            ),
        )
    )
    inner_parts.append(
        record(
            OP_CHANNEL,
            _channel_body(
                exp.channel_id,
                exp.schema_id,
                exp.channel_topic,
                exp.channel_encoding,
                exp.channel_metadata,
            ),
        )
    )
    for sequence, (log_time, publish_time, data) in enumerate(exp.messages):
        inner_parts.append(
            record(
                OP_MESSAGE,
                _message_body(
                    exp.channel_id, sequence, log_time, publish_time, data
                ),
            )
        )
    inner = b"".join(inner_parts)
    compressed = _compress(compression, inner)
    chunk_body = (
        u64(10)
        + u64(30)
        + u64(len(inner))
        + u32(zlib.crc32(inner))
        + pstr(compression)
        + u64(len(compressed))
        + compressed
    )
    # A chunked file without a summary still exercises inner-record boundary parsing.
    parts = [MAGIC]
    parts.append(record(OP_HEADER, pstr(exp.header_profile) + pstr(exp.header_library)))
    parts.append(record(OP_CHUNK, chunk_body))
    parts.append(record(OP_DATA_END, u32(0)))
    parts.append(record(OP_FOOTER, u64(0) + u64(0) + u32(0)))
    parts.append(MAGIC)
    return b"".join(parts), exp
