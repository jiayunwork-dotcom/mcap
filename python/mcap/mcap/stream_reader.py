import struct
import zlib
from io import BufferedReader, BytesIO, RawIOBase
from typing import IO, Iterator, List, Optional, Tuple, Union

try:
    import lz4.frame as lz4
except ImportError:
    lz4 = None

try:
    import zstandard
except ImportError:
    zstandard = None

from .data_stream import BoundedReadDataStream, ReadDataStream
from .exceptions import (
    InvalidMagic,
    McapFormatError,
    RecordLengthLimitExceeded,
    UnsupportedCompressionError,
)
from .opcode import Opcode
from .records import (
    Attachment,
    AttachmentIndex,
    Channel,
    Chunk,
    ChunkIndex,
    DataEnd,
    Footer,
    Header,
    McapRecord,
    Message,
    MessageIndex,
    Metadata,
    MetadataIndex,
    PrivateRecord,
    Schema,
    Statistics,
    SummaryOffset,
)

MAGIC_SIZE = 8


class CRCValidationError(ValueError):
    def __init__(self, expected: int, actual: int, record: McapRecord):
        self.expected = expected
        self.actual = actual
        self.record = record

    def __str__(self):
        return (
            f"crc validation failed in {type(self.record).__name__}, "
            f"expected: {self.expected}, calculated: {self.actual}"
        )


def _check_record_size_limit(
    opcode: int, length: int, record_size_limit: Optional[int]
) -> None:
    """Reject a record whose declared body length exceeds the configured limit.

    The check happens before any byte of the record body is requested, so an oversized body is
    never read. ``None`` means no limit is applied.
    """
    if record_size_limit is not None and length > record_size_limit:
        raise RecordLengthLimitExceeded(opcode, length, record_size_limit)


def _parse_record(
    opcode: int,
    length: int,
    stream: BoundedReadDataStream,
    *,
    record_size_limit: Optional[int],
    emit_private_records: bool,
) -> Optional[McapRecord]:
    """Parse one record body from a stream already constrained to that body's declared length.

    ``stream`` is a bounded view covering exactly the record body, so no parser can consume bytes
    belonging to the next record or field. Bytes left over after a known record's fixed fields are
    legal extension bytes and are discarded inside the same boundary.
    """
    record: Optional[McapRecord]
    if opcode == Opcode.ATTACHMENT:
        record = Attachment.read(stream)
    elif opcode == Opcode.ATTACHMENT_INDEX:
        record = AttachmentIndex.read(stream)
    elif opcode == Opcode.CHANNEL:
        record = Channel.read(stream)
    elif opcode == Opcode.CHUNK:
        record = Chunk.read(stream)
    elif opcode == Opcode.CHUNK_INDEX:
        record = ChunkIndex.read(stream)
    elif opcode == Opcode.DATA_END:
        record = DataEnd.read(stream)
    elif opcode == Opcode.FOOTER:
        record = Footer.read(stream)
    elif opcode == Opcode.HEADER:
        record = Header.read(stream)
    elif opcode == Opcode.MESSAGE:
        record = Message.read(stream, length)
    elif opcode == Opcode.MESSAGE_INDEX:
        record = MessageIndex.read(stream)
    elif opcode == Opcode.METADATA:
        record = Metadata.read(stream)
    elif opcode == Opcode.METADATA_INDEX:
        record = MetadataIndex.read(stream)
    elif opcode == Opcode.SCHEMA:
        record = Schema.read(stream)
    elif opcode == Opcode.STATISTICS:
        record = Statistics.read(stream)
    elif opcode == Opcode.SUMMARY_OFFSET:
        record = SummaryOffset.read(stream)
    elif emit_private_records and 0x80 <= opcode <= 0xFF:
        record = PrivateRecord.read(stream, opcode, length)
    else:
        # Unknown record type (including skipped private records such as the publish-time index).
        # Its outer length was already validated; discard the body without retaining it.
        stream.discard(length)
        return None

    # Known records may carry legal extension bytes after their documented fields; skip anything
    # left within this record's own boundary, never touching the next record.
    stream.discard(stream.remaining)
    return record


def breakup_chunk(
    chunk: Chunk,
    validate_crc: bool = False,
    record_size_limit: Optional[int] = 4 * 2**30,
) -> List[McapRecord]:
    """Parse the records contained inside a (decompressed) chunk.

    Every inner record is read inside a bounded view over its own declared body length and checked
    against ``record_size_limit``, the same constraint applied to top-level records. The length of
    the whole decompressed chunk is never treated as the length of a single record.
    """
    stream, stream_length = get_chunk_data_stream(chunk, validate_crc=validate_crc)
    records: List[McapRecord] = []
    while stream.count < stream_length:
        opcode = stream.read1()
        length = stream.read8()
        _check_record_size_limit(opcode, length, record_size_limit)
        record_stream = stream.bounded(length)
        record = _parse_record(
            opcode,
            length,
            record_stream,
            record_size_limit=record_size_limit,
            emit_private_records=False,
        )
        if isinstance(record, (Channel, Message, Schema)):
            records.append(record)
        # Unknown or unexpected inner record types are skipped inside their bounded view.
        record_stream.discard(record_stream.remaining)

    return records


def get_chunk_data_stream(
    chunk: Chunk, validate_crc: bool = False
) -> Tuple[ReadDataStream, int]:
    if chunk.compression == "zstd":
        if zstandard is None:
            raise UnsupportedCompressionError("zstandard")
        data: bytes = zstandard.decompress(chunk.data, chunk.uncompressed_size)
    elif chunk.compression == "lz4":
        if lz4 is None:
            raise UnsupportedCompressionError("lz4")
        data: bytes = lz4.decompress(chunk.data)  # type: ignore
    else:
        data = chunk.data

    if validate_crc and chunk.uncompressed_crc != 0:
        calculated_crc = zlib.crc32(data)
        if calculated_crc != chunk.uncompressed_crc:
            raise CRCValidationError(
                expected=chunk.uncompressed_crc,
                actual=calculated_crc,
                record=chunk,
            )

    return ReadDataStream(BytesIO(data)), len(data)


def read_magic(stream: ReadDataStream) -> bool:
    try:
        magic = struct.unpack("<8B", stream.read(MAGIC_SIZE))
    except struct.error as exc:
        raise McapFormatError(f"failed to decode the MCAP magic: {exc}") from exc
    if magic != (137, 77, 67, 65, 80, 48, 13, 10):
        raise InvalidMagic(magic)
    return True


class StreamReader:
    """
    Reads MCAP data sequentially from an input stream.

    :param input: a file-like object for reading the source data from.
    :param skip_magic: if ``True``, will not expect MCAP magic at start or end of stream.
    :param emit_chunks: if ``True``, will return Chunk records directly and do not parse out the
        records inside.
    :param validate_crcs: if ``True``, will validate chunk and data section CRC values.
    :param record_size_limit: An upper bound to the size of MCAP records that this reader will
        attempt to load in bytes, defaulting to 4 GiB. If this reader encounters a record with a
        greater length, it will throw an :py:class:`~mcap.exceptions.RecordLengthLimitExceeded`
        error.  Setting to ``None`` removes the limit, but can allow corrupted MCAP files to trigger
        a `MemoryError` exception.
    :param emit_private_records: if ``True``, private records (opcodes 0x80-0xFF) are returned as
        :py:class:`~mcap.records.PrivateRecord` instances rather than skipped.
    """

    def __init__(
        self,
        input: Union[str, BytesIO, RawIOBase, BufferedReader, IO[bytes]],
        skip_magic: bool = False,
        emit_chunks: bool = False,
        validate_crcs: bool = False,
        record_size_limit: Optional[int] = (4 * 2**30),  # 4 Gib
        emit_private_records: bool = False,
    ):
        """
        input: The input stream from which to read records.
        """
        if isinstance(input, str):
            self._stream = ReadDataStream(
                open(input, "rb"), calculate_crc=validate_crcs
            )
        elif isinstance(input, RawIOBase):
            self._stream = ReadDataStream(
                BufferedReader(input), calculate_crc=validate_crcs
            )
        else:
            self._stream = ReadDataStream(input, calculate_crc=validate_crcs)
        self._footer: Optional[Footer] = None
        self._skip_magic: bool = skip_magic
        self._emit_chunks: bool = emit_chunks
        self._validate_crcs: bool = validate_crcs
        self._calculated_data_section_crc = None
        self._record_size_limit = record_size_limit
        self._emit_private_records = emit_private_records

    @property
    def records(self) -> Iterator[McapRecord]:
        """
        Returns records encountered in the MCAP in order.
        """
        if not self._skip_magic:
            read_magic(self._stream)

        checksum_before_read: int = 0

        while self._footer is None:
            # Can't validate the data_end crc if we skip magic.
            if self._validate_crcs and not self._skip_magic:
                checksum_before_read = self._stream.checksum()
            opcode = self._stream.read1()
            length = self._stream.read8()
            # Enforce the size limit before requesting any of the record body.
            _check_record_size_limit(opcode, length, self._record_size_limit)
            count = self._stream.count
            # The parser only ever sees a view over this record's declared body.
            record_stream = self._stream.bounded(length)
            record = _parse_record(
                opcode,
                length,
                record_stream,
                record_size_limit=self._record_size_limit,
                emit_private_records=self._emit_private_records,
            )
            # Anything the parser did not consume must still belong to this record (legal
            # extension bytes) and is skipped inside its boundary.
            record_stream.discard(record_stream.remaining)
            consumed = self._stream.count - count
            if consumed != length:
                raise McapFormatError(
                    f"record with opcode {opcode} declared {length} body bytes but "
                    f"{consumed} were consumed"
                )
            if (
                self._validate_crcs
                and not self._skip_magic
                and isinstance(record, DataEnd)
                and record.data_section_crc != 0
                and record.data_section_crc != checksum_before_read
            ):
                raise CRCValidationError(
                    expected=record.data_section_crc,
                    actual=checksum_before_read,
                    record=record,
                )
            if isinstance(record, Chunk) and not self._emit_chunks:
                chunk_records = breakup_chunk(
                    record,
                    validate_crc=self._validate_crcs,
                    record_size_limit=self._record_size_limit,
                )
                for chunk_record in chunk_records:
                    yield chunk_record
            elif record:
                yield record
            if isinstance(record, Footer):
                self._footer = record
                read_magic(self._stream)


__all__ = ["StreamReader"]
