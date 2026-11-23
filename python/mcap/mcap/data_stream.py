import struct
import zlib
from io import BytesIO
from typing import IO, Optional, Union

from .exceptions import EndOfFile, McapFormatError
from .opcode import Opcode

#: Upper bound on the number of bytes requested from the underlying source in a single call while
#: discarding (skipping) bytes that do not need to be retained.
MAX_SKIP_READ = 64 * 1024


class ReadDataStream:
    """Reads structured values from a binary file object.

    The underlying file object is a blocking binary stream: ``read(n)`` may return fewer than ``n``
    non-empty bytes, and only an empty ``b""`` result marks the end of the input. Reads therefore
    loop until the full requested amount is available or the source signals end of file. Exceptions
    raised by the source (for example :class:`OSError`) propagate unchanged.
    """

    def __init__(self, stream: IO[bytes], calculate_crc: bool = False):
        self._count = 0
        self._stream = stream
        self._crc: Optional[int] = None
        if calculate_crc:
            self._crc = 0

    @property
    def count(self) -> int:
        return self._count

    @property
    def crc(self) -> Optional[int]:
        return self._crc

    def read(self, length: int) -> bytes:
        if length == 0:
            return b""
        if length < 0:
            raise McapFormatError(f"cannot read a negative number of bytes ({length})")

        # A blocking binary read may return fewer bytes than requested; keep reading until the full
        # amount is available. Only b"" signals end of file. Source exceptions (e.g. OSError) are
        # deliberately not intercepted.
        remaining = length
        parts = []
        while remaining > 0:
            try:
                data = self._stream.read(remaining)
            except OverflowError as exc:
                # Some file objects raise OverflowError rather than MemoryError when a corrupted
                # length asks for an impossibly large read. Treat that as a bad declared length.
                raise McapFormatError(
                    f"declared length {length} is too large to read: {exc}"
                ) from exc
            if data is None:
                raise McapFormatError(
                    "underlying stream returned None; non-blocking streams are not supported"
                )
            if len(data) == 0:
                raise EndOfFile(
                    f"reached end of input while {remaining} of {length} requested bytes "
                    "were still outstanding"
                )
            parts.append(data)
            got = len(data)
            self._count += got
            remaining -= got
            if self._crc is not None:
                self._crc = zlib.crc32(data, self._crc)
        if len(parts) == 1:
            return parts[0]
        try:
            return b"".join(parts)
        except OverflowError as exc:
            raise McapFormatError(
                f"declared length {length} is too large to assemble: {exc}"
            ) from exc

    def checksum(self) -> int:
        if self._crc is not None:
            return self._crc
        else:
            raise RuntimeError("requested checksum where calculate_crc == false")

    def bounded(self, length: int) -> "BoundedReadDataStream":
        """Return a view limited to the next ``length`` bytes of this stream."""
        if length < 0:
            raise McapFormatError(f"cannot create a bounded view of negative length {length}")
        return BoundedReadDataStream(self, length)

    def discard(self, length: int) -> None:
        """Advance the stream by ``length`` bytes without retaining them.

        Each request to the underlying source is at most :data:`MAX_SKIP_READ` bytes, so the extra
        working memory needed to skip a record does not grow with its body length.
        """
        while length > 0:
            chunk = self.read(min(length, MAX_SKIP_READ))
            length -= len(chunk)

    def read1(self) -> int:
        try:
            [value] = struct.unpack("<B", self.read(1))
        except struct.error as exc:
            raise McapFormatError(f"failed to decode a u8 value: {exc}") from exc
        return value

    def read2(self) -> int:
        try:
            [value] = struct.unpack("<H", self.read(2))
        except struct.error as exc:
            raise McapFormatError(f"failed to decode a u16 value: {exc}") from exc
        return value

    def read4(self) -> int:
        try:
            [value] = struct.unpack("<I", self.read(4))
        except struct.error as exc:
            raise McapFormatError(f"failed to decode a u32 value: {exc}") from exc
        return value

    def read8(self) -> int:
        try:
            [value] = struct.unpack("<Q", self.read(8))
        except (struct.error, OverflowError) as exc:
            raise McapFormatError(f"failed to decode a u64 value: {exc}") from exc
        return value

    def read_prefixed_string(self) -> str:
        length = self.read4()
        try:
            return str(self.read(length), "utf-8")
        except UnicodeDecodeError as exc:
            raise McapFormatError(f"invalid UTF-8 in string: {exc}") from exc


class BoundedReadDataStream(ReadDataStream):
    """A :class:`ReadDataStream` constrained to a fixed byte range of a parent stream.

    Every read is checked against the number of bytes remaining in the range, so a parser cannot
    borrow bytes belonging to the next field, record or chunk. Running out of declared range is a
    structural corruption (:class:`McapFormatError`); running out of physical input first is an
    :class:`EndOfFile`, raised by the parent read. The byte count and CRC stay owned by the parent
    so multi-part reads are never counted twice.
    """

    def __init__(self, parent: ReadDataStream, length: int):
        if length < 0:
            raise McapFormatError(f"bounded stream length must not be negative, got {length}")
        self._parent = parent
        self._remaining = length
        self._length = length

    @property
    def count(self) -> int:
        return self._parent.count

    @property
    def crc(self) -> Optional[int]:
        return self._parent.crc

    @property
    def remaining(self) -> int:
        return self._remaining

    def read(self, length: int) -> bytes:
        if length < 0:
            raise McapFormatError(f"cannot read a negative number of bytes ({length})")
        if length > self._remaining:
            raise McapFormatError(
                f"read of {length} bytes exceeds the {self._length}-byte bounded range "
                f"({self._remaining} bytes remain)"
            )
        data = self._parent.read(length)
        self._remaining -= len(data)
        return data

    def bounded(self, length: int) -> "BoundedReadDataStream":
        if length < 0:
            raise McapFormatError(f"cannot create a bounded view of negative length {length}")
        if length > self._remaining:
            raise McapFormatError(
                f"nested range of {length} bytes exceeds the {self._remaining} remaining bytes "
                "of the enclosing bounded range"
            )
        return BoundedReadDataStream(self, length)

    def discard(self, length: int) -> None:
        if length < 0:
            raise McapFormatError(f"cannot discard a negative number of bytes ({length})")
        if length > self._remaining:
            raise McapFormatError(
                f"discard of {length} bytes exceeds the {self._length}-byte bounded range "
                f"({self._remaining} bytes remain)"
            )
        while length > 0:
            chunk_size = min(length, MAX_SKIP_READ)
            chunk = self._parent.read(chunk_size)
            consumed = len(chunk)
            self._remaining -= consumed
            length -= consumed

    def checksum(self) -> int:
        return self._parent.checksum()


class RecordBuilder:
    def __init__(self) -> None:
        self._buffer = BytesIO()

    @property
    def count(self) -> int:
        return self._buffer.tell()

    def start_record(self, opcode: Union[Opcode, int]):
        self._record_start_offset = self._buffer.tell()
        self._buffer.write(struct.pack("<BQ", opcode, 0))  # placeholder size

    def finish_record(self):
        pos = self._buffer.tell()
        length = pos - self._record_start_offset - 9
        self._buffer.seek(self._record_start_offset + 1)
        self._buffer.write(struct.pack("<Q", length))
        self._buffer.seek(pos)

    def end(self):
        buf = self._buffer.getvalue()
        self._buffer.close()
        self._buffer = BytesIO()
        return buf

    def write(self, data: bytes):
        self._buffer.write(data)

    def write_prefixed_string(self, value: str):
        bytes = value.encode()
        self.write4(len(bytes))
        self.write(bytes)

    def write1(self, value: int):
        self.write(struct.pack("<B", value))

    def write2(self, value: int):
        self.write(struct.pack("<H", value))

    def write4(self, value: int):
        self.write(struct.pack("<I", value))

    def write8(self, value: int):
        self.write(struct.pack("<Q", value))
