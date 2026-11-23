from typing import Any

from mcap.opcode import Opcode


class McapError(Exception):
    pass


class McapFormatError(McapError):
    """Raised when an MCAP's declared structure is inconsistent with its bytes.

    Examples include a field that does not fit inside its record's declared length, a map entry
    that crosses its declared map range, an invalid UTF-8 string, or a length that cannot be
    represented. This describes corruption of the format rather than the physical input ending
    early (which raises :class:`EndOfFile`).
    """


class InvalidMagic(McapError):
    def __init__(self, bad_magic: Any):
        super().__init__(f"not a valid MCAP file, invalid magic: {bad_magic}")


class DecoderNotFoundError(McapError):
    pass


class EndOfFile(McapError):
    pass


class RecordLengthLimitExceeded(McapError):
    def __init__(self, opcode: int, length: int, limit: int):
        opcode_name = f"unknown (opcode {opcode})"
        try:
            opcode_name = Opcode(opcode).name
        except ValueError:
            # unknown opcode will trigger a ValueError
            pass
        super().__init__(
            f"{opcode_name} record has length {length} that exceeds limit {limit}",
        )


class UnsupportedCompressionError(McapError):
    def __init__(self, compression: str):
        super().__init__(f"unsupported compression type {compression}")
