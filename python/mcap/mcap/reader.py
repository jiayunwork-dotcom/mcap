"""High-level classes for reading content out of MCAP data sources."""

import io
import numbers
from abc import ABC, abstractmethod
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

from ._message_queue import ChunkCandidate, make_message_queue
from .data_stream import ReadDataStream, RecordBuilder
from .decoder import DecoderFactory
from .exceptions import DecoderNotFoundError, McapError
from .opcode import Opcode
from .publish_time_index import (
    PUBLISH_TIME_INDEX_MAGIC,
    PublishTimeIndexEntry,
    parse_publish_time_index,
    validate_publish_time_index,
)
from .records import (
    Attachment,
    AttachmentIndex,
    Channel,
    Chunk,
    ChunkIndex,
    Footer,
    Header,
    McapRecord,
    Message,
    Metadata,
    MetadataIndex,
    PrivateRecord,
    Schema,
    Statistics,
)
from .stream_reader import MAGIC_SIZE, StreamReader, breakup_chunk, read_magic
from .summary import Summary

TIME_BASES = ("log", "publish")
MAX_TIME = 2**64 - 1


def _validate_time_basis(time_basis: str) -> str:
    if time_basis not in TIME_BASES:
        raise ValueError(
            f"invalid time_basis {time_basis!r}, expected one of {TIME_BASES}"
        )
    return time_basis


def _validate_time_bound(value: Any, name: str) -> Optional[int]:
    if value is None:
        return None
    # bool is a subclass of int, but accepting it here would hide mistakes like start_time=True.
    if isinstance(value, bool) or not isinstance(value, numbers.Integral):
        raise ValueError(f"{name} must be an integer or None, got {value!r}")
    value = int(value)
    if value < 0 or value > MAX_TIME:
        raise ValueError(f"{name} must be in the range [0, {MAX_TIME}], got {value}")
    return value


def _validate_query(
    start_time: Any,
    end_time: Any,
    log_time_order: bool,
    reverse: bool,
    time_basis: str,
) -> Tuple[Optional[int], Optional[int]]:
    """Validate common query arguments before any record is read.

    Raises ValueError for an unknown time basis, a non-integer or out-of-range time boundary, a
    start after the end, or a reverse request without ordered output.
    """
    _validate_time_basis(time_basis)
    start_time = _validate_time_bound(start_time, "start_time")
    end_time = _validate_time_bound(end_time, "end_time")
    if start_time is not None and end_time is not None and start_time > end_time:
        raise ValueError(
            f"start_time ({start_time}) must not be greater than end_time ({end_time})"
        )
    if reverse and not log_time_order:
        raise ValueError("reverse is only valid with log_time_order=True")
    return start_time, end_time


def _message_time(message: Message, time_basis: str) -> int:
    if time_basis == "publish":
        return message.publish_time
    return message.log_time


def _get_record_size(record: McapRecord):
    """utility for counting the number of bytes a given record occupies in an MCAP."""
    rb = RecordBuilder()
    record.write(rb)
    return rb.count


FOOTER_SIZE = _get_record_size(Footer(0, 0, 0))


def _looks_like_publish_time_index(record: PrivateRecord) -> bool:
    """True if a private record starts with the publish-time index magic."""
    return record.opcode == Opcode.PUBLISH_TIME_INDEX and record.data.startswith(
        PUBLISH_TIME_INDEX_MAGIC
    )


def _read_summary_from_stream_reader(stream_reader: StreamReader) -> Optional[Summary]:
    """read summary records from an MCAP stream reader, collecting them into a Summary."""
    summary = Summary()
    for record in stream_reader.records:
        if isinstance(record, Statistics):
            summary.statistics = record
        elif isinstance(record, Schema):
            summary.schemas[record.id] = record
        elif isinstance(record, Channel):
            summary.channels[record.id] = record
        elif isinstance(record, AttachmentIndex):
            summary.attachment_indexes.append(record)
        elif isinstance(record, ChunkIndex):
            summary.chunk_indexes.append(record)
        elif isinstance(record, MetadataIndex):
            summary.metadata_indexes.append(record)
        elif isinstance(record, PrivateRecord):
            if _looks_like_publish_time_index(record):
                summary.publish_time_indexes.append(record.data)
        elif isinstance(record, Footer):
            # There is no summary!
            if record.summary_start == 0:
                return None
            else:
                return summary
    return summary


def _topic_set(topics: Optional[Iterable[str]]) -> Optional[set]:
    """Materialise ``topics`` once, so a generator argument is not consumed per chunk."""
    return None if topics is None else set(topics)


def _selected_channel_ids(
    summary: Summary,
    topic_set: Optional[set],
    channel_ids: Iterable[int],
) -> List[int]:
    """channel ids from ``channel_ids`` whose topic is in ``topic_set`` (None selects all)."""
    out: List[int] = []
    for channel_id in channel_ids:
        channel = summary.channels.get(channel_id)
        if channel is None:
            continue
        if topic_set is None or channel.topic in topic_set:
            out.append(channel_id)
    return out


def _channel_ids_in_chunk(
    chunk_index: ChunkIndex,
    envelope: Optional[PublishTimeIndexEntry],
) -> Iterable[int]:
    """Channel ids known to appear in a chunk.

    The publish-time envelope is authoritative about which channels actually contributed
    messages to a chunk, even when the writer did not write MessageIndex records. Otherwise fall
    back to the standard MessageIndex-derived channel set.
    """
    if envelope is not None and envelope.channel_publish_times:
        return envelope.channel_publish_times.keys()
    return chunk_index.message_index_offsets.keys()


def _chunks_matching_topics(
    summary: Summary,
    topics: Optional[Iterable[str]],
    start_time: Optional[float],
    end_time: Optional[float],
) -> List[ChunkIndex]:
    """returns a list of ChunkIndex records that include one or more messages of the given topics.

    :param summary: the summary of this MCAP.
    :param topics: topics to match. If None, all chunk indices in the summary are returned.
    :param start_time: if not None, messages from before this unix timestamp are not included.
    :param end_time: if not None, messages at or after this unix timestamp are not included.
    """
    out: List[ChunkIndex] = []
    topic_set = _topic_set(topics)
    for chunk_index in summary.chunk_indexes:
        if start_time is not None and chunk_index.message_end_time < start_time:
            continue
        if end_time is not None and chunk_index.message_start_time >= end_time:
            continue
        for channel_id in chunk_index.message_index_offsets.keys():
            if topic_set is None or summary.channels[channel_id].topic in topic_set:
                out.append(chunk_index)
                break
    return out


def _channels_resolvable(
    summary: Summary,
    publish_index: Dict[int, PublishTimeIndexEntry],
) -> bool:
    """True if every channel referenced by the publish-time index is in the summary."""
    known = set(summary.channels.keys())
    for chunk_index in summary.chunk_indexes:
        envelope = publish_index.get(chunk_index.chunk_start_offset)
        channel_ids = set(_channel_ids_in_chunk(chunk_index, envelope))
        channel_ids.update(chunk_index.message_index_offsets.keys())
        if not channel_ids.issubset(known):
            return False
    return True


def _chunks_matching_publish_times(
    summary: Summary,
    topics: Optional[Iterable[str]],
    start_time: Optional[int],
    end_time: Optional[int],
    publish_index: Dict[int, PublishTimeIndexEntry],
) -> List[ChunkCandidate]:
    """Select chunks for a publish-time query using the per-chunk, per-channel envelope.

    A chunk is selected when at least one selected channel's publish-time envelope intersects the
    half-open query interval ``[start_time, end_time)``.  An empty interval selects nothing.
    """
    if start_time is not None and end_time is not None and start_time == end_time:
        return []
    out: List[ChunkCandidate] = []
    topic_set = _topic_set(topics)
    for chunk_index in summary.chunk_indexes:
        envelope = publish_index.get(chunk_index.chunk_start_offset)
        if envelope is None:
            continue
        channel_ids = list(_channel_ids_in_chunk(chunk_index, envelope))
        if topic_set is not None:
            channel_ids = _selected_channel_ids(summary, topic_set, channel_ids)
        if not channel_ids:
            continue
        min_time: Optional[int] = None
        max_time: Optional[int] = None
        for channel_id in channel_ids:
            channel_times = envelope.channel_publish_times.get(channel_id)
            if channel_times is None:
                continue
            channel_min, channel_max = channel_times
            if start_time is not None and channel_max < start_time:
                continue
            if end_time is not None and channel_min >= end_time:
                continue
            min_time = channel_min if min_time is None else min(min_time, channel_min)
            max_time = channel_max if max_time is None else max(max_time, channel_max)
        if min_time is None or max_time is None:
            continue
        out.append(
            ChunkCandidate(
                chunk_index=chunk_index, min_time=min_time, max_time=max_time
            )
        )
    return out


class DecodedMessageTuple(NamedTuple):
    """Yielded from every iteration of :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`."""

    schema: Optional[Schema]
    channel: Channel
    message: Message
    decoded_message: Any


class McapReader(ABC):
    """Reads data out of an MCAP file, using the summary section where available to efficiently
    read only the parts of the file that are needed.

    :param decoder_factories: An iterable of :py:class:`~mcap.decoder.DecoderFactory`
        instances which can provide decoding functionality to
        :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`.
    """

    def __init__(
        self,
        decoder_factories: Iterable[DecoderFactory] = (),
    ):
        self._decoder_factories = decoder_factories
        self._decoders: dict[int, Callable[[bytes], Any]] = {}

    @abstractmethod
    def iter_messages(
        self,
        topics: Union[Iterable[str], str, None] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
        *,
        time_basis: str = "log",
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        """iterates through the messages in an MCAP.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped before this timestamp are not included.
        :param end_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped at or after this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending order of the selected
            ``time_basis``. If False, messages will be yielded in the order they appear in the
            MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending order of the selected ``time_basis``.
        :param time_basis: which message clock drives ``start_time``, ``end_time`` and ordered
            output. ``"log"`` (the default) uses ``Message.log_time``; ``"publish"`` uses
            ``Message.publish_time``. The two clocks may be in any relative order. Ties are broken
            by physical file order.
        """
        raise NotImplementedError()

    def iter_decoded_messages(
        self,
        topics: Union[Iterable[str], str, None] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
        *,
        time_basis: str = "log",
    ) -> Iterator[DecodedMessageTuple]:
        """iterates through messages in an MCAP, decoding their contents.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped before this timestamp are not included.
        :param end_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped at or after this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending order of the selected
            ``time_basis``. If False, messages will be yielded in the order they appear in the
            MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending order of the selected ``time_basis``.
        :param time_basis: which message clock drives ``start_time``, ``end_time`` and ordered
            output. ``"log"`` (the default) uses ``Message.log_time``; ``"publish"`` uses
            ``Message.publish_time``. Decoding only happens for messages that pass filtering and
            ordering.
        """
        # Validate eagerly so a bad query raises before the caller starts consuming messages.
        _validate_query(start_time, end_time, log_time_order, reverse, time_basis)
        message_iterator = self.iter_messages(
            topics,
            start_time,
            end_time,
            log_time_order,
            reverse,
            time_basis=time_basis,
        )
        return self._decode_messages(message_iterator)

    def _decode_messages(
        self,
        message_iterator: Iterator[Tuple[Optional[Schema], Channel, Message]],
    ) -> Iterator["DecodedMessageTuple"]:

        def decoded_message(
            schema: Optional[Schema], channel: Channel, message: Message
        ) -> Any:
            decoder = self._decoders.get(message.channel_id)
            if decoder is not None:
                return decoder(message.data)
            for factory in self._decoder_factories:
                decoder = factory.decoder_for(channel.message_encoding, schema)
                if decoder is not None:
                    self._decoders[message.channel_id] = decoder
                    return decoder(message.data)

            raise DecoderNotFoundError(
                f"no decoder factory supplied for message encoding {channel.message_encoding}, "
                f"schema {schema}"
            )

        for schema, channel, message in message_iterator:
            yield DecodedMessageTuple(
                schema, channel, message, decoded_message(schema, channel, message)
            )

    @abstractmethod
    def get_header(self) -> Header:
        """Reads the Header records from the beginning of the MCAP file."""
        raise NotImplementedError()

    @abstractmethod
    def get_summary(self) -> Optional[Summary]:
        """Reads the (optional) summary section from the MCAP file."""
        raise NotImplementedError()

    @abstractmethod
    def iter_attachments(self) -> Iterator[Attachment]:
        """Iterates through attachment records in the MCAP."""
        raise NotImplementedError()

    @abstractmethod
    def iter_metadata(self) -> Iterator[Metadata]:
        """Iterates through metadata records in the MCAP."""
        raise NotImplementedError()


def make_reader(
    stream: IO[bytes],
    validate_crcs: bool = False,
    decoder_factories: Iterable[DecoderFactory] = (),
) -> McapReader:
    """constructs the appropriate McapReader implementation for this data source."""
    if stream.seekable():
        return SeekingReader(
            stream, validate_crcs=validate_crcs, decoder_factories=decoder_factories
        )
    return NonSeekingReader(
        stream, validate_crcs=validate_crcs, decoder_factories=decoder_factories
    )


class SeekingReader(McapReader):
    """an McapReader for reading out of seekable data sources.

    :param stream: a file-like object for reading the source data from.
    :param validate_crcs: if ``True``, will validate Chunk CRCs for any chunks read. This class
        does not validate the data section CRC in the DataEnd record because it is designed not to
        read the entire data section when reading messages. To read messages while validating the
        data section CRC, use :py:class:`NonSeekingReader`.
    :param decoder_factories: An iterable of :py:class:`~mcap.decoder.DecoderFactory`
        instances which can provide decoding functionality to
        :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`.
    :param record_size_limit: An upper bound to the size of MCAP records that this reader will
        attempt to load in bytes, defaulting to 4 GiB. If this reader encounters a record with a
        greater length, it will throw an :py:class:`~mcap.exceptions.RecordLengthLimitExceeded`
        error.  Setting to ``None`` removes the limit, but can allow corrupted MCAP files to trigger
        a `MemoryError` exception.
    """

    def __init__(
        self,
        stream: IO[bytes],
        validate_crcs: bool = False,
        decoder_factories: Iterable[DecoderFactory] = (),
        record_size_limit: Optional[int] = 4 * 2**30,
    ):
        super().__init__(decoder_factories=decoder_factories)
        read_magic(ReadDataStream(stream, calculate_crc=False))
        self._stream = stream
        self._validate_crcs = validate_crcs
        self._summary: Optional[Summary] = None
        self._summary_start: Optional[int] = None
        self._record_size_limit = record_size_limit

    def iter_messages(
        self,
        topics: Union[Iterable[str], str, None] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
        *,
        time_basis: str = "log",
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        """iterates through the messages in an MCAP.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped before this timestamp are not included.
        :param end_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped at or after this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending order of the selected
            ``time_basis``. If False, messages will be yielded in the order they appear in the
            MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending order of the selected ``time_basis``.
        :param time_basis: which message clock drives ``start_time``, ``end_time`` and ordered
            output: ``"log"`` (the default) or ``"publish"``. When ``"publish"`` is selected and
            the file carries the private publish-time index, chunks whose selected channels cannot
            contain a matching publish time are not read at all.
        """
        # Validate the query eagerly, so a bad call raises before the caller starts consuming the
        # iterator. Parsing the publish-time index also happens here, so a malformed extension is
        # reported before any message is handed out.
        start_time, end_time = _validate_query(
            start_time, end_time, log_time_order, reverse, time_basis
        )
        if isinstance(topics, str):
            topics = [topics]
        elif topics is not None:
            # Materialise once: chunk selection, filtering and the fallback scan each test
            # membership, and a one-shot iterable would otherwise be exhausted after the first.
            topics = list(topics)
        summary = self.get_summary()
        publish_index: Optional[Dict[int, PublishTimeIndexEntry]] = None
        if summary is not None and time_basis == "publish":
            # Parse and validate the extension eagerly, before any message is delivered, even if
            # the summary has no chunk indices.
            publish_index = self._get_publish_time_index(summary)
            if publish_index is not None and not _channels_resolvable(
                summary, publish_index
            ):
                # Channels are not all repeated in the summary, so a chunk's selected topics
                # cannot be resolved without reading it. Fall back to a linear scan for
                # correctness, as for a file without the extension.
                publish_index = None
        return self._iter_messages_impl(
            topics,
            summary,
            publish_index,
            start_time,
            end_time,
            log_time_order,
            reverse,
            time_basis,
        )

    def _iter_messages_impl(
        self,
        topics: Optional[Iterable[str]],
        summary: Optional[Summary],
        publish_index: Optional[Dict[int, PublishTimeIndexEntry]],
        start_time: Optional[int],
        end_time: Optional[int],
        log_time_order: bool,
        reverse: bool,
        time_basis: str,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        if summary is None or len(summary.chunk_indexes) == 0:
            # No chunk indices available, so there is no index to search for messages.
            # use a non-seeking reader to read linearly through the stream.
            yield from self._iter_messages_linearly(
                topics, start_time, end_time, log_time_order, reverse, time_basis
            )
            return

        if time_basis == "publish":
            if publish_index is None:
                # The file has no usable publish-time index: a chunk's standard time envelope says
                # nothing about publish times, so scan linearly. Ordered output collects the
                # matches and sorts them; this matches the non-seeking reader's memory contract.
                yield from self._iter_messages_linearly(
                    topics, start_time, end_time, log_time_order, reverse, time_basis
                )
                return
            candidates = _chunks_matching_publish_times(
                summary, topics, start_time, end_time, publish_index
            )
            yield from self._iter_indexed_messages(
                summary,
                topics,
                start_time,
                end_time,
                log_time_order,
                reverse,
                time_basis,
                candidates,
            )
            return

        message_queue = make_message_queue(
            log_time_order=log_time_order, reverse=reverse, time_basis=time_basis
        )
        for chunk_index in _chunks_matching_topics(
            summary, topics, start_time, end_time
        ):
            message_queue.push(chunk_index)
        yield from self._drain_message_queue(
            summary,
            topics,
            start_time,
            end_time,
            time_basis,
            message_queue,
        )

    def _iter_messages_linearly(
        self,
        topics: Optional[Iterable[str]],
        start_time: Optional[int],
        end_time: Optional[int],
        log_time_order: bool,
        reverse: bool,
        time_basis: str,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        """Scan the whole file with a non-seeking reader (used when no index can locate data)."""
        self._stream.seek(0, io.SEEK_SET)
        yield from NonSeekingReader(self._stream).iter_messages(
            topics,
            start_time,
            end_time,
            log_time_order,
            reverse,
            time_basis=time_basis,
        )

    def _get_publish_time_index(
        self, summary: Summary
    ) -> Optional[Dict[int, PublishTimeIndexEntry]]:
        """Parse and validate the private publish-time index carried by ``summary``.

        Returns ``None`` if the file does not carry the extension. Raises
        :py:class:`~mcap.exceptions.McapError` if the extension is present but malformed, before
        any message is delivered.
        """
        raw_indexes = summary.publish_time_indexes
        if not raw_indexes:
            return None
        if len(raw_indexes) > 1:
            raise McapError("file contains more than one publish time index record")
        index = parse_publish_time_index(raw_indexes[0])
        return validate_publish_time_index(
            index, summary.chunk_indexes, summary_start=self._summary_start
        )

    def _iter_indexed_messages(
        self,
        summary: Summary,
        topics: Optional[Iterable[str]],
        start_time: Optional[int],
        end_time: Optional[int],
        log_time_order: bool,
        reverse: bool,
        time_basis: str,
        candidates: List[ChunkCandidate],
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        message_queue = make_message_queue(
            log_time_order=log_time_order, reverse=reverse, time_basis=time_basis
        )
        for candidate in candidates:
            message_queue.push(candidate)
        yield from self._drain_message_queue(
            summary,
            topics,
            start_time,
            end_time,
            time_basis,
            message_queue,
        )

    def _drain_message_queue(
        self,
        summary: Summary,
        topics: Optional[Iterable[str]],
        start_time: Optional[int],
        end_time: Optional[int],
        time_basis: str,
        message_queue: Any,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        topic_set = None if topics is None else set(topics)
        while message_queue:
            next_item = message_queue.pop()
            if isinstance(next_item, (ChunkIndex, ChunkCandidate)):
                chunk_index = (
                    next_item.chunk_index
                    if isinstance(next_item, ChunkCandidate)
                    else next_item
                )
                self._stream.seek(chunk_index.chunk_start_offset + 1 + 8, io.SEEK_SET)
                chunk = Chunk.read(ReadDataStream(self._stream))
                for index, record in enumerate(
                    breakup_chunk(chunk, validate_crc=self._validate_crcs)
                ):
                    if not isinstance(record, Message):
                        continue
                    channel = summary.channels[record.channel_id]
                    if topic_set is not None and channel.topic not in topic_set:
                        continue
                    message_time = _message_time(record, time_basis)
                    if start_time is not None and message_time < start_time:
                        continue
                    if end_time is not None and message_time >= end_time:
                        continue
                    if channel.schema_id == 0:
                        schema = None
                    else:
                        schema = summary.schemas[channel.schema_id]
                    message_queue.push(
                        (
                            (schema, channel, record),
                            chunk_index.chunk_start_offset,
                            index,
                        )
                    )
            else:
                yield next_item[0]

    def get_header(self) -> Header:
        """Reads the Header record from the beginning of the MCAP file."""
        self._stream.seek(0)
        header = next(
            StreamReader(
                self._stream,
                skip_magic=False,
                record_size_limit=self._record_size_limit,
            ).records
        )
        if not isinstance(header, Header):
            raise McapError(
                f"expected header at beginning of MCAP file, found {type(header)}"
            )
        return header

    def get_summary(self) -> Optional[Summary]:
        """Reads the (optional) summary section from the MCAP file."""
        if self._summary is not None:
            return self._summary
        self._stream.seek(-(FOOTER_SIZE + MAGIC_SIZE), io.SEEK_END)
        footer = next(
            StreamReader(
                self._stream,
                skip_magic=True,
                record_size_limit=self._record_size_limit,
            ).records
        )
        if not isinstance(footer, Footer):
            raise McapError(
                f"expected footer at end of MCAP file, found {type(footer)}"
            )
        self._summary_start = footer.summary_start
        if footer.summary_start == 0:
            return None
        self._stream.seek(footer.summary_start, io.SEEK_SET)
        self._summary = _read_summary_from_stream_reader(
            StreamReader(
                self._stream,
                skip_magic=True,
                record_size_limit=self._record_size_limit,
                emit_private_records=True,
            )
        )
        return self._summary

    def iter_attachments(self) -> Iterator[Attachment]:
        """Iterates through attachment records in the MCAP."""
        summary = self.get_summary()
        if summary is None:
            # no index available, use a non-seeking reader to read linearly through the stream.
            self._stream.seek(0, io.SEEK_SET)
            yield from NonSeekingReader(self._stream).iter_attachments()
            return
        for attachment_index in summary.attachment_indexes:
            self._stream.seek(attachment_index.offset)
            record = next(
                StreamReader(
                    self._stream,
                    skip_magic=True,
                    record_size_limit=self._record_size_limit,
                ).records
            )
            if isinstance(record, Attachment):
                yield record
            else:
                raise McapError(f"expected attachment record, got {type(record)}")

    def iter_metadata(self) -> Iterator[Metadata]:
        """Iterates through metadata records in the MCAP."""
        summary = self.get_summary()
        if summary is None:
            # fall back to a non-seeking reader
            self._stream.seek(0, io.SEEK_SET)
            yield from NonSeekingReader(self._stream).iter_metadata()
            return
        for metadata_index in summary.metadata_indexes:
            self._stream.seek(metadata_index.offset)
            record = next(
                StreamReader(
                    self._stream,
                    skip_magic=True,
                    record_size_limit=self._record_size_limit,
                ).records
            )
            if isinstance(record, Metadata):
                yield record
            else:
                raise McapError(f"expected attachment record, got {type(record)}")


class NonSeekingReader(McapReader):
    """an McapReader for reading out of non-seekable data sources, such as a pipe or socket.

    :param stream: a file-like object for reading the source data from.
    :param validate_crcs: if ``True``, will validate chunk and data section CRC values.
    :param decoder_factories: An iterable of :py:class:`~mcap.decoder.DecoderFactory`
        instances which can provide decoding functionality to
        :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`.
    :param record_size_limit: An upper bound to the size of MCAP records that this reader will
        attempt to load in bytes, defaulting to 4 GiB. If this reader encounters a record with a
        greater length, it will throw an :py:class:`~mcap.exceptions.RecordLengthLimitExceeded`
        error.  Setting to ``None`` removes the limit, but can allow corrupted MCAP files to trigger
        a `MemoryError` exception.
    """

    def __init__(
        self,
        stream: IO[bytes],
        validate_crcs: bool = False,
        decoder_factories: Iterable[DecoderFactory] = (),
        record_size_limit: Optional[int] = 4 * 2**30,
    ):
        super().__init__(decoder_factories=decoder_factories)
        self._stream_reader = StreamReader(
            stream,
            validate_crcs=validate_crcs,
            record_size_limit=record_size_limit,
        )
        self._schemas: Dict[int, Schema] = {}
        self._channels: Dict[int, Channel] = {}
        self._spent: bool = False

    def _check_spent(self):
        if self._spent:
            raise RuntimeError(
                "cannot use more than one query against a non-seeking data source"
            )
        self._spent = True

    def get_header(self) -> Header:
        """Reads the Header record from the beginning of the MCAP file."""
        self._check_spent()
        header = next(self._stream_reader.records)
        if not isinstance(header, Header):
            raise McapError(
                f"expected header at beginning of MCAP file, found {type(header)}"
            )
        return header

    def iter_messages(
        self,
        topics: Union[Iterable[str], str, None] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
        *,
        time_basis: str = "log",
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        """Iterates through the messages in an MCAP.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped before this timestamp are not included.
        :param end_time: an integer nanosecond timestamp in the selected ``time_basis``. If
            provided, messages timestamped at or after this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending order of the selected
            ``time_basis``. If False, messages will be yielded in the order they appear in the
            MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending order of the selected ``time_basis``.
        :param time_basis: which message clock drives ``start_time``, ``end_time`` and ordered
            output: ``"log"`` (the default) or ``"publish"``. A non-seekable stream cannot use a
            publish-time index, so ``"publish"`` always scans linearly here.

        .. warning::
            setting log_time_order to True on a non-seekable stream will cause the entire content
            of the MCAP to be loaded into memory.
        """
        start_time, end_time = _validate_query(
            start_time, end_time, log_time_order, reverse, time_basis
        )
        if isinstance(topics, str):
            topics = [topics]
        elif topics is not None:
            # Materialise once; the physical scan tests membership per message.
            topics = list(topics)
        return self._iter_messages_sorted(
            topics, start_time, end_time, log_time_order, reverse, time_basis
        )

    def _iter_messages_sorted(
        self,
        topics: Optional[Iterable[str]],
        start_time: Optional[int],
        end_time: Optional[int],
        log_time_order: bool,
        reverse: bool,
        time_basis: str,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        if not log_time_order:
            yield from self._iter_messages_internal(
                topics, start_time, end_time, time_basis
            )
            return
        # Sort stably by the selected clock, so equal timestamps keep physical file order. A
        # reverse read is then the exact reverse of that forward sequence.
        ordered = sorted(
            self._iter_messages_internal(topics, start_time, end_time, time_basis),
            key=lambda tup: _message_time(tup[2], time_basis),
        )
        if reverse:
            yield from reversed(ordered)
        else:
            yield from ordered

    def _iter_messages_internal(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        time_basis: str = "log",
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        self._check_spent()
        for record in self._stream_reader.records:
            if isinstance(record, Schema):
                self._schemas[record.id] = record
            if isinstance(record, Channel):
                if record.schema_id != 0 and record.schema_id not in self._schemas:
                    raise McapError(
                        f"no schema record found with id {record.schema_id}"
                    )
                self._channels[record.id] = record
            if isinstance(record, Message):
                if record.channel_id not in self._channels:
                    raise McapError(
                        f"no channel record found with id {record.channel_id}"
                    )
                channel = self._channels[record.channel_id]
                if topics is not None and channel.topic not in topics:
                    continue
                message_time = _message_time(record, time_basis)
                if start_time is not None and message_time < start_time:
                    continue
                if end_time is not None and message_time >= end_time:
                    continue
                if channel.schema_id == 0:
                    schema = None
                else:
                    schema = self._schemas[channel.schema_id]
                yield (schema, channel, record)

    def get_summary(self) -> Optional[Summary]:
        """Returns a Summary object containing records from the (optional) summary section."""
        self._check_spent()
        return _read_summary_from_stream_reader(self._stream_reader)

    def iter_attachments(self) -> Iterator[Attachment]:
        """Iterates through attachment records in the MCAP."""
        self._check_spent()
        for record in self._stream_reader.records:
            if isinstance(record, Attachment):
                yield record

    def iter_metadata(self) -> Iterator[Metadata]:
        """Iterates through metadata records in the MCAP."""
        self._check_spent()
        for record in self._stream_reader.records:
            if isinstance(record, Metadata):
                yield record
