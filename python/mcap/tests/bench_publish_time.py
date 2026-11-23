"""Performance experiment for publish-time queries.

Generates synthetic archives with 2,000 and 20,000 chunks in three access distributions and
reports, per query:

* bytes read in explicitly requested read intervals (library-level reads, not OS prefetch),
* number of chunk payloads decompressed,
* number of messages decoded,
* peak number of items in the message priority queue,
* elapsed wall time.

Run with::

    PYTHONPATH=python/mcap python3 -I python/mcap/tests/bench_publish_time.py

``-I`` keeps the script's directory off ``sys.path`` so a planted module in the working directory
cannot shadow imports; ``PYTHONPATH`` still supplies the ``mcap`` package.
"""

import bisect
import io
import statistics
import sys
import time
from typing import Dict, List, Tuple

from mcap import reader as reader_module
from mcap._message_queue import LogTimeOrderQueue
from mcap.decoder import DecoderFactory
from mcap.reader import SeekingReader
from mcap.writer import CompressionType, IndexType, Writer

CHUNKS = [2000, 20000]
MESSAGES_PER_CHUNK = 4
TWO_TOPICS = ["/hot", "/cold"]


class CountingStream(io.IOBase):
    def __init__(self, data: bytes):
        self._buffer = io.BytesIO(data)
        self.bytes_read = 0
        self.intervals: List[Tuple[int, int]] = []

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        return self._buffer.seek(offset, whence)

    def tell(self):
        return self._buffer.tell()

    def read(self, n=-1):
        position = self._buffer.tell()
        data = self._buffer.read(n)
        if n:
            self.bytes_read += len(data)
            self.intervals.append((position, len(data)))
        return data

    def readinto(self, b):
        position = self._buffer.tell()
        n = self._buffer.readinto(b)
        n = n or 0
        self.bytes_read += n
        self.intervals.append((position, n))
        return n


class CountingDecoder(DecoderFactory):
    def __init__(self):
        self.count = 0

    def decoder_for(self, message_encoding, schema):
        if message_encoding != "raw":
            return None

        def decode(data):
            self.count += 1
            return data

        return decode


def build(num_chunks: int, distribution: str) -> bytes:
    """Build a chunked archive with two clocks in opposite order.

    Every chunk holds ``MESSAGES_PER_CHUNK`` messages of two topics. For ``/hot`` the publish
    order is the reverse of the log order; ``/cold`` has disjoint publish times. Chunk-level
    publish envelopes of the two topics therefore never overlap, which lets the experiment prove
    that an unrelated topic in the same chunk does not force decompression.
    """
    out = io.BytesIO()
    writer = Writer(
        out,
        compression=CompressionType.ZSTD,
        chunk_size=200,  # exactly MESSAGES_PER_CHUNK messages per chunk
        index_types=IndexType.ALL,
        publish_time_index=True,
    )
    writer.start("bench", "bench")
    hot = writer.register_channel("/hot", "raw", 0)
    cold = writer.register_channel("/cold", "raw", 0)
    sequence = 0
    for chunk in range(num_chunks):
        for within in range(MESSAGES_PER_CHUNK):
            log_time = chunk * 1000 + within
            if within % 2 == 0:
                channel = hot
                # Reversed relative to log order.
                publish_time = HOT_BASE - log_time
            else:
                channel = cold
                publish_time = COLD_BASE + log_time
            writer.add_message(channel, log_time, b"x" * 32, publish_time, sequence)
            sequence += 1
    writer.finish()
    return out.getvalue()


HOT_BASE = 1_000_000_000
COLD_BASE = 2_000_000_000


def _queue_peak_tracker():
    state = {"peak": 0}
    original_push = LogTimeOrderQueue.push
    original_pop = LogTimeOrderQueue.pop

    def push(self, item):
        original_push(self, item)
        state["peak"] = max(state["peak"], len(self))

    def pop(self):
        item = original_pop(self)
        state["peak"] = max(state["peak"], len(self))
        return item

    LogTimeOrderQueue.push = push
    LogTimeOrderQueue.pop = pop

    def restore():
        LogTimeOrderQueue.push = original_push
        LogTimeOrderQueue.pop = original_pop

    return (lambda: state["peak"]), restore


def _query(distribution: str, num_chunks: int):
    """Return (topics, start_time, end_time) for one access distribution."""
    if distribution == "few_hits":
        # A narrow publish window around one /hot message.
        hot_time = HOT_BASE - 1000
        return ["/hot"], hot_time, hot_time + 1
    if distribution == "same_chunk_other_topic":
        # Ask for /hot, but span every chunk's /cold publish times. No /hot envelope intersects,
        # so no chunk may be decompressed.
        return ["/hot"], COLD_BASE, COLD_BASE + num_chunks * 1000
    if distribution == "all_overlap":
        # Span every /hot publish time, so every chunk's /hot envelope intersects.
        return ["/hot"], 0, 2**64 - 1
    raise ValueError(distribution)


def measure(content: bytes, distribution: str, num_chunks: int) -> Dict[str, float]:
    stream = CountingStream(content)
    reader = SeekingReader(stream)
    summary = reader.get_summary()
    assert summary is not None

    peak, restore = _queue_peak_tracker()
    decompressed = [0]
    original_breakup = reader_module.breakup_chunk

    def tracking_breakup(chunk, validate_crc=False):
        decompressed[0] += 1
        return original_breakup(chunk, validate_crc=validate_crc)

    topics, start, end = _query(distribution, num_chunks)
    reader_module.breakup_chunk = tracking_breakup
    decoder = CountingDecoder()
    try:
        begin = time.perf_counter()
        observed = list(
            reader.iter_messages(
                topics=topics,
                start_time=start,
                end_time=end,
                time_basis="publish",
            )
        )
        elapsed = time.perf_counter() - begin
        decoded_count = 0
        if distribution == "all_overlap":
            # Decoding is checked on the worst-case distribution only.
            decoder_reader = SeekingReader(
                CountingStream(content), decoder_factories=[decoder]
            )
            begin_decode = time.perf_counter()
            decoded = list(
                decoder_reader.iter_decoded_messages(
                    topics=topics,
                    start_time=start,
                    end_time=end,
                    time_basis="publish",
                )
            )
            elapsed += time.perf_counter() - begin_decode
            decoded_count = decoder.count
            assert len(decoded) == len(observed)
    finally:
        reader_module.breakup_chunk = original_breakup
        restore()

    total_bytes = len(content)
    payload_spans = [
        (
            chunk.chunk_start_offset,
            chunk.chunk_start_offset + chunk.chunk_length,
        )
        for chunk in summary.chunk_indexes
    ]

    def payload_bytes(intervals):
        starts = [s for s, _ in payload_spans]
        total = 0
        for position, length in intervals:
            end = position + length
            # Only spans starting before this interval's end can overlap.
            i = bisect.bisect_left(starts, end)
            while i > 0:
                i -= 1
                start_offset, end_offset = payload_spans[i]
                if end_offset <= position:
                    # Spans are sorted by start and non-overlapping, so stop once past.
                    break
                overlap = min(end, end_offset) - max(position, start_offset)
                if overlap > 0:
                    total += overlap
        return total

    result = {
        "chunks": num_chunks,
        "actual_chunks": len(summary.chunk_indexes),
        "distribution": distribution,
        "messages_returned": len(observed),
        "bytes_read": stream.bytes_read,
        "payload_bytes_read": payload_bytes(stream.intervals),
        "total_bytes": total_bytes,
        "total_payload_bytes": sum(e - s for s, e in payload_spans),
        "intervals": len(stream.intervals),
        "chunks_decompressed": decompressed[0],
        "messages_decoded": decoded_count,
        "queue_peak": peak(),
        "elapsed_s": elapsed,
    }
    return result


def main() -> int:
    print(f"python {sys.version.split()[0]}")
    print()
    rows = []
    for num_chunks in CHUNKS:
        for distribution in ("few_hits", "same_chunk_other_topic", "all_overlap"):
            content = build(num_chunks, distribution)
            # Median of three runs to reduce noise; the archive is built once per cell.
            samples = [measure(content, distribution, num_chunks) for _ in range(3)]
            row = dict(samples[0])
            row["elapsed_s"] = statistics.median(s["elapsed_s"] for s in samples)
            row["bytes_read"] = int(statistics.median(s["bytes_read"] for s in samples))
            row["payload_bytes_read"] = int(
                statistics.median(s["payload_bytes_read"] for s in samples)
            )
            rows.append(row)
            print(
                f"chunks={row['actual_chunks']:>6} dist={row['distribution']:<24} "
                f"returned={row['messages_returned']:>6} "
                f"payload={row['payload_bytes_read']:>10}/{row['total_payload_bytes']:<10} "
                f"all_bytes={row['bytes_read']:>10}/{row['total_bytes']:<10} "
                f"reads={row['intervals']:>6} "
                f"decompressed={row['chunks_decompressed']:>6} "
                f"decoded={row['messages_decoded']:>6} "
                f"queue_peak={row['queue_peak']:>6} "
                f"elapsed={row['elapsed_s'] * 1000:8.1f}ms"
            )
    print()
    print("all_overlap is the worst case for decompression and queue occupancy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
