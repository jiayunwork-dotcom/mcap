Publish-time queries and the publish-time index
===============================================

Messages in MCAP carry two timestamps: ``log_time`` (when the recorder saw the message) and
``publish_time`` (when the publisher produced it). They can be in any relative order, and a
message that arrives late can have a much older ``publish_time``. The standard ``ChunkIndex``,
``MessageIndex`` and ``Statistics`` records only describe ``log_time``, so a publish-time query
against them can miss messages.

The Python reader can answer queries keyed on either clock, and the writer can optionally store a
private extension that makes publish-time queries efficient.

Selecting the clock
-------------------

``McapReader.iter_messages()`` and ``McapReader.iter_decoded_messages()`` accept a keyword-only
``time_basis`` argument:

.. code-block:: python

    from mcap.reader import make_reader

    with open("recording.mcap", "rb") as f:
        reader = make_reader(f)

        # Default: identical to previous releases.
        for schema, channel, message in reader.iter_messages():
            ...

        # Filter and order by publish_time instead.
        for schema, channel, message in reader.iter_messages(
            start_time=publish_start,
            end_time=publish_end,
            time_basis="publish",
        ):
            ...

``time_basis`` is ``"log"`` (the default) or ``"publish"``. It selects which clock drives
``start_time``, ``end_time`` and ordered iteration. The interval is half open: a message is
included when ``start_time <= t < end_time``. Both bounds are optional.

Compatibility rules:

* ``log_time_order`` keeps its old name. ``True`` means "ordered by the selected clock", ``False``
  means "physical order in the file". ``reverse=True`` is still only allowed together with
  ``log_time_order=True``.
* With ``time_basis="publish"`` the raw ``publish_time`` is used; zero is a real timestamp and is
  never replaced by ``log_time``.
* Messages with equal timestamps are ordered by physical position in the file. A reverse read is
  the exact reverse of the corresponding forward sequence. Duplicate messages each appear once.
* ``Message`` fields are never rewritten.
* Time bounds must be non-``bool`` integers in ``[0, 2**64 - 1]`` or ``None``. A ``start_time``
  greater than ``end_time``, an unknown ``time_basis``, or ``reverse`` without ordered output
  raises ``ValueError`` before any message is delivered.
* ``topics``, schemas, decoder factories and the yielded tuples behave as before.
* Every legal ``time_basis="log"`` call, and every call that omits the argument, produces exactly
  the results of previous releases.

Files without the extension, files without a summary, and non-seekable streams
-----------------------------------------------------------------------------

A publish-time query works on any file. When the file does not carry the extension (or the
reader cannot seek, or channels are not repeated in the summary) the reader falls back to a
linear scan and filters every message with the same rules. Ordered output in that fallback
collects the matching messages and sorts them in memory, mirroring the long-standing behaviour
of ``NonSeekingReader`` with ``log_time_order=True``; that mode has never guaranteed bounded
memory. Log-time queries never interpret the extension at all.

Writer option
-------------

``Writer(..., publish_time_index=True)`` additionally writes the extension described below. It is
off by default and has no cost when disabled. It requires ``use_chunking=True`` and
``index_types`` including ``IndexType.CHUNK``; an incompatible combination raises ``ValueError``
in the constructor, before anything is written. The index is written by the normal ``finish()``
call into the same MCAP file; there is no sidecar file and no process-local cache.

The writer retains, for each chunk, one small record per channel that appears in that chunk
(two integers). Total added memory is proportional to the number of chunks and the number of
distinct channels per chunk, plus the current chunk. It never retains the payloads of messages
that have already been written.

Extension layout
----------------

The extension is a private MCAP record (opcode ``0x80``; the MCAP specification reserves
``0x80``–``0xFF`` for application extensions) placed in the summary section. It is not given a
``SummaryOffset`` group, so offset-driven readers that only follow the standard groups never see
it; readers that walk every summary record skip it because the opcode is unknown. A reader that
does not know the extension therefore reads every message exactly as before. The standard
``ChunkIndex``, ``MessageIndex`` and ``Statistics`` records keep their spec-defined ``log_time``
values; the extension adds information and does not replace anything.

The record body (little endian) is::

    magic           8 bytes   b"MCAPPTI\x00"
    version         u32       1
    entry_count     u64
    entries         entry_count times:
        chunk_start_offset  u64       same as ChunkIndex.chunk_start_offset
        chunk_length        u64       same as ChunkIndex.chunk_length
        channel_count       u32       >= 1
        channels            channel_count times:
            channel_id          u16
            publish_min_time    u64       minimum publish_time of this channel in this chunk
            publish_max_time    u64       maximum publish_time, >= publish_min_time
    crc32           u32       CRC-32 of all preceding bytes of the record

The index is a per-chunk, per-channel publish-time *envelope*. It deliberately does not record
individual message positions: the envelope is what both the chunk-pruning rule and the merge
queue need, at a small fraction of the size of a per-message index.

Integrity checks
----------------

``parse_publish_time_index()`` rejects, before any message is delivered:

* an unknown ``version``,
* a truncated record, a declared length that runs past the record body, or trailing bytes,
* a ``channel_count`` of zero, a duplicate ``channel_id`` within an entry, or ``min > max``,
* a CRC mismatch.

``validate_publish_time_index()`` then binds the extension to the standard summary:

* exactly one entry per ``ChunkIndex``, with no duplicate and no missing ``chunk_start_offset``,
* ``chunk_length`` equal to the ``ChunkIndex`` value and at least the minimum possible size of a
  ``Chunk`` record,
* ``chunk_start_offset + chunk_length <= summary_start`` (references stay inside the data
  section),
* when the file also stores standard ``MessageIndex`` records, the channel set of each entry
  equals the ``MessageIndex`` channel set. This detects an extension that is out of sync with the
  standard indexes without reading any message payload,
* every channel id referenced by the extension (or by ``MessageIndex``) is present in the
  summary's ``Channel`` records; otherwise the reader falls back to a linear scan rather than
  silently under-selecting.

Detecting a producer that consistently rewrites every checksum, every index and every summary
record (a malicious forgery) is out of scope. The envelope contents are trusted; comparing them
against the payload would require reading the payload the index exists to avoid.

Selected chunks are still validated with the reader's ``validate_crcs`` setting. A damaged chunk
that the query does not select is never read and does not affect the query; the reader also never
scans the whole file to prove unrelated chunks are intact. A malformed extension raises
``McapError`` from the query; opening a new reader and issuing a valid query still works.

Read behaviour
--------------

With the extension present and a seekable file, a publish-time query:

1. reads the footer and the summary section (including the extension),
2. selects a chunk when at least one selected channel's envelope intersects the window,
3. reads and decompresses only those chunks, then filters messages exactly by ``publish_time``,
4. merges chunks and messages with a priority queue whose chunk keys are the extension
   envelopes (over the selected channels), not the standard log-time bounds.

Consequences:

* A chunk whose ``log_time`` range lies entirely outside the window is still read if its publish
  envelope can match; no matching message is lost.
* A chunk whose selected-channel envelopes are all disjoint from the window is not read at all;
  its compressed payload is never touched.
* A chunk is not read merely because another topic in the same chunk has a publish time in the
  window; only the selected channels' envelopes are consulted.
* An empty window or a topic that does not exist decompresses no message chunk.
* Pruning granularity is the per-chunk, per-channel envelope. A chunk whose envelope intersects
  is read and decompressed in full and filtered precisely; time gaps inside an envelope are not
  exploited, and sub-message compressed reads are not attempted.
* Ordered output keeps only candidate chunks still needed for the merge and the messages currently
  competing in the queue. Chunks already known to be irrelevant are never read in preparation for
  ordered output.

Two possible index organisations
--------------------------------

Two natural layouts were considered.

**A. Per-chunk, per-channel envelope (implemented).** Each chunk stores one (min, max) pair per
channel that appears in it.

* *Size*: ``O(chunks × channels_per_chunk)``, typically ~20 bytes per pair. For many channels with
  wide time spans, size grows with the channel fan-out per chunk; repeated timestamps or dense
  interleaving add nothing.
* *Write maintenance*: two comparisons per message, plus one entry per (chunk, channel) flushed
  when the chunk closes. Memory grows with the chunk and with the number of channels in it.
* *First query*: reads the summary and extension once; selects chunks in a single pass over
  ``ChunkIndex`` records. Only chunks whose selected-channel envelope intersects are read.
* *Merge occupancy*: the queue may hold candidate envelopes for every intersecting chunk (small)
  and the messages of chunks that have been decompressed but not yet drained. With heavy
  cross-chunk overlap, that is the same shape as the existing log-time queue; it does not grow
  with chunks that are not selected.

**B. Per-message publish-time positions.** Store, per chunk and channel, the publish time and
byte offset of every message, analogous to ``MessageIndex``.

* *Size*: ``O(messages)``, roughly 16 bytes per message — much larger. It grows with duplicates
  and dense timestamps, which A absorbs for free.
* *Write maintenance*: an entry per message; memory proportional to the current chunk, similar to
  ``MessageIndex``. Writing is heavier than A, and it duplicates information that is only useful
  for sub-chunk seeking, which the task explicitly does not require.
* *First query*: could prune down to individual messages, but the reader still has to decompress
  the containing chunk to read them. It therefore saves no decompression over A and adds a second
  index to keep consistent.
* *Merge occupancy*: can avoid decompressing a whole chunk when only a sliver matches, but only
  after a chunk-level decision; at equal envelope granularity the queue occupancy is the same.

Because the requirement is envelope-level pruning with whole-chunk decompression, organisation A
is the better fit: it is smaller, cheaper to write, and sufficient to avoid both missed messages
and unnecessary decompression. Organisation B would only pay off with sub-message compressed
reads, which are not in scope.

Cost when not used
------------------

``publish_time_index=False`` (the default) changes nothing in the output file and adds no
per-message work. The standard writer path is unchanged.

Performance experiment
----------------------

``python/mcap/tests/bench_publish_time.py`` builds archives with 2,000 and 20,000 chunks. Each
chunk carries four messages across two topics; ``/hot``'s publish order is the reverse of its log
order, and ``/cold`` has disjoint publish times. It then measures, per query, the payload bytes
read (overlap between the library's requested read intervals and chunk payload spans), the number
of chunks decompressed, messages decoded, peak priority-queue length and wall time. Three runs per
cell are taken and the median elapsed time and byte counts reported.

Run it with (Python 3.11, Linux)::

    cd /workspace
    PYTHONPATH=$PWD/python/mcap python3 -P python/mcap/tests/bench_publish_time.py

``-P`` keeps the script's own directory off ``sys.path`` so a planted module cannot shadow
imports; ``PYTHONPATH`` supplies the ``mcap`` package.

Results (Python 3.11.2, Linux, ZSTD chunks; timings vary run to run, byte and count columns do
not):

    python 3.11.2

    chunks=  2001 dist=few_hits                 returned=     1 payload=       123/270368     all_bytes=    306643/764922     reads= 32099 decompressed=     1 decoded=     0 queue_peak=     1 elapsed=     2.2ms
    chunks=  2001 dist=same_chunk_other_topic   returned=     0 payload=         0/270368     all_bytes=    306520/764922     reads= 32091 decompressed=     0 decoded=     0 queue_peak=     0 elapsed=     2.0ms
    chunks=  2001 dist=all_overlap              returned=  4000 payload=    252276/270368     all_bytes=    558796/764922     reads= 48091 decompressed=  4000 decoded=  4000 queue_peak=  2001 elapsed=    95.8ms
    chunks= 20001 dist=few_hits                 returned=     1 payload=       123/2703812    all_bytes=   3060643/7644366    reads=320099 decompressed=     1 decoded=     0 queue_peak=     1 elapsed=    26.3ms
    chunks= 20001 dist=same_chunk_other_topic   returned=     0 payload=         0/2703812    all_bytes=   3060520/7644366    reads=320091 decompressed=     0 decoded=     0 queue_peak=     0 elapsed=    26.1ms
    chunks= 20001 dist=all_overlap              returned= 40000 payload=   2523720/2703812    all_bytes=   5584240/7644366    reads=480091 decompressed= 40000 decoded= 40000 queue_peak= 20001 elapsed=  1271.7ms

Interpretation:

* ``few_hits``: a narrow publish window matches one message. Exactly one chunk payload is read,
  so payload bytes read are ~123 of ~270 KB (2,000 chunks) and ~123 of ~2.7 MB (20,000 chunks).
  ``all_bytes`` also includes the footer and summary section, which grow with chunk count because
  they hold one ``ChunkIndex`` and one extension entry per chunk.
* ``same_chunk_other_topic``: the window spans every chunk's ``/cold`` publish times while asking
  only for ``/hot``. Zero chunks are decompressed and zero payload bytes are read; the formerly
  plausible "the other topic is in the window, so read the chunk" behaviour does not happen.
* ``all_overlap``: every chunk's ``/hot`` envelope intersects, so every payload is read and
  decompressed (the worst case). The queue holds at most one candidate entry per chunk. This
  confirms the pruning is per envelope, not per message: intersecting chunks are read whole.

The ``decoded`` column is reported only for ``all_overlap``, where decoding is measured on a
separate reader; ``iter_decoded_messages`` calls the decoder exactly once per returned message.

What was verified, and what was not
-----------------------------------

Verified by the test suite (``python/mcap/tests/test_publish_time.py``), with commands below:

* Results computed by an independent model from the original inputs: full message identity
  (topic, sequence, both times, payload), ``log`` and ``publish`` bases, forward and reverse
  order, ties, duplicates, zero publish times, half-open boundaries, topic sets.
* Decoder invocation count equals the number of selected messages.
* A plain ``StreamReader`` (the "old reader") reads every message from a file with the extension,
  with unchanged fields.
* Old files with no extension, no summary, and non-seekable/pipe streams still answer publish
  queries.
* Corrupted extension (bad CRC, unknown version, incomplete count, duplicate chunk reference,
  out-of-range length) raises ``McapError`` before delivery; a damaged, unselected chunk does not
  affect a query, and a fresh reader still works afterwards.
* Read-savings checks: unrelated chunk payloads are not read, empty windows and missing topics do
  not decompress, and the first query does not read most chunks.
* Linux, Python 3.11.2: ``python3 -m pytest python/mcap/tests`` (120 tests), including the
  pristine 1.3.1 reader driven as a subprocess against a newly written file (the test skips if a
  copy of the pre-extension package is not available at ``$MCAP_ORIGINAL_PACKAGE``, default
  ``/tmp/orig_mcap``).

Not verified / out of scope:

* Sub-message compressed reads: an intersecting chunk is decompressed whole.
* Detecting an attacker who rewrites every checksum and index consistently.
* Appending to a partially written file, or recovering unfinished files.
* Other Python versions and non-Linux platforms. The CI matrix drives Python 3.8 for the core
  package; this work was exercised on 3.11.2 only.
* The other language implementations' conformance runners (Go/TypeScript/Rust/Swift) were not
  executed here; the extension is a private record and is not interpreted by them.
