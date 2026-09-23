# Local synthetic ingestion benchmark

Run: `python3 -m settlement_watch.benchmark --payments 100000`

On the 2026-09-23 container run, 100,000 payment keys × 10 capture events each = **1,000,000 generated events**. The engine ingested them in **22.652 seconds**, or **44,146 events/second**, into a 313,827,328-byte SQLite file. Peak process RSS reported by `resource.getrusage` was **15,488 KiB**. The input iterator generates one event at a time; it does not preload a million rows. A first and last payment were checked after both source watermarks advanced.

This is a single local writer, one transaction, default local storage, generated uniform keys, and no network or concurrent readers. It is **not** a production SLA, an end-to-end processor integration, horizontal scaling evidence, or a guarantee of stable throughput. WAL/checkpoint and storage behavior depend on the host. Re-run the command on the target machine and report its environment. The benchmark does not time exact count scans, model calls, or external delivery.
