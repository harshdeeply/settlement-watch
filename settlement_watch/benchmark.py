"""Repeatable local ingest benchmark; synthetic events, not production throughput."""
import argparse
import json
import resource
import tempfile
import time
from pathlib import Path
from .engine import Engine


def stream(payments):
    for payment in range(payments):
        amount = 1000 + (payment % 9000)
        for part in range(5):
            for source in ('ledger', 'processor'):
                yield {'tenant_id': 'benchmark-shop', 'payment_id': f'pay-{payment:08d}',
                       'event_id': f'{source}-{payment:08d}-{part}', 'source': source,
                       'kind': 'capture', 'amount_minor': amount, 'currency': 'CAD',
                       'occurred_at': f'2026-09-23T12:{part:02d}:00Z'}


def run(payments, db_path):
    engine = Engine(db_path)
    start = time.perf_counter()
    counts = engine.ingest_many(stream(payments))
    elapsed = time.perf_counter() - start
    engine.advance_watermark('benchmark-shop', 'ledger', '2026-09-23T13:00:00Z')
    engine.advance_watermark('benchmark-shop', 'processor', '2026-09-23T13:00:00Z')
    first = engine.payment('benchmark-shop', 'pay-00000000')
    last = engine.payment('benchmark-shop', f'pay-{payments-1:08d}')
    assert counts == {'inserted': payments * 10, 'replayed': 0}
    assert first['status'] == last['status'] == 'BALANCED'
    engine.close()
    size = sum(p.stat().st_size for p in Path(db_path).parent.glob(Path(db_path).name + '*'))
    return {'payments': payments, 'events': counts['inserted'], 'seconds': round(elapsed, 3),
            'events_per_second': round(counts['inserted'] / elapsed), 'database_bytes': size,
            'peak_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'dataset': 'synthetic, generated lazily; single local SQLite writer',
            'environment': 'local container; not a production performance claim'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--payments', type=int, default=100000)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        print(json.dumps(run(args.payments, str(Path(tmp) / 'bench.db')), indent=2))
