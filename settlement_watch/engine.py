"""Durable, event-time reconciliation. Amounts are integer minor units."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

SOURCES = ('ledger', 'processor')
KINDS = ('capture', 'refund')
COLUMNS = {(s, k): f'{s}_{k}' for s in SOURCES for k in KINDS}


class Conflict(ValueError):
    pass


def timestamp(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError('timestamp must be ISO 8601 with timezone')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('invalid timestamp') from exc
    if parsed.tzinfo is None:
        raise ValueError('timestamp needs timezone')
    return parsed.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def event(value: dict) -> dict:
    fields = {'tenant_id', 'payment_id', 'event_id', 'source', 'kind', 'amount_minor', 'currency', 'occurred_at'}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('event fields do not match contract')
    row = dict(value)
    for field in ('tenant_id', 'payment_id', 'event_id'):
        if not isinstance(row[field], str) or not 1 <= len(row[field]) <= 100 or row[field].strip() != row[field]:
            raise ValueError(f'invalid {field}')
    if row['source'] not in SOURCES or row['kind'] not in KINDS:
        raise ValueError('invalid source or kind')
    if type(row['amount_minor']) is not int or not 0 < row['amount_minor'] < 10**12:
        raise ValueError('amount_minor must be positive integer minor units')
    if not isinstance(row['currency'], str) or len(row['currency']) != 3 or not row['currency'].isupper():
        raise ValueError('currency must be a three-letter uppercase code')
    row['occurred_at'] = timestamp(row['occurred_at'])
    return row


class Engine:
    def __init__(self, path: str | Path = ':memory:', grace_seconds: int = 120):
        if not 0 <= grace_seconds <= 86400:
            raise ValueError('grace_seconds out of range')
        self.grace = grace_seconds
        self.db = sqlite3.connect(str(path), isolation_level='DEFERRED')
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA busy_timeout=5000')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS events (
          tenant_id TEXT NOT NULL, event_id TEXT NOT NULL, payment_id TEXT NOT NULL,
          source TEXT NOT NULL, kind TEXT NOT NULL, amount_minor INTEGER NOT NULL,
          currency TEXT NOT NULL, occurred_at TEXT NOT NULL, fingerprint TEXT NOT NULL,
          late INTEGER NOT NULL, PRIMARY KEY (tenant_id, event_id)
        );
        CREATE INDEX IF NOT EXISTS events_payment ON events(tenant_id, payment_id, occurred_at);
        CREATE TABLE IF NOT EXISTS payments (
          tenant_id TEXT NOT NULL, payment_id TEXT NOT NULL, currency TEXT NOT NULL,
          ledger_capture INTEGER NOT NULL DEFAULT 0, processor_capture INTEGER NOT NULL DEFAULT 0,
          ledger_refund INTEGER NOT NULL DEFAULT 0, processor_refund INTEGER NOT NULL DEFAULT 0,
          first_at TEXT NOT NULL, last_at TEXT NOT NULL, due_at TEXT NOT NULL,
          events INTEGER NOT NULL DEFAULT 0, late_events INTEGER NOT NULL DEFAULT 0,
          revision INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (tenant_id, payment_id)
        );
        CREATE TABLE IF NOT EXISTS watermarks (
          tenant_id TEXT NOT NULL, source TEXT NOT NULL, complete_through TEXT NOT NULL,
          PRIMARY KEY (tenant_id, source)
        );
        ''')

    def close(self):
        self.db.close()

    def ingest(self, raw: dict) -> str:
        with self.db:
            return self._ingest(event(raw))

    def ingest_many(self, raws: Iterable[dict]) -> dict:
        counts = {'inserted': 0, 'replayed': 0}
        with self.db:
            for raw in raws:
                result = self._ingest(event(raw))
                counts['inserted' if result == 'inserted' else 'replayed'] += 1
        return counts

    def _ingest(self, row: dict) -> str:
        identity = (row['tenant_id'], row['event_id'])
        fingerprint = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        prior = self.db.execute('SELECT fingerprint FROM events WHERE tenant_id=? AND event_id=?', identity).fetchone()
        if prior:
            if prior['fingerprint'] == fingerprint:
                return 'replayed'
            raise Conflict('event ID reused with different payload')
        prior_payment = self.db.execute('SELECT currency FROM payments WHERE tenant_id=? AND payment_id=?',
                                        (row['tenant_id'], row['payment_id'])).fetchone()
        if prior_payment and prior_payment['currency'] != row['currency']:
            raise Conflict('payment currency changed')
        watermark = self.db.execute('SELECT complete_through FROM watermarks WHERE tenant_id=? AND source=?',
                                    (row['tenant_id'], row['source'])).fetchone()
        late = int(bool(watermark and row['occurred_at'] <= watermark['complete_through']))
        due = timestamp((datetime.fromisoformat(row['occurred_at'].replace('Z', '+00:00')) +
                         timedelta(seconds=self.grace)).isoformat())
        self.db.execute('''INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?)''',
                        (*identity, row['payment_id'], row['source'], row['kind'], row['amount_minor'],
                         row['currency'], row['occurred_at'], fingerprint, late))
        column = COLUMNS[(row['source'], row['kind'])]  # Only allowlisted identifiers enter SQL.
        self.db.execute(f'''INSERT INTO payments(tenant_id,payment_id,currency,{column},first_at,last_at,due_at,events,late_events,revision)
            VALUES(?,?,?,?,?,?,?,1,?,1)
            ON CONFLICT(tenant_id,payment_id) DO UPDATE SET
            {column}={column}+excluded.{column},
            first_at=min(first_at,excluded.first_at), last_at=max(last_at,excluded.last_at),
            due_at=max(due_at,excluded.due_at), events=events+1,
            late_events=late_events+excluded.late_events, revision=revision+1''',
                        (row['tenant_id'], row['payment_id'], row['currency'], row['amount_minor'],
                         row['occurred_at'], row['occurred_at'], due, late))
        return 'inserted'

    def advance_watermark(self, tenant_id: str, source: str, complete_through: str):
        if not isinstance(tenant_id, str) or not tenant_id.strip() or source not in SOURCES:
            raise ValueError('invalid tenant or source')
        through = timestamp(complete_through)
        with self.db:
            prior = self.db.execute('SELECT complete_through FROM watermarks WHERE tenant_id=? AND source=?',
                                    (tenant_id, source)).fetchone()
            if prior and through < prior['complete_through']:
                raise Conflict('source watermark cannot move backwards')
            self.db.execute('''INSERT INTO watermarks VALUES (?,?,?) ON CONFLICT(tenant_id,source)
                DO UPDATE SET complete_through=excluded.complete_through''', (tenant_id, source, through))

    def watermarks(self, tenant_id: str) -> dict:
        return {row['source']: row['complete_through'] for row in self.db.execute(
            'SELECT source,complete_through FROM watermarks WHERE tenant_id=?', (tenant_id,))}

    @staticmethod
    def classify(row: dict, marks: dict) -> tuple[str, list[str]]:
        differences = []
        for kind in KINDS:
            left, right = row[f'ledger_{kind}'], row[f'processor_{kind}']
            if left != right:
                differences.append(f'{kind}: ledger {left} vs processor {right} minor units')
        if not differences:
            return 'BALANCED', []
        if all(marks.get(source, '') >= row['due_at'] for source in SOURCES):
            return 'EXCEPTION', differences
        return 'WAITING_FOR_SOURCE', differences

    def payment(self, tenant_id: str, payment_id: str) -> dict | None:
        row = self.db.execute('SELECT * FROM payments WHERE tenant_id=? AND payment_id=?',
                              (tenant_id, payment_id)).fetchone()
        if not row:
            return None
        result = dict(row)
        result['watermarks'] = self.watermarks(tenant_id)
        result['status'], result['differences'] = self.classify(result, result['watermarks'])
        result['evidence'] = [dict(e) for e in self.db.execute('''SELECT event_id,source,kind,amount_minor,currency,occurred_at,late
            FROM events WHERE tenant_id=? AND payment_id=? ORDER BY occurred_at,event_id LIMIT 100''',
            (tenant_id, payment_id))]
        result['evidence_truncated'] = result['events'] > 100
        return result

    def list_payments(self, tenant_id: str, limit: int = 100) -> list[dict]:
        if not 1 <= limit <= 1000:
            raise ValueError('limit out of range')
        marks = self.watermarks(tenant_id)
        rows = self.db.execute('SELECT * FROM payments WHERE tenant_id=? ORDER BY last_at DESC LIMIT ?',
                               (tenant_id, limit))
        result = []
        for row in rows:
            item = dict(row)
            item['status'], item['differences'] = self.classify(item, marks)
            result.append(item)
        return result

    def counts(self, tenant_id: str) -> dict:
        # Exact aggregate; costs O(number of payment keys), explicitly not an ingestion hot-path operation.
        marks = self.watermarks(tenant_id)
        totals = {'BALANCED': 0, 'WAITING_FOR_SOURCE': 0, 'EXCEPTION': 0, 'payments': 0, 'events': 0, 'late_events': 0}
        for row in self.db.execute('SELECT * FROM payments WHERE tenant_id=?', (tenant_id,)):
            status, _ = self.classify(row, marks)
            totals[status] += 1
            totals['payments'] += 1
            totals['events'] += row['events']
            totals['late_events'] += row['late_events']
        return totals
