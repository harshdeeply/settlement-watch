import tempfile
import unittest
from pathlib import Path

from settlement_watch.engine import Conflict, Engine


def e(identifier, *, tenant='shop-a', payment='pay-1', source='ledger', kind='capture', amount=2500,
      at='2026-09-23T12:00:00Z', currency='CAD'):
    return {'tenant_id': tenant, 'payment_id': payment, 'event_id': identifier,
            'source': source, 'kind': kind, 'amount_minor': amount,
            'currency': currency, 'occurred_at': at}


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = Engine(grace_seconds=60)

    def tearDown(self):
        self.engine.close()

    def marks(self, tenant='shop-a', at='2026-09-23T12:05:00Z'):
        for source in ('ledger', 'processor'):
            self.engine.advance_watermark(tenant, source, at)

    def test_missing_processor_waits_for_both_source_watermarks(self):
        self.engine.ingest(e('l1'))
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'WAITING_FOR_SOURCE')
        self.engine.advance_watermark('shop-a', 'ledger', '2026-09-23T12:05:00Z')
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'WAITING_FOR_SOURCE')
        self.engine.advance_watermark('shop-a', 'processor', '2026-09-23T12:00:30Z')
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'WAITING_FOR_SOURCE')
        self.engine.advance_watermark('shop-a', 'processor', '2026-09-23T12:01:00Z')
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'EXCEPTION')

    def test_late_arrival_corrects_exception_with_auditable_revision(self):
        self.engine.ingest(e('l1'))
        self.marks()
        before = self.engine.payment('shop-a', 'pay-1')
        self.assertEqual(before['status'], 'EXCEPTION')
        self.engine.ingest(e('p1', source='processor'))
        after = self.engine.payment('shop-a', 'pay-1')
        self.assertEqual(after['status'], 'BALANCED')
        self.assertEqual(after['late_events'], 1)
        self.assertEqual(after['revision'], before['revision'] + 1)
        self.assertEqual(after['evidence'][1]['late'], 1)

    def test_out_of_order_partial_capture_does_not_raise_early_exception(self):
        self.engine.ingest(e('p2', source='processor', amount=1500, at='2026-09-23T12:00:20Z'))
        self.engine.ingest(e('l1'))
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'WAITING_FOR_SOURCE')
        self.engine.ingest(e('p1', source='processor', amount=1000, at='2026-09-23T11:59:50Z'))
        item = self.engine.payment('shop-a', 'pay-1')
        self.assertEqual(item['status'], 'BALANCED')
        self.assertEqual(item['first_at'], '2026-09-23T11:59:50.000Z')

    def test_refund_is_reconciled_independently_of_capture(self):
        self.engine.ingest_many([e('l1'), e('p1', source='processor'), e('lr', kind='refund', amount=700)])
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'WAITING_FOR_SOURCE')
        self.marks()
        self.assertIn('refund', self.engine.payment('shop-a', 'pay-1')['differences'][0])
        self.engine.ingest(e('pr', source='processor', kind='refund', amount=700))
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'BALANCED')

    def test_exact_replay_is_noop_conflicting_id_is_rejected(self):
        self.assertEqual(self.engine.ingest(e('l1')), 'inserted')
        self.assertEqual(self.engine.ingest(e('l1')), 'replayed')
        with self.assertRaises(Conflict):
            self.engine.ingest(e('l1', amount=9000))
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['events'], 1)

    def test_batch_rejects_all_rows_on_conflict(self):
        self.engine.ingest(e('l1'))
        with self.assertRaises(Conflict):
            self.engine.ingest_many([e('p1', source='processor'), e('l1', amount=3)])
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['events'], 1)

    def test_tenant_scoping_and_watermark_isolation(self):
        self.engine.ingest_many([e('l1'), e('l1', tenant='shop-b')])
        self.marks()
        self.assertEqual(self.engine.payment('shop-a', 'pay-1')['status'], 'EXCEPTION')
        self.assertEqual(self.engine.payment('shop-b', 'pay-1')['status'], 'WAITING_FOR_SOURCE')
        self.assertEqual(self.engine.counts('shop-b')['events'], 1)

    def test_currency_change_and_backward_watermark_rejected(self):
        self.engine.ingest(e('l1'))
        with self.assertRaises(Conflict):
            self.engine.ingest(e('p1', source='processor', currency='USD'))
        self.marks()
        with self.assertRaises(Conflict):
            self.engine.advance_watermark('shop-a', 'ledger', '2026-09-23T12:00:00Z')

    def test_invalid_amount_or_naive_time_rejected(self):
        for candidate in (e('x', amount=0), e('x', amount=2.5), e('x', at='2026-09-23T12:00:00')):
            with self.assertRaises(ValueError):
                self.engine.ingest(candidate)
        self.assertEqual(self.engine.counts('shop-a')['payments'], 0)

    def test_restart_preserves_event_ids_watermarks_and_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'state.db'
            first = Engine(path)
            first.ingest(e('l1'))
            first.advance_watermark('shop-a', 'ledger', '2026-09-23T12:05:00Z')
            first.close()
            second = Engine(path)
            self.assertEqual(second.ingest(e('l1')), 'replayed')
            self.assertIn('ledger', second.watermarks('shop-a'))
            self.assertEqual(second.payment('shop-a', 'pay-1')['events'], 1)
            second.close()


if __name__ == '__main__':
    unittest.main()
