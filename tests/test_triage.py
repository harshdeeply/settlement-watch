import io
import json
import unittest
from settlement_watch.engine import Engine
from settlement_watch.triage import model_brief, offline_brief, validate_brief
from test_engine import e


class TriageTests(unittest.TestCase):
    def setUp(self):
        self.engine = Engine(grace_seconds=0)
        self.engine.ingest(e('l1'))
        for source in ('ledger', 'processor'):
            self.engine.advance_watermark('shop-a', source, '2026-09-23T12:05:00Z')
        self.payment = self.engine.payment('shop-a', 'pay-1')

    def tearDown(self):
        self.engine.close()

    def test_offline_brief_does_not_claim_root_cause(self):
        self.assertIn('does not establish a root cause', offline_brief(self.payment)['summary'])

    def test_unknown_citation_or_high_confidence_is_rejected(self):
        base = {'summary': 'Inspect the mismatch.', 'hypotheses': [
            {'possibility': 'Processor event is delayed.', 'evidence_ids': ['unknown'], 'confidence': 'low'}],
            'next_checks': ['Compare the source export.']}
        with self.assertRaises(ValueError):
            validate_brief(base, self.payment)
        base['hypotheses'][0]['evidence_ids'] = ['l1']
        base['hypotheses'][0]['confidence'] = 'high'
        with self.assertRaises(ValueError):
            validate_brief(base, self.payment)

    def test_model_receives_bounded_evidence_and_returns_structured_brief(self):
        output = {'summary': 'Ledger has a capture without a processor record.', 'hypotheses': [
            {'possibility': 'Processor export is incomplete.', 'evidence_ids': ['l1'], 'confidence': 'low'}],
            'next_checks': ['Inspect processor delivery log.']}
        captured = []
        class Response(io.BytesIO):
            def __enter__(self): return self
            def __exit__(self, *_): self.close()
        def opener(request, timeout):
            captured.append(json.loads(request.data))
            return Response(json.dumps({'status': 'completed', 'output': [
                {'type': 'message', 'content': [{'type': 'output_text', 'text': json.dumps(output)}]}]}).encode())
        self.assertEqual(model_brief(self.payment, api_key='test-key', opener=opener), output)
        self.assertEqual(captured[0]['store'], False)
        self.assertNotIn('tenant_id', json.loads(captured[0]['input']))

    def test_model_bypassed_while_waiting_for_source(self):
        self.engine.advance_watermark('shop-a', 'processor', '2026-09-23T12:05:00Z')
        self.engine.ingest(e('l2', payment='pay-2'))
        pending = self.engine.payment('shop-a', 'pay-2')
        self.assertEqual(pending['status'], 'EXCEPTION')  # Both watermarks had already advanced.
        self.engine.ingest(e('l3', payment='pay-3', at='2026-09-23T12:10:00Z'))
        pending = self.engine.payment('shop-a', 'pay-3')
        self.assertEqual(pending['status'], 'WAITING_FOR_SOURCE')
        self.assertIn('Wait for complete', model_brief(pending)['summary'])


if __name__ == '__main__':
    unittest.main()
