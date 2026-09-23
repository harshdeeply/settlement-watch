# Browser replay

A Vite/React/TypeScript operator interface with a dedicated Web Worker. `sim.worker.ts` generates fictional payment events in batches, deduplicates exact replay IDs, maintains per-payment totals, advances two source watermarks, and allows an explicit late correction. Select 10,000 or 100,000 payment keys and restart the replay. No API key, customer data, payment connector, or money movement is present.

```bash
npm ci
npm run lint
npm run build
npm run dev
```

The hosted site is a static deploy of `dist/`. The measured SQLite benchmark in the root repository is **independent** of this worker replay. Worker throughput varies by browser and is not a server benchmark. See the root README and architecture notes for the durable engine, optional AI triage, and production gaps.
