/// <reference lib="webworker" />

type Source = 'ledger' | 'processor'
type Row = { id: string; payment: string; source: Source; amount: number; at: number }
type Payment = { ledger: number; processor: number; events: Row[]; revision: number; late: number; lastAt: number }
const payments = new Map<string, Payment>()
const seen = new Map<string, string>()
const marks: Record<Source, number> = { ledger: 3, processor: 1 }
let target = 10000, cursor = 0, accepted = 0, replays = 0, late = 0, running = false, fast = true
const relevant = ['pay-00000257', 'pay-00000509', 'pay-00001028']

function status(p: Payment): 'balanced' | 'waiting' | 'exception' {
  if (p.ledger === p.processor) return 'balanced'
  return marks.ledger >= p.lastAt + 1 && marks.processor >= p.lastAt + 1 ? 'exception' : 'waiting'
}
function ingest(row: Row) {
  const fingerprint = `${row.payment}:${row.source}:${row.amount}:${row.at}`
  const prior = seen.get(row.id)
  if (prior) {
    if (prior !== fingerprint) throw new Error('Conflicting event ID')
    replays++
    return
  }
  seen.set(row.id, fingerprint)
  const payment = payments.get(row.payment) ?? { ledger: 0, processor: 0, events: [], revision: 0, late: 0, lastAt: row.at }
  payment[row.source] += row.amount
  payment.revision++
  payment.lastAt = Math.max(payment.lastAt, row.at)
  if (row.at <= marks[row.source]) { payment.late++; late++ }
  if (relevant.includes(row.payment)) payment.events.push(row)
  payments.set(row.payment, payment)
  accepted++
}
function detail(id: string) {
  const p = payments.get(id)
  return p ? { id, ...p, status: status(p), gap: p.ledger - p.processor } : null
}
function publish() {
  let balanced = 0, waiting = 0, exceptions = 0
  for (const p of payments.values()) {
    const s = status(p)
    if (s === 'balanced') balanced++
    else if (s === 'waiting') waiting++
    else exceptions++
  }
  postMessage({ type: 'snapshot', target, cursor, accepted, replays, late, running, marks: { ...marks },
    balanced, waiting, exceptions, samples: relevant.map(detail).filter(Boolean) })
}
function emitPayment(index: number) {
  const id = `pay-${String(index).padStart(8, '0')}`
  const amount = 1000 + index % 9000
  for (let part = 0; part < 5; part++) {
    for (const source of ['ledger', 'processor'] as const) {
      // Every 257th payment misses one processor event. Every 509th has a 300-unit mismatch.
      if (source === 'processor' && part === 0 && index % 257 === 0 && index !== 0) continue
      const adjusted = source === 'processor' && part === 0 && index % 509 === 0 && index !== 0 ? amount - 300 : amount
      const row = { id: `${source}-${id}-${part}`, payment: id, source, amount: adjusted, at: part }
      ingest(row)
      if (index % 997 === 0 && part === 0) ingest(row) // Exact replay leaves sums unchanged.
    }
  }
}
function pump() {
  if (!running) return
  const batch = fast ? 1000 : 40
  const end = Math.min(target, cursor + batch)
  for (; cursor < end; cursor++) emitPayment(cursor)
  publish()
  if (cursor < target) setTimeout(pump, fast ? 0 : 80)
  else { running = false; publish() }
}
self.onmessage = (message: MessageEvent<{ type: string; payments?: number; fast?: boolean; id?: string }>) => {
  const command = message.data
  if (command.type === 'start') {
    payments.clear(); seen.clear(); marks.ledger = 3; marks.processor = 1
    target = command.payments ?? 10000; fast = command.fast ?? true
    cursor = accepted = replays = late = 0; running = true; publish(); pump()
  } else if (command.type === 'pause') { running = false; publish() }
  else if (command.type === 'resume' && cursor < target) { running = true; pump() }
  else if (command.type === 'mature') { marks.ledger = 10; marks.processor = 10; publish() }
  else if (command.type === 'correct') {
    const id = command.id ?? 'pay-00000257'
    const index = Number(id.slice(4))
    if (payments.has(id) && index % 257 === 0 && index !== 0) {
      ingest({ id: `processor-${id}-0`, payment: id, source: 'processor', amount: 1000 + index % 9000, at: 0 })
    }
    publish()
  } else if (command.type === 'snapshot') publish()
}
