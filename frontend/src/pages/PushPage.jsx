import { useEffect, useMemo, useRef, useState } from 'react'
import './PushPage.css'

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

const formatElapsed = (seconds) => {
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = seconds % 60
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
}

function SyncTimeline({ events }) {
  if (!events || events.length === 0) return null
  return (
    <div className="sync-timeline">
      {events.map((ev, idx) => (
        <div key={idx} className={`timeline-step ${ev.error ? 'step-error' : ev.active ? 'step-active' : 'step-done'}`}>
          <div className="step-dot" />
          <div className="step-content">
            <span className="step-label">{ev.label}</span>
            {ev.time && <span className="step-time">{ev.time}</span>}
          </div>
        </div>
      ))}
    </div>
  )
}

const renderTableReport = (title, report) => {
  if (!report) return null
  const missingCount = report.missing_row_ids?.length || 0
  return (
    <div className="table-report">
      <div className="table-report-head">
        <p className="label">{title}</p>
        <span className={`pill ${missingCount ? 'pill-warn' : 'pill-ok'}`}>
          {missingCount ? `${missingCount} rows pending` : report.attempted > 0 ? 'All pushed' : 'Nothing to push'}
        </span>
      </div>
      <div className="mini-stats">
        <div>
          <p className="label">Attempted</p>
          <p className="value small">{report.attempted ?? 0}</p>
        </div>
        <div>
          <p className="label">Pushed</p>
          <p className="value small">{report.upserted ?? 0}</p>
        </div>
        <div>
          <p className="label">Failed</p>
          <p className="value small">{missingCount}</p>
        </div>
      </div>
      {report.retry_batches?.length ? (
        <div className="retry-block">
          <p className="label">Retry batches</p>
          <ul className="retry-list">
            {report.retry_batches.map((batch, idx) => (
              <li key={`${title}-retry-${idx}`}>
                <span className="badge">#{idx + 1}</span>
                <span className="ids">{(batch.row_ids || []).join(', ') || 'n/a'}</span>
                <span className="reason">{batch.reason}</span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
      {report.errors?.length ? (
        <div className="error-list">
          {report.errors.map((msg, idx) => (
            <p key={`${title}-err-${idx}`} className="warn">
              {msg}
            </p>
          ))}
        </div>
      ) : null}
    </div>
  )
}

export default function PushPage() {
  const [unsyncedCount, setUnsyncedCount] = useState(null)
  const [tables, setTables] = useState({ sales: true, payments: true, line_items: true })
  const [batchSize, setBatchSize] = useState('500')
  const [pushStatus, setPushStatus] = useState({ tone: 'idle', message: 'Ready to push' })
  const [summary, setSummary] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [health, setHealth] = useState(null)
  const [elapsed, setElapsed] = useState(null)
  const [timelineEvents, setTimelineEvents] = useState([])
  const timerRef = useRef(null)

  const apiRoot = useMemo(() => API_BASE.replace(/\/$/, ''), [])

  const startTimer = () => {
    setElapsed(0)
    if (timerRef.current) clearInterval(timerRef.current)
    timerRef.current = setInterval(() => setElapsed((s) => s + 1), 1000)
  }

  const stopTimer = () => {
    if (timerRef.current) {
      clearInterval(timerRef.current)
      timerRef.current = null
    }
  }

  useEffect(() => () => stopTimer(), [])

  const loadCounts = async () => {
    try {
      const res = await fetch(`${apiRoot}/local/unsynced-count`)
      if (res.ok) setUnsyncedCount(await res.json())
    } catch (_) {
      // best-effort
    }
  }

  useEffect(() => {
    loadCounts()
    const checkHealth = async () => {
      try {
        const res = await fetch(`${apiRoot}/health`)
        if (res.ok) setHealth(await res.json())
      } catch (_) {}
    }
    checkHealth()
  }, [apiRoot])

  const toggleTable = (key) => {
    setTables((prev) => ({ ...prev, [key]: !prev[key] }))
  }

  const submit = async () => {
    const selectedTables = Object.keys(tables).filter((k) => tables[k])
    if (selectedTables.length === 0) {
      setPushStatus({ tone: 'error', message: 'Select at least one table to push.' })
      return
    }

    setIsLoading(true)
    setPushStatus({ tone: 'busy', message: 'Pushing to Oracle…' })
    setSummary(null)

    const now = new Date().toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
    setTimelineEvents([{ label: '⟳ Push started', time: now, active: true, done: false }])
    startTimer()

    try {
      const res = await fetch(`${apiRoot}/push`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tables: selectedTables, batch_size: Number(batchSize) || 500 }),
      })

      if (!res.ok) {
        const detail = await res.text()
        throw new Error(detail || 'Push failed')
      }

      const data = await res.json()
      setSummary(data)
      const pushed = (data.sales_pushed || 0) + (data.payments_pushed || 0) + (data.line_items_pushed || 0)
      setPushStatus({
        tone: data.data_integrity_ok ? 'success' : 'error',
        message: data.oracle?.connected
          ? `Push complete. ${pushed} rows sent to Oracle.`
          : 'Oracle not reachable. Check connection settings.',
      })
      const doneTime = new Date().toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
      setTimelineEvents((prev) => {
        const updated = prev.map((e) => ({ ...e, active: false, done: true }))
        return [
          ...updated,
          { label: `✓ Push complete - ${pushed} rows`, time: doneTime, active: false, done: true },
        ]
      })
      await loadCounts()
    } catch (err) {
      setPushStatus({ tone: 'error', message: err.message })
      const errTime = new Date().toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
      setTimelineEvents((prev) => [
        ...prev,
        { label: `⚠ ${err.message}`, time: errTime, active: false, done: true, error: true },
      ])
    } finally {
      stopTimer()
      setIsLoading(false)
    }
  }

  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">Local Database → Oracle</p>
        <h1>Push to Oracle</h1>
        <p className="lede">
          Select which tables to push and send unsynced rows from the local database to Oracle.
          Already-synced rows are skipped automatically.
        </p>
      </header>

      <main className="grid">
        <section className="panel">
          <div className="panel-head">
            <h2>Push settings</h2>
            <p>Choose tables and batch size, then trigger the push.</p>
          </div>

          {health && (
            <div className={`connection-badge ${health.oracle_connected ? 'badge-ok' : 'badge-warn'}`}>
              <span className="dot" />
              Oracle: {health.oracle_connected ? `Connected to ${health.oracle_target}` : 'Not connected'}
            </div>
          )}

          <div className="table-checks">
            <p className="label" style={{ marginBottom: '0.5rem' }}>Tables to push</p>
            {[
              { key: 'sales', label: 'Sales', count: unsyncedCount?.sales },
              { key: 'payments', label: 'Payments', count: unsyncedCount?.payments },
              { key: 'line_items', label: 'Line Items', count: unsyncedCount?.line_items },
            ].map(({ key, label, count }) => (
              <label key={key} className="check-label">
                <input
                  type="checkbox"
                  checked={tables[key]}
                  onChange={() => toggleTable(key)}
                />
                <span>{label}</span>
                {count !== undefined && (
                  <span className={`pill-sm ${count > 0 ? 'pill-warn' : 'pill-ok'}`}>
                    {count > 0 ? `${count} pending` : 'synced'}
                  </span>
                )}
              </label>
            ))}
          </div>

          <label className="field" style={{ marginTop: '1rem' }}>
            <span>Batch size (rows per table)</span>
            <input
              type="number"
              min="1"
              max="5000"
              value={batchSize}
              onChange={(e) => setBatchSize(e.target.value)}
            />
          </label>

          <button className="cta" style={{ marginTop: '1.25rem' }} onClick={submit} disabled={isLoading}>
            {isLoading ? 'Pushing…' : 'Push to Oracle'}
          </button>

          <div className={`status ${pushStatus.tone}`} style={{ marginTop: '0.75rem' }}>
            <span className="dot" />
            <span>{pushStatus.message}</span>
          </div>

          {elapsed != null && (
            <div style={{ marginTop: '0.75rem' }}>
              <span className={`elapsed-badge ${isLoading ? 'elapsed-running' : 'elapsed-done'}`}>
                ⏱ {formatElapsed(elapsed)}
              </span>
            </div>
          )}

          <SyncTimeline events={timelineEvents} />
        </section>

        <section className="panel summary">
          <div className="panel-head">
            <h2>Push result</h2>
            <p>Rows pushed to Oracle in this session.</p>
          </div>
          {summary ? (
            <>
              <div className="integrity">
                <span className={`pill ${summary.data_integrity_ok ? 'pill-ok' : 'pill-warn'}`}>
                  {summary.data_integrity_ok ? 'All pushed' : 'Some rows failed'}
                </span>
                <p className="hint">
                  Oracle target: {summary.oracle?.target || 'not configured'} · User:{' '}
                  {summary.oracle?.user || 'n/a'} · Connection:{' '}
                  {summary.oracle?.connected ? 'connected' : 'not connected'}
                </p>
              </div>
              <div className="stats">
                <div className="stat">
                  <p className="label">Sales pushed</p>
                  <p className="value">{summary.sales_pushed}</p>
                </div>
                <div className="stat">
                  <p className="label">Payments</p>
                  <p className="value">{summary.payments_pushed}</p>
                </div>
                <div className="stat">
                  <p className="label">Line items</p>
                  <p className="value">{summary.line_items_pushed}</p>
                </div>
              </div>
              <div className="report-grid">
                {renderTableReport('Sales table', summary.sales_report)}
                {renderTableReport('Payments table', summary.payments_report)}
                {renderTableReport('Line items table', summary.line_items_report)}
              </div>
            </>
          ) : (
            <div className="placeholder">
              <p>No push yet.</p>
              <p className="hint">Results will appear here after you push.</p>
              {unsyncedCount && (
                <div className="integrity" style={{ marginTop: '1rem' }}>
                  <p className="hint">
                    Pending: {unsyncedCount.sales} sales · {unsyncedCount.payments} payments ·{' '}
                    {unsyncedCount.line_items} line items
                  </p>
                </div>
              )}
            </div>
          )}
        </section>
      </main>
    </div>
  )
}
