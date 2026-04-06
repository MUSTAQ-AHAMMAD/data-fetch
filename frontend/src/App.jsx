import { useEffect, useMemo, useState } from 'react'
import './App.css'

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

const toInputValue = (value) => {
  const normalized = new Date(value)
  normalized.setSeconds(0, 0)
  return normalized.toISOString().slice(0, 16)
}

const defaultEnd = () => toInputValue(new Date())
const defaultStart = () => {
  const start = new Date()
  start.setDate(start.getDate() - 1)
  return toInputValue(start)
}

const renderTableReport = (title, report) => {
  if (!report) return null
  const missingCount = report.missing_row_ids?.length || 0
  return (
    <div className="table-report">
      <div className="table-report-head">
        <p className="label">{title}</p>
        <span className={`pill ${missingCount ? 'pill-warn' : 'pill-ok'}`}>
          {missingCount ? `${missingCount} rows pending` : 'Mapped'}
        </span>
      </div>
      <div className="mini-stats">
        <div>
          <p className="label">Attempted</p>
          <p className="value small">{report.attempted ?? 0}</p>
        </div>
        <div>
          <p className="label">Upserted</p>
          <p className="value small">{report.upserted ?? 0}</p>
        </div>
        <div>
          <p className="label">Missing</p>
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
      ) : (
        <p className="hint">No retry batches required.</p>
      )}
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

function App() {
  const [startDate, setStartDate] = useState(defaultStart)
  const [endDate, setEndDate] = useState(defaultEnd)
  const [posId, setPosId] = useState('')
  const [orderFloor, setOrderFloor] = useState('5525874')
  const [pageLimit, setPageLimit] = useState('100')
  const [status, setStatus] = useState({ tone: 'idle', message: 'Ready to sync' })
  const [summary, setSummary] = useState(null)
  const [health, setHealth] = useState(null)
  const [isLoading, setIsLoading] = useState(false)

  const apiRoot = useMemo(() => API_BASE.replace(/\/$/, ''), [])
  const formattedEndpoint = useMemo(() => `${apiRoot}/sync`, [apiRoot])

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const response = await fetch(`${apiRoot}/health`)
        if (!response.ok) return
        const data = await response.json()
        setHealth(data)
        setStatus((prev) =>
          prev.tone === 'idle'
            ? {
                tone: data.oracle_connected ? 'success' : 'error',
                message: data.oracle_connected
                  ? `Connected to ${data.oracle_target || 'Oracle'}`
                  : 'Oracle not reachable yet',
              }
            : prev
        )
      } catch (_error) {
        // Health check is best-effort; ignore failures.
      }
    }

    checkHealth()
  }, [apiRoot])

  const submit = async (event) => {
    event.preventDefault()
    setIsLoading(true)
    setStatus({ tone: 'busy', message: 'Contacting FastAPI service…' })
    setSummary(null)

    const payload = {
      start_date: new Date(startDate).toISOString(),
      end_date: new Date(endDate).toISOString(),
      order_id_gt: orderFloor ? Number(orderFloor) : undefined,
      limit: pageLimit ? Number(pageLimit) : undefined,
      pos_id: posId ? Number(posId) : undefined,
    }

    try {
      const response = await fetch(formattedEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const detail = await response.text()
        throw new Error(detail || 'Sync failed')
      }

      const data = await response.json()
      const needsRetry = data?.data_integrity_ok === false
      setSummary(data)
      setStatus({
        tone: needsRetry ? 'error' : 'success',
        message: needsRetry
          ? 'Sync finished with retry batches ready.'
          : `Sync completed. Oracle ${data?.oracle?.connected ? 'connected' : 'unreachable'}.`,
      })
    } catch (error) {
      setStatus({ tone: 'error', message: error.message })
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">Odoo → Oracle</p>
        <h1>POS Order Bridge</h1>
        <p className="lede">
          Pull POS orders by date range, then upsert sales, payments, and line items directly into
          Oracle. Keep Merch ops in sync without touching SQL.
        </p>
      </header>

      <main className="grid">
        <section className="panel">
          <div className="panel-head">
            <h2>Run a sync</h2>
            <p>Select a window and trigger the import pipeline.</p>
          </div>
          <form className="form" onSubmit={submit}>
            <label className="field">
              <span>Start date/time</span>
              <input
                type="datetime-local"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                required
              />
            </label>
            <label className="field">
              <span>End date/time</span>
              <input
                type="datetime-local"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                required
              />
            </label>
            <div className="inline">
              <label className="field">
                <span>POS ID</span>
                <input
                  type="number"
                  min="1"
                  value={posId}
                  onChange={(e) => setPosId(e.target.value)}
                  placeholder="e.g. 342"
                />
              </label>
              <label className="field">
                <span>Order ID floor</span>
                <input
                  type="number"
                  min="0"
                  value={orderFloor}
                  onChange={(e) => setOrderFloor(e.target.value)}
                  placeholder="5525874"
                />
              </label>
              <label className="field">
                <span>Page limit</span>
                <input
                  type="number"
                  min="1"
                  value={pageLimit}
                  onChange={(e) => setPageLimit(e.target.value)}
                  placeholder="100"
                />
              </label>
            </div>
            <button type="submit" className="cta" disabled={isLoading}>
              {isLoading ? 'Syncing…' : 'Sync orders'}
            </button>
            <div className={`status ${status.tone}`}>
              <span className="dot" />
              <span>{status.message}</span>
            </div>
          </form>
        </section>

        <section className="panel summary">
          <div className="panel-head">
            <h2>Result</h2>
            <p>Detailed report of what landed in Oracle and what needs a retry batch.</p>
          </div>
          {summary ? (
            <>
              <div className="integrity">
                <span className={`pill ${summary.data_integrity_ok ? 'pill-ok' : 'pill-warn'}`}>
                  {summary.data_integrity_ok ? '100% mapped' : 'Rows pending retry'}
                </span>
                <p className="hint">
                  Oracle target: {summary.oracle?.target || 'not configured'} · User:{' '}
                  {summary.oracle?.user || 'n/a'} · Connection:{' '}
                  {summary.oracle?.connected ? 'connected' : 'not connected'}
                </p>
              </div>
              <div className="stats">
                <div className="stat">
                  <p className="label">Orders fetched</p>
                  <p className="value">{summary.orders_fetched}</p>
                </div>
                <div className="stat">
                  <p className="label">Sales rows</p>
                  <p className="value">{summary.sales_upserted}</p>
                </div>
                <div className="stat">
                  <p className="label">Payments</p>
                  <p className="value">{summary.payments_upserted}</p>
                </div>
                <div className="stat">
                  <p className="label">Line items</p>
                  <p className="value">{summary.line_items_upserted}</p>
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
              <p>No sync yet.</p>
              <p className="hint">Results will land here once you trigger a run.</p>
              {health ? (
                <div className="integrity">
                  <span className={`pill ${health.oracle_connected ? 'pill-ok' : 'pill-warn'}`}>
                    {health.oracle_connected ? 'Oracle connected' : 'Oracle not connected'}
                  </span>
                  <p className="hint">
                    Target: {health.oracle_target || 'not configured'} · User:{' '}
                    {health.oracle_user || 'n/a'}
                  </p>
                </div>
              ) : null}
            </div>
          )}
          <div className="endpoint">
            <p className="label">API endpoint</p>
            <code>{formattedEndpoint}</code>
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
