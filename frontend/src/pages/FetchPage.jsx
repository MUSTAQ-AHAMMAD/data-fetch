import { useEffect, useMemo, useRef, useState } from 'react'

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

// Return a date as YYYY-MM-DD (local time, no UTC conversion), offset by `offsetDays`.
const localDate = (offsetDays = 0) => {
  const d = new Date()
  d.setDate(d.getDate() + offsetDays)
  const yyyy = d.getFullYear()
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const dd = String(d.getDate()).padStart(2, '0')
  return `${yyyy}-${mm}-${dd}`
}

const renderTableReport = (title, report) => {
  if (!report) return null
  const missingCount = report.missing_row_ids?.length || 0
  return (
    <div className="table-report">
      <div className="table-report-head">
        <p className="label">{title}</p>
        <span className={`pill ${missingCount ? 'pill-warn' : 'pill-ok'}`}>
          {missingCount ? `${missingCount} rows pending` : 'Stored'}
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

const TABLE_LABELS = { sales: 'Sales', payments: 'Payments', line_items: 'Line Items' }
const TABLE_ORDER = ['sales', 'payments', 'line_items']

function DbInsertProgress({ progress }) {
  const { store_total, store_completed, store_current_table, status } = progress
  if (store_total == null) return null

  const storePct = store_total > 0 ? Math.min(100, Math.round((store_completed / store_total) * 100)) : 0
  const storeRemaining = Math.max(0, store_total - store_completed)
  const isDone = status === 'done'

  // Determine per-table state based on which table is currently active.
  // Guard against unexpected table names from the backend by only using known values.
  const knownTable = TABLE_ORDER.includes(store_current_table) ? store_current_table : null
  const currentIdx = knownTable ? TABLE_ORDER.indexOf(knownTable) : (isDone ? TABLE_ORDER.length : -1)

  return (
    <div className="db-insert-progress">
      <div className="db-insert-header">
        <span className="db-insert-title">
          {isDone ? '✓ Database insert complete' : `💾 Inserting into database…`}
        </span>
        <span className="db-insert-pct">{storePct}%</span>
      </div>

      <div className="fetch-progress-bar-track" style={{ marginBottom: '10px' }}>
        <div
          className={`fetch-progress-bar-fill ${isDone ? 'fill-done' : ''}`}
          style={{ width: `${storePct}%` }}
        />
      </div>

      <div className="db-insert-stats">
        <div className="fp-stat">
          <span className="fp-stat-label">Total records</span>
          <span className="fp-stat-value">{store_total.toLocaleString()}</span>
        </div>
        <div className="fp-stat">
          <span className="fp-stat-label">Completed</span>
          <span className="fp-stat-value fp-fetched">{store_completed.toLocaleString()}</span>
        </div>
        <div className="fp-stat">
          <span className="fp-stat-label">Remaining</span>
          <span className="fp-stat-value fp-pending">{storeRemaining.toLocaleString()}</span>
        </div>
      </div>

      <div className="db-insert-tables">
        {TABLE_ORDER.map((key, idx) => {
          const isActive = key === knownTable
          const isDoneTable = idx < currentIdx
          const isPending = idx > currentIdx
          return (
            <div key={key} className={`db-table-row ${isActive ? 'db-table-active' : isDoneTable ? 'db-table-done' : 'db-table-pending'}`}>
              <span className="db-table-dot">
                {isDoneTable ? '✓' : isActive ? '⟳' : '○'}
              </span>
              <span className="db-table-name">{TABLE_LABELS[key]}</span>
              <span className="db-table-status">
                {isDoneTable ? 'Done' : isActive ? 'Inserting…' : 'Waiting'}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function FetchProgressBar({ progress, elapsedSeconds }) {
  if (!progress || progress.status === 'idle') return null

  const { status, fetched, total } = progress
  const pct = total != null && total > 0 ? Math.min(100, Math.round((fetched / total) * 100)) : null
  const pending = total != null ? Math.max(0, total - fetched) : null

  const isError = status === 'error'
  const isStoring = status === 'storing'
  const isDone = status === 'done'
  // Show indeterminate animation when fetching but total is not yet known
  const isIndeterminate = !isError && !isStoring && !isDone && pct == null

  const barFill = isStoring || isDone ? 100 : (pct ?? 0)

  return (
    <div className={`fetch-progress ${isError ? 'fetch-progress-error' : ''}`}>
      <div className="fetch-progress-header">
        <span className="fetch-progress-label">
          {isError
            ? '⚠ Error during fetch'
            : isStoring
            ? '⟳ Fetching from Odoo — complete'
            : isDone
            ? '✓ Complete'
            : '⟳ Fetching from Odoo…'}
        </span>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          {elapsedSeconds != null && (
            <span className={`elapsed-badge ${isDone || isError ? 'elapsed-done' : 'elapsed-running'}`}>
              ⏱ {formatElapsed(elapsedSeconds)}
            </span>
          )}
          {pct != null && !isStoring && !isDone && (
            <span className="fetch-progress-pct">{pct}%</span>
          )}
        </div>
      </div>
      <div className="fetch-progress-bar-track">
        <div
          className={`fetch-progress-bar-fill ${isError ? 'fill-error' : isDone ? 'fill-done' : isIndeterminate ? 'fill-indeterminate' : ''}`}
          style={isIndeterminate ? {} : { width: `${barFill}%` }}
        />
      </div>
      <div className="fetch-progress-stats">
        <div className="fp-stat">
          <span className="fp-stat-label">Total</span>
          <span className="fp-stat-value">{total != null ? total.toLocaleString() : '—'}</span>
        </div>
        <div className="fp-stat">
          <span className="fp-stat-label">Fetched</span>
          <span className="fp-stat-value fp-fetched">{fetched.toLocaleString()}</span>
        </div>
        <div className="fp-stat">
          <span className="fp-stat-label">Pending</span>
          <span className="fp-stat-value fp-pending">{pending != null ? pending.toLocaleString() : '—'}</span>
        </div>
      </div>
      {(isStoring || isDone) && <DbInsertProgress progress={progress} />}
      {isError && progress.error && (
        <p className="warn" style={{ marginTop: '6px' }}>{progress.error}</p>
      )}
    </div>
  )
}

const ALL_TABLES = ['sales', 'payments', 'line_items']

function ClearAllPanel({ apiRoot }) {
  const [clearing, setClearing] = useState(false)
  const [confirmed, setConfirmed] = useState(false)
  const [clearResult, setClearResult] = useState(null)
  const [clearError, setClearError] = useState(null)

  const handleClearAll = async () => {
    if (!confirmed) {
      setConfirmed(true)
      return
    }
    setClearing(true)
    setClearResult(null)
    setClearError(null)
    setConfirmed(false)
    try {
      const res = await fetch(`${apiRoot}/local/clear`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tables: ALL_TABLES }),
      })
      if (!res.ok) {
        const text = await res.text()
        let detail = text
        try { detail = JSON.parse(text)?.detail || text } catch (_) {}
        throw new Error(`Failed to clear database: ${detail}`)
      }
      const data = await res.json()
      const total = Object.values(data.deleted).reduce((a, b) => a + b, 0)
      setClearResult(total)
    } catch (err) {
      setClearError(err.message)
    } finally {
      setClearing(false)
    }
  }

  return (
    <div className="clear-all-panel">
      <div className="clear-all-head">
        <span className="clear-all-title">🗑 Clear entire database</span>
        <span className="clear-all-desc">
          Wipe all sales, payments, and line item records from the local database before fetching to get a clean picture.
        </span>
      </div>
      {clearError && <p className="warn" style={{ margin: 0 }}>{clearError}</p>}
      {clearResult != null && (
        <p className="clear-all-ok">
          ✓ Cleared — {clearResult} row{clearResult !== 1 ? 's' : ''} deleted from all tables.
        </p>
      )}
      <button
        type="button"
        className={`cta cta-sm${confirmed ? ' cta-danger' : ' cta-ghost'}`}
        onClick={handleClearAll}
        disabled={clearing}
        style={{ marginTop: 0, alignSelf: 'flex-start' }}
      >
        {clearing ? 'Clearing…' : confirmed ? '⚠ Confirm — delete all data' : 'Clear all data'}
      </button>
      {confirmed && !clearing && (
        <p className="warn" style={{ margin: 0, fontSize: '0.8rem' }}>
          Click again to confirm. This cannot be undone.
        </p>
      )}
    </div>
  )
}

export default function FetchPage() {
  const [startDate, setStartDate] = useState(() => localDate(-3))
  const [startTime, setStartTime] = useState('00:00:00')
  const [endDate, setEndDate] = useState(() => localDate())
  const [endTime, setEndTime] = useState('23:59:59')
  const [posId, setPosId] = useState('')
  const [companyId, setCompanyId] = useState('')
  const [orderFloor, setOrderFloor] = useState('')
  const [pageLimit, setPageLimit] = useState('100')
  const [syncStatus, setSyncStatus] = useState({ tone: 'idle', message: 'Ready to sync' })
  const [summary, setSummary] = useState(null)
  const [health, setHealth] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [progress, setProgress] = useState(null)
  const [elapsed, setElapsed] = useState(null)
  const [timelineEvents, setTimelineEvents] = useState([])
  const abortControllerRef = useRef(null)
  const progressIntervalRef = useRef(null)
  const timerRef = useRef(null)
  const prevStatusRef = useRef(null)

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

  // Track timeline events based on progress status changes
  useEffect(() => {
    if (!progress) return
    const status = progress.status
    if (status === prevStatusRef.current) return
    prevStatusRef.current = status
    const now = new Date().toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
    if (status === 'storing') {
      setTimelineEvents((prev) => [
        ...prev,
        { label: '💾 Storing to local database', time: now, active: true, done: false },
      ])
    } else if (status === 'done') {
      setTimelineEvents((prev) => {
        const updated = prev.map((e) => ({ ...e, active: false, done: true }))
        return [...updated, { label: '✓ Fetch complete', time: now, active: false, done: true }]
      })
    } else if (status === 'error') {
      setTimelineEvents((prev) => [
        ...prev,
        { label: '⚠ Error during fetch', time: now, active: false, done: true, error: true },
      ])
    }
  }, [progress])

  // Cleanup timer on unmount
  useEffect(() => () => stopTimer(), [])

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const response = await fetch(`${apiRoot}/health`)
        if (!response.ok) return
        const data = await response.json()
        setHealth(data)
        setSyncStatus((prev) =>
          prev.tone === 'idle'
            ? {
                tone: data.odoo_ready ? 'success' : 'error',
                message: data.odoo_ready ? 'Odoo API key configured. Ready to fetch.' : 'Odoo API key not configured.',
              }
            : prev
        )
      } catch (_error) {
        // Health check is best-effort; ignore failures.
      }
    }
    checkHealth()
  }, [apiRoot])

  const startProgressPolling = (root) => {
    if (progressIntervalRef.current) clearInterval(progressIntervalRef.current)
    progressIntervalRef.current = setInterval(async () => {
      try {
        const res = await fetch(`${root}/sync/progress`)
        if (!res.ok) return
        const data = await res.json()
        setProgress(data)
        if (data.status === 'done' || data.status === 'error') {
          clearInterval(progressIntervalRef.current)
          progressIntervalRef.current = null
        }
      } catch (_err) {
        // best-effort
      }
    }, 800)
  }

  const stopProgressPolling = () => {
    if (progressIntervalRef.current) {
      clearInterval(progressIntervalRef.current)
      progressIntervalRef.current = null
    }
  }

  const submit = async (event) => {
    event.preventDefault()
    setIsLoading(true)
    setSyncStatus({ tone: 'busy', message: 'Fetching from Odoo…' })
    setSummary(null)
    setProgress({ status: 'fetching', fetched: 0, total: null, error: null })
    prevStatusRef.current = null

    const now = new Date().toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
    setTimelineEvents([{ label: '⟳ Fetch started', time: now, active: true, done: false }])
    startTimer()

    const controller = new AbortController()
    abortControllerRef.current = controller

    startProgressPolling(apiRoot)

    const payload = {
      start_date: `${startDate} ${startTime}`,
      end_date: `${endDate} ${endTime}`,
      order_id_gt: orderFloor.trim() !== '' ? Number(orderFloor) : undefined,
      limit: pageLimit ? Number(pageLimit) : undefined,
      pos_id: posId.trim() !== '' ? Number(posId) : undefined,
      company_id: companyId.trim() !== '' ? Number(companyId) : undefined,
    }

    try {
      const response = await fetch(`${apiRoot}/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: controller.signal,
      })

      if (response.status === 409) {
        const detail = await response.json().catch(() => ({}))
        setSyncStatus({ tone: 'idle', message: detail?.detail || 'Sync cancelled.' })
        return
      }

      if (!response.ok) {
        const detail = await response.text()
        throw new Error(detail || 'Sync failed')
      }

      const data = await response.json()
      setSummary(data)
      setSyncStatus({
        tone: 'success',
        message: `Fetched ${data.orders_fetched} orders and stored to local database.`,
      })
    } catch (error) {
      if (error.name === 'AbortError') {
        setSyncStatus({ tone: 'idle', message: 'Sync cancelled.' })
      } else {
        setSyncStatus({ tone: 'error', message: error.message })
      }
    } finally {
      stopProgressPolling()
      stopTimer()
      // Do a final progress poll to show the terminal state
      try {
        const res = await fetch(`${apiRoot}/sync/progress`)
        if (res.ok) setProgress(await res.json())
      } catch (_err) { /* best-effort */ }
      abortControllerRef.current = null
      setIsLoading(false)
    }
  }

  const cancelSync = async () => {
    try {
      await fetch(`${apiRoot}/cancel`, { method: 'POST' })
    } catch (_err) {
      // best-effort
    }
    abortControllerRef.current?.abort()
  }

  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">Odoo → Local Database</p>
        <h1>Fetch Orders</h1>
        <p className="lede">
          Pull POS orders from Odoo and store them in the local database. Once stored, you can view
          the data on the <strong>Local Data</strong> page and push it to Oracle when ready.
        </p>
      </header>

      <main className="grid">
        <section className="panel">
          <div className="panel-head">
            <h2>Run a fetch</h2>
            <p>Select a date window and trigger the import pipeline.</p>
          </div>
          <form className="form" onSubmit={submit}>
            <div className="date-time-row">
              <label className="field field-grow">
                <span>Start date</span>
                <input
                  type="date"
                  value={startDate}
                  onChange={(e) => setStartDate(e.target.value)}
                  required
                />
              </label>
              <label className="field field-time">
                <span>Start time</span>
                <input
                  type="text"
                  placeholder="HH:MM:SS"
                  pattern="\d{2}:\d{2}:\d{2}"
                  maxLength={8}
                  value={startTime}
                  onChange={(e) => setStartTime(e.target.value)}
                  required
                />
              </label>
            </div>
            <div className="date-time-row">
              <label className="field field-grow">
                <span>End date</span>
                <input
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                  required
                />
              </label>
              <label className="field field-time">
                <span>End time</span>
                <input
                  type="text"
                  placeholder="HH:MM:SS"
                  pattern="\d{2}:\d{2}:\d{2}"
                  maxLength={8}
                  value={endTime}
                  onChange={(e) => setEndTime(e.target.value)}
                  required
                />
              </label>
            </div>
            <div className="inline">
              <label className="field">
                <span>Order ID floor</span>
                <input
                  type="number"
                  min="0"
                  value={orderFloor}
                  onChange={(e) => setOrderFloor(e.target.value)}
                  placeholder="optional"
                />
              </label>
              <label className="field">
                <span>POS ID</span>
                <input
                  type="number"
                  min="1"
                  value={posId}
                  onChange={(e) => setPosId(e.target.value)}
                  placeholder="optional"
                />
              </label>
              <label className="field">
                <span>Company ID</span>
                <input
                  type="number"
                  min="1"
                  value={companyId}
                  onChange={(e) => setCompanyId(e.target.value)}
                  placeholder="optional"
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
              {isLoading ? 'Fetching…' : 'Fetch & store orders'}
            </button>
            {isLoading && (
              <button type="button" className="cta cta-cancel" onClick={cancelSync}>
                Cancel
              </button>
            )}
            <div className={`status ${syncStatus.tone}`}>
              <span className="dot" />
              <span>{syncStatus.message}</span>
            </div>
            <FetchProgressBar progress={progress} elapsedSeconds={elapsed} />
            <SyncTimeline events={timelineEvents} />
          </form>
        </section>

        <section className="panel summary">
          <div className="panel-head">
            <h2>Result</h2>
            <p>Rows stored in local database. Use the Push page to send to Oracle.</p>
          </div>
          <ClearAllPanel apiRoot={apiRoot} />
          {summary ? (
            <>
              <div className="integrity">
                <span className={`pill ${summary.data_integrity_ok ? 'pill-ok' : 'pill-warn'}`}>
                  {summary.data_integrity_ok ? 'All stored' : 'Some rows missing'}
                </span>
                <p className="hint">
                  Oracle status: {summary.oracle?.connected ? 'connected' : 'not connected'} · Use
                  Push page to upload
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
              <p>No fetch yet.</p>
              <p className="hint">Results will appear here once you trigger a run.</p>
              {health ? (
                <div className="integrity">
                  <span className={`pill ${health.odoo_ready ? 'pill-ok' : 'pill-warn'}`}>
                    {health.odoo_ready ? 'Odoo ready' : 'Odoo API key missing'}
                  </span>
                  <span className={`pill ${health.oracle_connected ? 'pill-ok' : 'pill-warn'}`} style={{ marginLeft: '0.5rem' }}>
                    {health.oracle_connected ? 'Oracle connected' : 'Oracle offline'}
                  </span>
                </div>
              ) : null}
            </div>
          )}
        </section>
      </main>
    </div>
  )
}
