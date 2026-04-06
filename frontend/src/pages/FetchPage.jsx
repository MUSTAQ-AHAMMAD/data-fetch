import { useEffect, useMemo, useRef, useState } from 'react'

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

export default function FetchPage() {
  const [startDate, setStartDate] = useState(defaultStart)
  const [endDate, setEndDate] = useState(defaultEnd)
  const [posId, setPosId] = useState('')
  const [companyId, setCompanyId] = useState('')
  const [orderFloor, setOrderFloor] = useState('')
  const [pageLimit, setPageLimit] = useState('100')
  const [syncStatus, setSyncStatus] = useState({ tone: 'idle', message: 'Ready to sync' })
  const [summary, setSummary] = useState(null)
  const [health, setHealth] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const abortControllerRef = useRef(null)

  const apiRoot = useMemo(() => API_BASE.replace(/\/$/, ''), [])

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

  const submit = async (event) => {
    event.preventDefault()
    setIsLoading(true)
    setSyncStatus({ tone: 'busy', message: 'Fetching from Odoo…' })
    setSummary(null)

    const controller = new AbortController()
    abortControllerRef.current = controller

    const payload = {
      start_date: new Date(startDate).toISOString(),
      end_date: new Date(endDate).toISOString(),
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
          </form>
        </section>

        <section className="panel summary">
          <div className="panel-head">
            <h2>Result</h2>
            <p>Rows stored in local database. Use the Push page to send to Oracle.</p>
          </div>
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
