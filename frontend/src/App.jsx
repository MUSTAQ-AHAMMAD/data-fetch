import { useMemo, useState } from 'react'
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

function App() {
  const [startDate, setStartDate] = useState(defaultStart)
  const [endDate, setEndDate] = useState(defaultEnd)
  const [orderFloor, setOrderFloor] = useState('5525874')
  const [pageLimit, setPageLimit] = useState('100')
  const [status, setStatus] = useState({ tone: 'idle', message: 'Ready to sync' })
  const [summary, setSummary] = useState(null)
  const [isLoading, setIsLoading] = useState(false)

  const formattedEndpoint = useMemo(() => `${API_BASE.replace(/\/$/, '')}/sync`, [])

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
      setSummary(data)
      setStatus({ tone: 'success', message: 'Sync completed. Oracle updated.' })
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
            <p>Counts returned by the FastAPI endpoint.</p>
          </div>
          {summary ? (
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
          ) : (
            <div className="placeholder">
              <p>No sync yet.</p>
              <p className="hint">Results will land here once you trigger a run.</p>
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
