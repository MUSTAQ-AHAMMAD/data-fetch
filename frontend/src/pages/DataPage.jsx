import { useEffect, useMemo, useState } from 'react'
import './DataPage.css'

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

const TABS = [
  { key: 'sales', label: 'Sales', endpoint: '/local/sales' },
  { key: 'payments', label: 'Payments', endpoint: '/local/payments' },
  { key: 'line_items', label: 'Line Items', endpoint: '/local/line_items' },
]

const PAGE_SIZE = 100

function FilterBar({ filters, onChange, onSearch, loading }) {
  return (
    <div className="filter-bar">
      <div className="filter-fields">
        <label className="field">
          <span>Start date/time</span>
          <input
            type="datetime-local"
            value={filters.start_date}
            onChange={(e) => onChange('start_date', e.target.value)}
          />
        </label>
        <label className="field">
          <span>End date/time</span>
          <input
            type="datetime-local"
            value={filters.end_date}
            onChange={(e) => onChange('end_date', e.target.value)}
          />
        </label>
        <label className="field">
          <span>Invoice #</span>
          <input
            type="text"
            value={filters.invoice_number}
            onChange={(e) => onChange('invoice_number', e.target.value)}
            placeholder="search…"
          />
        </label>
        <label className="field">
          <span>Outlet</span>
          <input
            type="text"
            value={filters.outlet_name}
            onChange={(e) => onChange('outlet_name', e.target.value)}
            placeholder="search…"
          />
        </label>
        <label className="field">
          <span>Sync status</span>
          <select value={filters.synced} onChange={(e) => onChange('synced', e.target.value)}>
            <option value="">All</option>
            <option value="false">Pending</option>
            <option value="true">Synced</option>
          </select>
        </label>
      </div>
      <button className="cta cta-sm" onClick={onSearch} disabled={loading}>
        {loading ? 'Loading…' : 'Apply filters'}
      </button>
    </div>
  )
}

function DataTable({ rows, columns }) {
  if (!rows || rows.length === 0) {
    return <p className="hint" style={{ padding: '1.5rem 0' }}>No rows found.</p>
  }
  return (
    <div className="data-table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx} className={row.SYNCED_TO_ORACLE ? 'row-synced' : ''}>
              {columns.map((col) => (
                <td key={col} title={row[col] ?? ''}>
                  {row[col] === null || row[col] === undefined ? (
                    <span className="null-cell">—</span>
                  ) : String(row[col]).length > 30 ? (
                    <span title={row[col]}>{String(row[col]).slice(0, 28)}…</span>
                  ) : (
                    row[col]
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const TABLE_OPTIONS = [
  { key: 'sales', label: 'Sales' },
  { key: 'payments', label: 'Payments' },
  { key: 'line_items', label: 'Line Items' },
]

function ClearRecordsPanel({ apiRoot }) {
  const [clearStart, setClearStart] = useState('')
  const [clearEnd, setClearEnd] = useState('')
  const [selectedTables, setSelectedTables] = useState({ sales: true, payments: true, line_items: true })
  const [clearing, setClearing] = useState(false)
  const [clearResult, setClearResult] = useState(null)
  const [clearError, setClearError] = useState(null)
  const [confirmed, setConfirmed] = useState(false)

  const toggleTable = (key) => {
    setSelectedTables((prev) => ({ ...prev, [key]: !prev[key] }))
    setConfirmed(false)
  }

  const tables = TABLE_OPTIONS.filter((t) => selectedTables[t.key]).map((t) => t.key)

  const handleClear = async () => {
    if (!confirmed) {
      setConfirmed(true)
      return
    }
    setClearing(true)
    setClearResult(null)
    setClearError(null)
    setConfirmed(false)
    try {
      const body = { tables }
      if (clearStart) body.start_date = clearStart
      if (clearEnd) body.end_date = clearEnd
      const res = await fetch(`${apiRoot}/local/clear`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) {
        const text = await res.text()
        let detail = text
        try { detail = JSON.parse(text)?.detail || text } catch (_) {}
        throw new Error(`Failed to clear records: ${detail}`)
      }
      const data = await res.json()
      setClearResult(data.deleted)
    } catch (err) {
      setClearError(err.message)
    } finally {
      setClearing(false)
    }
  }

  const totalDeleted = clearResult ? Object.values(clearResult).reduce((a, b) => a + b, 0) : null

  return (
    <div className="clear-panel">
      <div className="clear-panel-head">
        <h3>🗑 Clear Database Records</h3>
        <p className="clear-panel-desc">
          Permanently delete rows from the local database within a date range.
          Leave dates empty to delete <strong>all</strong> records in the selected tables.
        </p>
      </div>
      <div className="clear-fields">
        <label className="field">
          <span>From date/time</span>
          <input
            type="datetime-local"
            value={clearStart}
            onChange={(e) => { setClearStart(e.target.value); setConfirmed(false) }}
          />
        </label>
        <label className="field">
          <span>To date/time</span>
          <input
            type="datetime-local"
            value={clearEnd}
            onChange={(e) => { setClearEnd(e.target.value); setConfirmed(false) }}
          />
        </label>
        <div className="clear-table-checks">
          <span className="clear-checks-label">Tables</span>
          <div className="clear-checks">
            {TABLE_OPTIONS.map((t) => (
              <label key={t.key} className="check-option">
                <input
                  type="checkbox"
                  checked={selectedTables[t.key]}
                  onChange={() => toggleTable(t.key)}
                />
                {t.label}
              </label>
            ))}
          </div>
        </div>
      </div>
      {clearError && <p className="warn">{clearError}</p>}
      {clearResult && (
        <div className="clear-result">
          <span className="pill pill-warn">
            {totalDeleted} row{totalDeleted !== 1 ? 's' : ''} deleted
          </span>
          <div className="clear-result-detail">
            {Object.entries(clearResult).map(([tbl, cnt]) => (
              <span key={tbl} className="clear-result-item">
                {tbl}: <strong>{cnt}</strong>
              </span>
            ))}
          </div>
        </div>
      )}
      <button
        className={`cta cta-sm ${confirmed ? 'cta-danger' : ''}`}
        onClick={handleClear}
        disabled={clearing || tables.length === 0}
      >
        {clearing ? 'Deleting…' : confirmed ? '⚠ Confirm delete' : 'Delete records'}
      </button>
      {confirmed && !clearing && (
        <p className="warn" style={{ marginTop: '4px' }}>
          Click again to confirm. This cannot be undone.
        </p>
      )}
    </div>
  )
}

const SALES_COLS = [
  'ROW_ID', 'INVOICE_NUMBER', 'OUTLET_NAME', 'REGISTER_NAME', 'SALE_DATE',
  'TOTAL_PRICE', 'TOTAL_TAX', 'TOTAL_PRICE_INCL_TAX', 'CUSTOMER_TYPE',
  'REGION', 'SYNCED_TO_ORACLE', 'FETCHED_AT',
]
const PAYMENTS_COLS = [
  'ROW_ID', 'INVOICE_NUMBER', 'OUTLET_NAME', 'AMOUNT', 'CURRENCY',
  'PAYMENT_TYPE', 'PAYMENT_DATE', 'REGION', 'SYNCED_TO_ORACLE', 'FETCHED_AT',
]
const LINE_ITEMS_COLS = [
  'ROW_ID', 'INVOICE_NUMBER', 'LINE_NUMBER', 'ITEM_NUMBER', 'ITEM_NAME',
  'QUANTITY', 'TOTAL_PRICE', 'TOTAL_TAX', 'TOTAL_DISCOUNT',
  'INV_UPLOAD_QNT_FLAG', 'SALE_DATE', 'SYNCED_TO_ORACLE',
]
const COLS_MAP = {
  sales: SALES_COLS,
  payments: PAYMENTS_COLS,
  line_items: LINE_ITEMS_COLS,
}

const defaultFilters = () => ({
  start_date: '',
  end_date: '',
  invoice_number: '',
  outlet_name: '',
  synced: '',
})

export default function DataPage() {
  const [activeTab, setActiveTab] = useState('sales')
  const [filters, setFilters] = useState(defaultFilters())
  const [result, setResult] = useState({ total: 0, rows: [] })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [offset, setOffset] = useState(0)

  const apiRoot = useMemo(() => API_BASE.replace(/\/$/, ''), [])
  const tab = TABS.find((t) => t.key === activeTab)
  const totalPages = Math.ceil(result.total / PAGE_SIZE)
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

  const fetchData = async (currentOffset = 0) => {
    setLoading(true)
    setError(null)
    const params = new URLSearchParams({ limit: PAGE_SIZE, offset: currentOffset })
    if (filters.start_date) params.set('start_date', filters.start_date)
    if (filters.end_date) params.set('end_date', filters.end_date)
    if (filters.invoice_number) params.set('invoice_number', filters.invoice_number)
    if (filters.outlet_name) params.set('outlet_name', filters.outlet_name)
    if (filters.synced !== '') params.set('synced', filters.synced)
    try {
      const res = await fetch(`${apiRoot}${tab.endpoint}?${params}`)
      if (!res.ok) throw new Error(await res.text())
      const data = await res.json()
      setResult(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    setOffset(0)
    fetchData(0)
    // fetchData is defined inside the component and changes every render, but adding it
    // to deps would cause an infinite loop.  We intentionally re-run only when the active
    // tab changes (and on initial mount), using the current filters captured at that moment.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab])

  const handleSearch = () => {
    setOffset(0)
    fetchData(0)
  }

  const handleFilterChange = (key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value }))
  }

  const goToPage = (page) => {
    const newOffset = (page - 1) * PAGE_SIZE
    setOffset(newOffset)
    fetchData(newOffset)
  }

  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">Local Database</p>
        <h1>Fetched Data</h1>
        <p className="lede">
          Browse orders stored in the local database. Filter by date, invoice, outlet, or sync
          status. Rows marked <em>Synced</em> have already been pushed to Oracle.
        </p>
      </header>

      <main className="data-main">
        <div className="tabs">
          {TABS.map((t) => (
            <button
              key={t.key}
              className={`tab-btn ${activeTab === t.key ? 'active' : ''}`}
              onClick={() => setActiveTab(t.key)}
            >
              {t.label}
            </button>
          ))}
        </div>

        <FilterBar
          filters={filters}
          onChange={handleFilterChange}
          onSearch={handleSearch}
          loading={loading}
        />

        {error && <p className="warn" style={{ padding: '0.75rem' }}>{error}</p>}

        <div className="data-meta">
          <span className="hint">
            {result.total > 0
              ? `${result.total} rows total · showing ${offset + 1}–${Math.min(offset + PAGE_SIZE, result.total)}`
              : 'No data loaded yet. Apply filters and click "Apply filters".'}
          </span>
          {result.total > PAGE_SIZE && (
            <div className="pagination">
              <button
                className="page-btn"
                disabled={currentPage === 1}
                onClick={() => goToPage(currentPage - 1)}
              >
                ‹ Prev
              </button>
              <span className="page-info">
                Page {currentPage} / {totalPages}
              </span>
              <button
                className="page-btn"
                disabled={currentPage === totalPages}
                onClick={() => goToPage(currentPage + 1)}
              >
                Next ›
              </button>
            </div>
          )}
        </div>

        <DataTable rows={result.rows} columns={COLS_MAP[activeTab]} />

        <ClearRecordsPanel apiRoot={apiRoot} />
      </main>
    </div>
  )
}
