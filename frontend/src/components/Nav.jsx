import { useEffect, useState } from 'react'
import { NavLink } from 'react-router-dom'
import './Nav.css'

function LiveClock() {
  const [now, setNow] = useState(new Date())
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  const date = now.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
  const time = now.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  return (
    <div className="nav-clock">
      <span className="nav-clock-date">{date}</span>
      <span className="nav-clock-time">{time}</span>
    </div>
  )
}

export default function Nav() {
  return (
    <nav className="main-nav">
      <div className="nav-brand">POS Bridge</div>
      <ul className="nav-links">
        <li>
          <NavLink to="/" end className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
            Fetch
          </NavLink>
        </li>
        <li>
          <NavLink to="/data" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
            Local Data
          </NavLink>
        </li>
        <li>
          <NavLink to="/push" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
            Push to Oracle
          </NavLink>
        </li>
      </ul>
      <LiveClock />
    </nav>
  )
}
