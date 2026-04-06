import { NavLink } from 'react-router-dom'
import './Nav.css'

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
    </nav>
  )
}
