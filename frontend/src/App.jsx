import { BrowserRouter, Route, Routes } from 'react-router-dom'
import './App.css'
import Nav from './components/Nav'
import DataPage from './pages/DataPage'
import FetchPage from './pages/FetchPage'
import PushPage from './pages/PushPage'

function App() {
  return (
    <BrowserRouter>
      <Nav />
      <Routes>
        <Route path="/" element={<FetchPage />} />
        <Route path="/data" element={<DataPage />} />
        <Route path="/push" element={<PushPage />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
