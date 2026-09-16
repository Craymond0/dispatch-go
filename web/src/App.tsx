import { useCallback, useEffect, useState } from 'react'
import { NavLink, Navigate, Route, Routes } from 'react-router-dom'
import { api, setUnauthorizedHandler } from './lib/api'
import Login from './pages/Login'
import Digest from './pages/Digest'
import Postings from './pages/Postings'
import Applications from './pages/Applications'
import Companies from './pages/Companies'
import Queue from './pages/Queue'
import Settings from './pages/Settings'

interface Me {
  authenticated: boolean
  method: string
  login_configured: boolean
}

export default function App() {
  const [me, setMe] = useState<Me | null>(null)

  const check = useCallback(() => {
    api
      .get<Me>('/auth/me')
      .then(setMe)
      .catch(() => setMe({ authenticated: false, method: '', login_configured: true }))
  }, [])

  useEffect(() => {
    setUnauthorizedHandler(() => setMe((m) => (m ? { ...m, authenticated: false } : m)))
    check()
  }, [check])

  if (!me) return <div className="login"><p className="muted">Loading…</p></div>
  if (!me.authenticated) return <Login loginConfigured={me.login_configured} onAuthenticated={check} />

  const logout = () => api.post('/auth/logout').then(check).catch(check)

  return (
    <div className="app">
      <header className="topbar">
        <span className="brand">dispatch</span>
        <nav className="nav">
          <NavLink to="/" end>Digest</NavLink>
          <NavLink to="/postings">Postings</NavLink>
          <NavLink to="/applications">Applications</NavLink>
          <NavLink to="/companies">Companies</NavLink>
          <NavLink to="/queue">Queue</NavLink>
          <NavLink to="/settings">Settings</NavLink>
        </nav>
        <span className="spacer" />
        <button className="ghost small" onClick={logout}>Sign out</button>
      </header>
      <main className="main">
        <Routes>
          <Route path="/" element={<Digest />} />
          <Route path="/postings" element={<Postings />} />
          <Route path="/applications" element={<Applications />} />
          <Route path="/companies" element={<Companies />} />
          <Route path="/queue" element={<Queue />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  )
}
