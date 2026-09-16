import { useState, type FormEvent } from 'react'
import { api, ApiError } from '../lib/api'
import { Banner } from '../components/ui'

export default function Login({ loginConfigured, onAuthenticated }: { loginConfigured: boolean; onAuthenticated: () => void }) {
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await api.post('/auth/login', { password })
      onAuthenticated()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="login">
      <form className="card col" onSubmit={submit}>
        <h1>dispatch</h1>
        {!loginConfigured && (
          <Banner kind="error">
            No dashboard password is set on the server. Set <code>DASHBOARD_PASSWORD</code> and restart.
          </Banner>
        )}
        {error && <Banner kind="error">{error}</Banner>}
        <label className="field">
          Password
          <input
            type="password"
            value={password}
            autoFocus
            autoComplete="current-password"
            onChange={(e) => setPassword(e.target.value)}
          />
        </label>
        <button className="primary" disabled={busy || !password}>
          {busy ? 'Signing in…' : 'Sign in'}
        </button>
      </form>
    </div>
  )
}
