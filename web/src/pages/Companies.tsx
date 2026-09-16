import { useState, type FormEvent } from 'react'
import { api, ApiError } from '../lib/api'
import { useApi } from '../hooks'
import type { Company } from '../types'
import { Badge, Banner, Empty } from '../components/ui'

export default function Companies() {
  const { data, error, loading, reload } = useApi<Company[]>('/tracker/companies?followed=1')
  const [form, setForm] = useState({ name: '', board_url: '' })
  const [busy, setBusy] = useState(false)
  const [addError, setAddError] = useState<string | null>(null)

  async function follow(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setAddError(null)
    try {
      await api.post('/tracker/companies', form)
      setForm({ name: '', board_url: '' })
      reload()
    } catch (err) {
      setAddError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function unfollow(c: Company) {
    await api.patch(`/tracker/companies/${c.id}`, { followed: false })
    reload()
  }

  return (
    <>
      <h1 style={{ marginBottom: 12 }}>Followed companies</h1>

      <form className="card row" onSubmit={follow}>
        <label className="field" style={{ flex: 2 }}>
          Board URL
          <input
            type="text"
            required
            placeholder="https://boards.greenhouse.io/stripe · jobs.lever.co/ramp · jobs.ashbyhq.com/vanta"
            value={form.board_url}
            onChange={(e) => setForm({ ...form, board_url: e.target.value })}
          />
        </label>
        <label className="field">
          Name (optional)
          <input type="text" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
        </label>
        <button className="primary" disabled={busy} style={{ alignSelf: 'flex-end' }}>
          {busy ? 'Following…' : 'Follow'}
        </button>
      </form>
      {addError && <Banner kind="error">{addError}</Banner>}
      <p className="muted small" style={{ margin: '8px 0 12px' }}>
        Followed boards are polled directly each sweep, which is fresher than the shared feed. Everything else still arrives through the feed.
      </p>

      {error && <Banner kind="error">{error}</Banner>}
      {loading && !data && <p className="muted">Loading…</p>}
      {data && data.length === 0 && <Empty>Not following any company boards yet.</Empty>}

      {!!data?.length && (
        <div className="card scroll" style={{ padding: 0 }}>
          <table>
            <thead><tr><th>Company</th><th>Board</th><th>Status</th><th></th></tr></thead>
            <tbody>
              {data.map((c) => (
                <tr key={c.id}>
                  <td><b>{c.name}</b></td>
                  <td className="muted small"><Badge>{c.board_type}</Badge> {c.board_id}</td>
                  <td>{c.board_error ? <Badge kind="bad">{c.board_error}</Badge> : <Badge kind="good">ok</Badge>}</td>
                  <td><button className="ghost small" onClick={() => unfollow(c)}>Unfollow</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}
