import { useMemo, useState, type FormEvent } from 'react'
import { api, ApiError } from '../lib/api'
import { useApi, useDebounced } from '../hooks'
import type { Posting } from '../types'
import { Banner, Empty, StateBadge } from '../components/ui'
import PostingDrawer from '../components/PostingDrawer'
import { host, relTime } from '../lib/format'

export default function Postings() {
  const [state, setState] = useState<'open' | 'closed' | ''>('open')
  const [tracked, setTracked] = useState(false)
  const [category, setCategory] = useState('')
  const [q, setQ] = useState('')
  const debounced = useDebounced(q)
  const [open, setOpen] = useState<Posting | null>(null)
  const [addError, setAddError] = useState<string | null>(null)
  const [adding, setAdding] = useState(false)
  const [form, setForm] = useState({ url: '', title: '', company: '' })

  const path = useMemo(() => {
    const p = new URLSearchParams()
    if (state) p.set('state', state)
    if (tracked) p.set('tracked', '1')
    if (category) p.set('category', category)
    if (debounced.trim()) p.set('q', debounced.trim())
    p.set('limit', '300')
    return `/tracker/postings?${p}`
  }, [state, tracked, category, debounced])

  const { data, error, loading, reload } = useApi<Posting[]>(path)

  async function add(e: FormEvent) {
    e.preventDefault()
    setAdding(true)
    setAddError(null)
    try {
      const created = await api.post<Posting>('/tracker/postings', form)
      setForm({ url: '', title: '', company: '' })
      reload()
      setOpen(created)
    } catch (err) {
      setAddError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setAdding(false)
    }
  }

  return (
    <>
      <h1 style={{ marginBottom: 12 }}>Postings</h1>

      <form className="card row" onSubmit={add}>
        <label className="field" style={{ flex: 2 }}>
          Posting URL
          <input type="text" required placeholder="https://…" value={form.url} onChange={(e) => setForm({ ...form, url: e.target.value })} />
        </label>
        <label className="field">
          Company
          <input type="text" required value={form.company} onChange={(e) => setForm({ ...form, company: e.target.value })} />
        </label>
        <label className="field">
          Title
          <input type="text" value={form.title} onChange={(e) => setForm({ ...form, title: e.target.value })} />
        </label>
        <button className="primary" disabled={adding} style={{ alignSelf: 'flex-end' }}>
          {adding ? 'Adding…' : 'Track posting'}
        </button>
      </form>
      {addError && <Banner kind="error">{addError}</Banner>}

      <div className="row" style={{ margin: '12px 0' }}>
        <input type="search" placeholder="Search title or company…" value={q} onChange={(e) => setQ(e.target.value)} style={{ maxWidth: 280 }} />
        <select value={state} onChange={(e) => setState(e.target.value as 'open' | 'closed' | '')} style={{ width: 'auto' }}>
          <option value="open">Open</option>
          <option value="closed">Closed</option>
          <option value="">All states</option>
        </select>
        <select value={category} onChange={(e) => setCategory(e.target.value)} style={{ width: 'auto' }}>
          <option value="">All categories</option>
          <option value="Software">Software</option>
          <option value="AI/ML/Data">AI/ML/Data</option>
          <option value="Quant">Quant</option>
          <option value="Hardware">Hardware</option>
          <option value="Product">Product</option>
        </select>
        <label className="row small" style={{ gap: 6 }}>
          <input type="checkbox" checked={tracked} onChange={(e) => setTracked(e.target.checked)} style={{ width: 'auto' }} />
          Tracked only
        </label>
        <span className="spacer" />
        <span className="muted small">{data?.length ?? 0} shown</span>
      </div>

      {error && <Banner kind="error">{error}</Banner>}
      {loading && !data && <p className="muted">Loading…</p>}
      {data && data.length === 0 && <Empty>Nothing matches these filters.</Empty>}

      {!!data?.length && (
        <div className="card scroll" style={{ padding: 0 }}>
          <table>
            <thead>
              <tr>
                <th>Company</th><th>Title</th><th>Location</th><th>Source</th><th>Seen</th><th></th>
              </tr>
            </thead>
            <tbody>
              {data.map((p) => (
                <tr key={p.id} onClick={() => setOpen(p)} style={{ cursor: 'pointer' }}>
                  <td><b>{p.company}</b></td>
                  <td>
                    {p.title}
                    {p.tracked && <> <span className="badge">tracked</span></>}
                    {p.state === 'closed' && <> <StateBadge state="closed" /></>}
                  </td>
                  <td className="muted small">{p.location}</td>
                  <td className="muted small">{p.source === 'manual' ? host(p.url) : p.source}</td>
                  <td className="muted small">{relTime(p.first_seen_at)}</td>
                  <td><a href={p.url} target="_blank" rel="noreferrer noopener" onClick={(e) => e.stopPropagation()}>↗</a></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {open && <PostingDrawer posting={open} onClose={() => setOpen(null)} onChanged={reload} />}
    </>
  )
}
