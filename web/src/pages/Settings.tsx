import { useEffect, useState } from 'react'
import { api, ApiError } from '../lib/api'
import { useApi } from '../hooks'
import type { Status } from '../types'
import { Badge, Banner } from '../components/ui'
import { relTime } from '../lib/format'

interface Resume { text: string; chars: number; updated_at: string }

export default function Settings() {
  const status = useApi<Status>('/tracker/status')
  const resume = useApi<Resume>('/tracker/resume')
  const [text, setText] = useState('')
  const [saved, setSaved] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    if (resume.data) setText(resume.data.text)
  }, [resume.data])

  async function save() {
    setBusy(true)
    setError(null)
    setSaved(false)
    try {
      await api.put('/tracker/resume', { text })
      setSaved(true)
      resume.reload()
      status.reload()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  const s = status.data
  const dirty = resume.data ? text !== resume.data.text : text.length > 0

  return (
    <>
      <h1 style={{ marginBottom: 12 }}>Settings</h1>

      <div className="card">
        <h2>Server</h2>
        <div className="row small" style={{ marginTop: 8 }}>
          <span>Fit analysis: {s?.llm_configured ? <Badge kind="good">ANTHROPIC_API_KEY set</Badge> : <Badge kind="bad">not configured</Badge>}</span>
          <span>Email digests: {s?.email_configured ? <Badge kind="good">configured</Badge> : <Badge>off</Badge>}</span>
          {s?.sweep_interval_seconds && <span className="muted">sweep every {Math.round(s.sweep_interval_seconds / 3600)}h</span>}
          {s?.next_sweep_at && <span className="muted">next {relTime(s.next_sweep_at)}</span>}
          {s?.last_digest_at && <span className="muted">last digest {relTime(s.last_digest_at)}</span>}
        </div>
      </div>

      <div className="card">
        <div className="row">
          <h2>Master resume</h2>
          <span className="spacer" />
          {resume.data?.updated_at && <span className="muted small">updated {relTime(resume.data.updated_at)}</span>}
          <span className="muted small">{text.length.toLocaleString()} chars</span>
        </div>
        <p className="muted small" style={{ margin: '6px 0 8px' }}>
          Plain text. This is the only thing the fit analysis is allowed to treat as true about you, so paste the full
          resume rather than a summary. Saving clears cached reports, since they were grounded in the previous text.
        </p>
        {error && <Banner kind="error">{error}</Banner>}
        {saved && !dirty && <Banner>Saved.</Banner>}
        <textarea rows={18} value={text} onChange={(e) => { setText(e.target.value); setSaved(false) }} placeholder="Paste your master resume as plain text…" />
        <div className="row" style={{ marginTop: 8 }}>
          <button className="primary" onClick={save} disabled={busy || !dirty || text.trim().length < 200}>
            {busy ? 'Saving…' : 'Save resume'}
          </button>
          {text.trim().length < 200 && text.length > 0 && <span className="muted small">needs at least 200 characters</span>}
          {dirty && resume.data && <button className="ghost" onClick={() => setText(resume.data!.text)}>Discard changes</button>}
        </div>
      </div>
    </>
  )
}
