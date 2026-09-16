import { useState } from 'react'
import { api } from '../lib/api'
import type { Posting } from '../types'
import { Drawer, StateBadge } from './ui'
import FitPanel from './FitPanel'
import { host, shortDate } from '../lib/format'

export default function PostingDrawer({
  posting,
  onClose,
  onChanged,
}: {
  posting: Posting
  onClose: () => void
  onChanged?: () => void
}) {
  const [tracked, setTracked] = useState(posting.tracked)
  const [applied, setApplied] = useState(false)
  const [appliedOn, setAppliedOn] = useState(new Date().toISOString().slice(0, 10))
  const [resumeRef, setResumeRef] = useState('')
  const [busy, setBusy] = useState(false)

  async function toggleTracked() {
    const next = !tracked
    setTracked(next)
    await api.patch(`/tracker/postings/${posting.id}`, { tracked: next })
    onChanged?.()
  }

  async function record() {
    setBusy(true)
    try {
      await api.post('/tracker/applications', { posting_id: posting.id, applied_on: appliedOn, resume_ref: resumeRef })
      setApplied(true)
      setTracked(true)
      onChanged?.()
    } finally {
      setBusy(false)
    }
  }

  return (
    <Drawer
      onClose={onClose}
      title={
        <>
          <h2 style={{ marginBottom: 2 }}>{posting.title}</h2>
          <div className="muted small">
            {posting.company}
            {posting.location && ` · ${posting.location}`} · <StateBadge state={posting.state} />
          </div>
        </>
      }
    >
      <div className="row">
        <a href={posting.url} target="_blank" rel="noreferrer noopener">Open posting ({host(posting.url)}) ↗</a>
        <span className="spacer" />
        <button onClick={toggleTracked}>{tracked ? 'Untrack' : 'Track'}</button>
      </div>
      <p className="muted small" style={{ marginTop: 6 }}>
        via {posting.source}
        {posting.posted_at && ` · posted ${shortDate(posting.posted_at)}`}
        {` · first seen ${shortDate(posting.first_seen_at)}`}
        {posting.closed_at && ` · closed ${shortDate(posting.closed_at)}`}
      </p>

      <section style={{ marginTop: 14 }}>
        <h3>Record an application</h3>
        {applied ? (
          <p className="muted small">Recorded. It now shows on the Applications page.</p>
        ) : (
          <div className="row" style={{ marginTop: 6 }}>
            <label className="field">
              Applied on
              <input type="date" value={appliedOn} onChange={(e) => setAppliedOn(e.target.value)} />
            </label>
            <label className="field">
              Resume used (optional)
              <input type="text" placeholder="Tailored/Company - Role" value={resumeRef} onChange={(e) => setResumeRef(e.target.value)} />
            </label>
            <button className="primary" onClick={record} disabled={busy} style={{ alignSelf: 'flex-end' }}>
              {busy ? 'Saving…' : 'Applied'}
            </button>
          </div>
        )}
      </section>

      <FitPanel postingId={posting.id} />
    </Drawer>
  )
}
