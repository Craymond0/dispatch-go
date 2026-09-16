import { useState } from 'react'
import { api } from '../lib/api'
import { useApi } from '../hooks'
import type { Digest as DigestData, Posting, Status } from '../types'
import { Badge, Banner, Empty, Stat } from '../components/ui'
import { host, relTime, shortDate } from '../lib/format'
import PostingDrawer from '../components/PostingDrawer'

function PostingList({ items, onOpen }: { items: Posting[] | null; onOpen: (p: Posting) => void }) {
  if (!items?.length) return null
  return (
    <ul style={{ margin: '4px 0 0', paddingLeft: 18 }}>
      {items.map((p) => (
        <li key={p.id} style={{ marginBottom: 4 }}>
          <a href="#" onClick={(e) => { e.preventDefault(); onOpen(p) }}>
            <b>{p.company}</b> — {p.title}
          </a>{' '}
          <span className="muted small">
            {p.location && `${p.location} · `}
            {host(p.url)}
            {p.posted_at && ` · posted ${shortDate(p.posted_at)}`}
          </span>
        </li>
      ))}
    </ul>
  )
}

export default function Digest() {
  const status = useApi<Status>('/tracker/status')
  const digest = useApi<DigestData | { digest: null }>('/tracker/digest')
  const [open, setOpen] = useState<Posting | null>(null)
  const [sweeping, setSweeping] = useState(false)

  const d = digest.data && 'sweep_id' in digest.data ? digest.data : null
  const s = status.data

  async function sweep() {
    setSweeping(true)
    try {
      await api.post('/tracker/sweep')
      setTimeout(() => { status.reload(); digest.reload(); setSweeping(false) }, 1500)
    } catch {
      setSweeping(false)
    }
  }

  const nothing = d && !d.new?.length && !d.closed?.length && !d.changed?.length && !d.follow_ups?.length

  return (
    <>
      <div className="row" style={{ marginBottom: 12 }}>
        <h1>Digest</h1>
        <span className="spacer" />
        {s?.next_sweep_at && <span className="muted small">next sweep {relTime(s.next_sweep_at)}</span>}
        <button onClick={sweep} disabled={sweeping}>{sweeping ? 'Sweeping…' : 'Sweep now'}</button>
      </div>

      {s && (
        <div className="stats" style={{ marginBottom: 12 }}>
          <Stat label="open postings" value={s.open_postings} />
          <Stat label="tracked" value={s.tracked_postings} />
          <Stat label="companies followed" value={s.followed_companies} />
          <Stat label="active applications" value={s.active_applications} />
          <Stat label="jobs queued" value={s.jobs_queued} />
        </div>
      )}

      {!s?.resume_stored && (
        <Banner>No master resume stored yet. Add it under Settings to enable fit analysis.</Banner>
      )}

      {digest.loading && <p className="muted">Loading…</p>}
      {!d && !digest.loading && <Empty>No digest yet. Run a sweep to populate it.</Empty>}

      {d && (
        <>
          {!!d.failed_sources?.length && (
            <Banner kind="error">
              <b>{d.failed_sources.length} source(s) failed this sweep.</b> Results below are partial.
              <ul style={{ margin: '6px 0 0', paddingLeft: 18 }}>
                {d.failed_sources.map((f) => <li key={f}><code>{f}</code></li>)}
              </ul>
            </Banner>
          )}
          {nothing && <Empty>Nothing changed since the previous digest.</Empty>}

          {!!d.follow_ups?.length && (
            <div className="card">
              <h2>Waiting on you <Badge kind="warn">{d.follow_ups.length}</Badge></h2>
              <ul style={{ margin: '4px 0 0', paddingLeft: 18 }}>
                {d.follow_ups.map((f) => (
                  <li key={f.application_id}>
                    <b>{f.company}</b> — {f.title} · <Badge>{f.status}</Badge>{' '}
                    <span className="muted small">quiet {f.days_quiet} days</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
          {!!d.new?.length && (
            <div className="card">
              <h2>New postings <Badge kind="good">{d.new.length}</Badge></h2>
              <PostingList items={d.new} onOpen={setOpen} />
            </div>
          )}
          {!!d.closed?.length && (
            <div className="card">
              <h2>Closed <Badge kind="bad">{d.closed.length}</Badge></h2>
              <PostingList items={d.closed} onOpen={setOpen} />
            </div>
          )}
          {!!d.changed?.length && (
            <div className="card">
              <h2>Changed <Badge>{d.changed.length}</Badge></h2>
              <PostingList items={d.changed} onOpen={setOpen} />
            </div>
          )}
          <p className="muted small" style={{ marginTop: 12 }}>
            Sweep #{d.sweep_id} · {relTime(d.at)}
          </p>
        </>
      )}

      {open && <PostingDrawer posting={open} onClose={() => setOpen(null)} onChanged={() => { digest.reload(); status.reload() }} />}
    </>
  )
}
