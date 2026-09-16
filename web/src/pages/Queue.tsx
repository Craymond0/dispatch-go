import { useEffect, useState } from 'react'
import { useApi, usePoll } from '../hooks'
import { metricsText } from '../lib/api'
import type { Job, Status } from '../types'
import { Banner, Empty, StateBadge, Stat } from '../components/ui'

/** Parse the Prometheus text exposition we serve at /metrics. */
function parseMetrics(text: string): { byType: Record<string, Record<string, number>>; retries: number } {
  const byType: Record<string, Record<string, number>> = {}
  let retries = 0
  for (const line of text.split('\n')) {
    if (line.startsWith('#') || !line.trim()) continue
    const m = /^dispatch_jobs\{type="([^"]+)",state="([^"]+)"\}\s+(\d+)/.exec(line)
    if (m) {
      byType[m[1]] ??= {}
      byType[m[1]][m[2]] = Number(m[3])
      continue
    }
    const r = /^dispatch_retries_total\s+(\d+)/.exec(line)
    if (r) retries = Number(r[1])
  }
  return { byType, retries }
}

const STATES = ['queued', 'running', 'succeeded', 'failed'] as const

export default function Queue() {
  const jobs = useApi<Job[]>('/jobs')
  const status = useApi<Status>('/tracker/status')
  const [metrics, setMetrics] = useState<ReturnType<typeof parseMetrics> | null>(null)
  const [live, setLive] = useState(true)

  const loadMetrics = () => {
    metricsText().then((t) => setMetrics(parseMetrics(t))).catch(() => {})
  }
  useEffect(loadMetrics, [])
  usePoll(() => {
    if (!live) return
    jobs.reload()
    status.reload()
    loadMetrics()
  }, 3000)

  const failed = jobs.data?.filter((j) => j.state === 'failed') ?? []

  return (
    <>
      <div className="row" style={{ marginBottom: 12 }}>
        <h1>Queue</h1>
        <span className="spacer" />
        <label className="row small" style={{ gap: 6 }}>
          <input type="checkbox" checked={live} onChange={(e) => setLive(e.target.checked)} style={{ width: 'auto' }} />
          Live
        </label>
        <button onClick={() => { jobs.reload(); loadMetrics() }}>Refresh</button>
      </div>

      <div className="stats" style={{ marginBottom: 12 }}>
        <Stat label="running" value={status.data?.jobs_running ?? '—'} />
        <Stat label="queued" value={status.data?.jobs_queued ?? '—'} />
        <Stat label="retries (total)" value={metrics?.retries ?? '—'} />
        <Stat label="failed (last 100)" value={failed.length} />
      </div>

      {metrics && Object.keys(metrics.byType).length > 0 && (
        <div className="card scroll">
          <h2 style={{ marginBottom: 8 }}>By job type</h2>
          <table>
            <thead><tr><th>Type</th>{STATES.map((s) => <th key={s}>{s}</th>)}</tr></thead>
            <tbody>
              {Object.entries(metrics.byType).sort().map(([type, counts]) => (
                <tr key={type}>
                  <td><code>{type}</code></td>
                  {STATES.map((s) => (
                    <td key={s} className={counts[s] ? undefined : 'muted'}>{counts[s] ?? 0}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {jobs.error && <Banner kind="error">{jobs.error}</Banner>}
      {jobs.data && jobs.data.length === 0 && <Empty>No jobs yet.</Empty>}

      {!!jobs.data?.length && (
        <div className="card scroll" style={{ padding: 0, marginTop: 12 }}>
          <table>
            <thead><tr><th>#</th><th>Type</th><th>State</th><th>Attempts</th><th>Waiting on</th><th>Error</th></tr></thead>
            <tbody>
              {jobs.data.map((j) => (
                <tr key={j.id}>
                  <td className="muted small">{j.id}</td>
                  <td><code>{j.type}</code></td>
                  <td><StateBadge state={j.state} /></td>
                  <td className={j.attempts > 1 ? 'badge warn' : 'muted small'}>{j.attempts}</td>
                  <td className="muted small">{j.pending_deps > 0 ? `${j.pending_deps} job(s)` : ''}</td>
                  <td className="small" style={{ color: j.error ? 'var(--bad)' : undefined, maxWidth: 380 }}>{j.error}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}
