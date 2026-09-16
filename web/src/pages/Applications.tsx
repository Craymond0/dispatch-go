import { api } from '../lib/api'
import { useApi } from '../hooks'
import { APP_STATUSES, type AppStatus, type Application } from '../types'
import { Banner, Empty, StateBadge } from '../components/ui'
import { shortDate } from '../lib/format'

export default function Applications() {
  const { data, error, loading, reload, setData } = useApi<Application[]>('/tracker/applications')

  async function patch(id: number, body: Record<string, unknown>) {
    const updated = await api.patch<Application>(`/tracker/applications/${id}`, body)
    setData((prev) => prev?.map((a) => (a.id === id ? updated : a)) ?? prev)
  }

  const byStatus = (data ?? []).reduce<Record<string, number>>((acc, a) => {
    acc[a.status] = (acc[a.status] ?? 0) + 1
    return acc
  }, {})

  return (
    <>
      <div className="row" style={{ marginBottom: 12 }}>
        <h1>Applications</h1>
        <span className="spacer" />
        {Object.entries(byStatus).map(([s, n]) => (
          <span key={s} className="small muted">{s}: {n}</span>
        ))}
        <button onClick={reload}>Refresh</button>
      </div>

      {error && <Banner kind="error">{error}</Banner>}
      {loading && !data && <p className="muted">Loading…</p>}
      {data && data.length === 0 && <Empty>No applications recorded yet. Open a posting and press “Applied”.</Empty>}

      {!!data?.length && (
        <div className="card scroll" style={{ padding: 0 }}>
          <table>
            <thead>
              <tr>
                <th>Company</th><th>Role</th><th>Applied</th><th>Status</th><th>Last contact</th><th>Resume</th><th>Posting</th>
              </tr>
            </thead>
            <tbody>
              {data.map((a) => (
                <tr key={a.id}>
                  <td><b>{a.company}</b></td>
                  <td>{a.title}</td>
                  <td className="muted small">{shortDate(a.applied_on)}</td>
                  <td>
                    <select
                      value={a.status}
                      onChange={(e) => patch(a.id, { status: e.target.value as AppStatus })}
                      style={{ width: 'auto' }}
                    >
                      {APP_STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
                    </select>
                  </td>
                  <td>
                    <input
                      type="date"
                      value={a.last_contact_on ?? ''}
                      onChange={(e) => e.target.value && patch(a.id, { last_contact_on: e.target.value })}
                      style={{ width: 140 }}
                    />
                  </td>
                  <td className="muted small">{a.resume_ref}</td>
                  <td>
                    <a href={a.url} target="_blank" rel="noreferrer noopener">↗</a>{' '}
                    {a.posting_state === 'closed' && <StateBadge state="closed" />}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}
