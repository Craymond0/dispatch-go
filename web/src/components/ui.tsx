import type { ReactNode } from 'react'

export function Badge({ kind, children }: { kind?: 'good' | 'warn' | 'bad'; children: ReactNode }) {
  return <span className={kind ? `badge ${kind}` : 'badge'}>{children}</span>
}

export function Banner({ kind = 'info', children }: { kind?: 'info' | 'error'; children: ReactNode }) {
  return <div className={`banner ${kind}`}>{children}</div>
}

export function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="stat">
      <div className="n">{value}</div>
      <div className="k">{label}</div>
    </div>
  )
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="muted" style={{ padding: '8px 0' }}>{children}</p>
}

export function Drawer({ title, onClose, children }: { title: ReactNode; onClose: () => void; children: ReactNode }) {
  return (
    <>
      <div className="drawer-backdrop" onClick={onClose} />
      <aside className="drawer" role="dialog" aria-label="details">
        <header className="row">
          <div style={{ flex: 1, minWidth: 0 }}>{title}</div>
          <button className="ghost" onClick={onClose} aria-label="Close">✕</button>
        </header>
        <div className="body">{children}</div>
      </aside>
    </>
  )
}

const STATUS_KIND: Record<string, 'good' | 'warn' | 'bad' | undefined> = {
  offer: 'good',
  onsite: 'good',
  phone: 'good',
  oa: 'warn',
  applied: undefined,
  rejected: 'bad',
  ghosted: 'bad',
  withdrawn: undefined,
  succeeded: 'good',
  running: 'warn',
  queued: undefined,
  failed: 'bad',
  open: 'good',
  closed: 'bad',
}

export function StateBadge({ state }: { state: string }) {
  return <Badge kind={STATUS_KIND[state]}>{state}</Badge>
}
