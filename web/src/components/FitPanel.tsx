import { useCallback, useEffect, useRef, useState, type ReactElement } from 'react'
import { api, ApiError, streamSSE } from '../lib/api'
import type { FitReport } from '../types'
import { Banner } from '../components/ui'
import { relTime } from '../lib/format'

/** Render the model's Markdown subset: ## headings, - bullets, paragraphs. */
function Rendered({ text }: { text: string }) {
  const blocks: ReactElement[] = []
  let bullets: string[] = []
  const flush = (key: string) => {
    if (bullets.length) {
      blocks.push(<ul key={key}>{bullets.map((b, i) => <li key={i}>{b}</li>)}</ul>)
      bullets = []
    }
  }
  text.split('\n').forEach((line, i) => {
    const l = line.trimEnd()
    if (l.startsWith('## ')) {
      flush(`u${i}`)
      blocks.push(<h3 key={i}>{l.slice(3)}</h3>)
    } else if (/^[-*]\s+/.test(l)) {
      bullets.push(l.replace(/^[-*]\s+/, ''))
    } else if (l.trim() === '') {
      flush(`u${i}`)
    } else {
      flush(`u${i}`)
      blocks.push(<p key={i} style={{ margin: '4px 0' }}>{l}</p>)
    }
  })
  flush('last')
  return <>{blocks}</>
}

export default function FitPanel({ postingId }: { postingId: number }) {
  const [text, setText] = useState('')
  const [report, setReport] = useState<FitReport | null>(null)
  const [streaming, setStreaming] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [needsPaste, setNeedsPaste] = useState(false)
  const [pasted, setPasted] = useState('')
  const abort = useRef<AbortController | null>(null)

  useEffect(() => {
    let live = true
    setText(''); setReport(null); setError(null); setNeedsPaste(false)
    api
      .get<{ report: FitReport | null }>(`/tracker/postings/${postingId}/fit`)
      .then(({ report }) => { if (live && report) { setReport(report); setText(report.report) } })
      .catch(() => {})
    return () => {
      live = false
      abort.current?.abort()
    }
  }, [postingId])

  const run = useCallback(
    async (force: boolean) => {
      abort.current?.abort()
      const ctl = new AbortController()
      abort.current = ctl
      setStreaming(true)
      setError(null)
      setNeedsPaste(false)
      setText('')
      setReport(null)
      try {
        await streamSSE(
          `/tracker/postings/${postingId}/fit${force ? '?force=1' : ''}`,
          {
            onData: (d) => {
              const { text } = JSON.parse(d) as { text: string }
              setText((t) => t + text)
            },
            onEvent: (event, d) => {
              const parsed = JSON.parse(d) as { report?: FitReport; error?: string; partial?: string }
              if (event === 'done' && parsed.report) {
                setReport(parsed.report)
                setText(parsed.report.report)
              } else if (event === 'error') {
                setError(parsed.error ?? 'failed')
                if (parsed.partial) setText(parsed.partial)
                if (parsed.error?.includes('Paste the description')) setNeedsPaste(true)
              }
            },
          },
          ctl.signal,
        )
      } catch (e) {
        if (!ctl.signal.aborted) setError(e instanceof ApiError ? e.message : String(e))
      } finally {
        if (!ctl.signal.aborted) setStreaming(false)
      }
    },
    [postingId],
  )

  async function savePaste() {
    await api.put(`/tracker/postings/${postingId}/description`, { text: pasted })
    setNeedsPaste(false)
    setPasted('')
    run(true)
  }

  return (
    <section style={{ marginTop: 16 }}>
      <div className="row">
        <h3>Fit analysis</h3>
        <span className="spacer" />
        {report && <span className="muted small">{report.model} · {report.input_tokens + report.output_tokens} tokens · {relTime(report.created_at)}</span>}
        <button onClick={() => run(!!report)} disabled={streaming}>
          {streaming ? 'Analysing…' : report || text ? 'Re-run' : 'Analyse'}
        </button>
      </div>

      {error && <Banner kind="error">{error}</Banner>}

      {needsPaste && (
        <div className="col" style={{ marginTop: 8 }}>
          <textarea
            rows={8}
            placeholder="Paste the job description here…"
            value={pasted}
            onChange={(e) => setPasted(e.target.value)}
          />
          <div className="row">
            <button className="primary" disabled={pasted.trim().length < 200} onClick={savePaste}>
              Save description and analyse
            </button>
            <span className="muted small">{pasted.trim().length} characters (200 minimum)</span>
          </div>
        </div>
      )}

      {(text || streaming) && (
        <div className={streaming ? 'report cursor' : 'report'} style={{ marginTop: 8 }}>
          <Rendered text={text} />
        </div>
      )}
      {!text && !streaming && !error && (
        <p className="muted small">Compares this posting against your stored master resume. Every claim is quoted from the resume.</p>
      )}
    </section>
  )
}
