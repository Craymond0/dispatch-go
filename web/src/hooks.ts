import { useCallback, useEffect, useRef, useState } from 'react'
import { api, ApiError } from './lib/api'

/** Fetch JSON on mount and whenever deps change, with manual reload. */
export function useApi<T>(path: string | null, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(path !== null)
  const version = useRef(0)

  const reload = useCallback(() => {
    if (path === null) return
    const v = ++version.current
    setLoading(true)
    api
      .get<T>(path)
      .then((d) => {
        if (v === version.current) {
          setData(d)
          setError(null)
        }
      })
      .catch((e: unknown) => {
        if (v === version.current) setError(e instanceof ApiError ? e.message : String(e))
      })
      .finally(() => {
        if (v === version.current) setLoading(false)
      })
  }, [path])

  useEffect(reload, [reload, ...deps])
  return { data, error, loading, reload, setData }
}

/** Run fn every ms while the tab is visible. */
export function usePoll(fn: () => void, ms: number) {
  const saved = useRef(fn)
  saved.current = fn
  useEffect(() => {
    const tick = () => {
      if (!document.hidden) saved.current()
    }
    const id = setInterval(tick, ms)
    return () => clearInterval(id)
  }, [ms])
}

/** Debounce a rapidly-changing value (search boxes). */
export function useDebounced<T>(value: T, ms = 250): T {
  const [v, setV] = useState(value)
  useEffect(() => {
    const id = setTimeout(() => setV(value), ms)
    return () => clearTimeout(id)
  }, [value, ms])
  return v
}
