// Thin fetch layer. Cookies carry the session; a 401 anywhere sends the user
// to the login page via the onUnauthorized hook the app installs.

let onUnauthorized: () => void = () => {}
export function setUnauthorizedHandler(fn: () => void) {
  onUnauthorized = fn
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message)
  }
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    credentials: 'same-origin',
    headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (res.status === 401) {
    onUnauthorized()
    throw new ApiError(401, 'unauthorized')
  }
  const text = await res.text()
  let data: unknown = null
  try {
    data = text ? JSON.parse(text) : null
  } catch {
    data = { error: text }
  }
  if (!res.ok) {
    const msg = (data as { error?: string })?.error ?? `${res.status} ${res.statusText}`
    throw new ApiError(res.status, msg)
  }
  return data as T
}

export const api = {
  get: <T>(path: string) => request<T>('GET', path),
  post: <T>(path: string, body?: unknown) => request<T>('POST', path, body),
  put: <T>(path: string, body?: unknown) => request<T>('PUT', path, body),
  patch: <T>(path: string, body?: unknown) => request<T>('PATCH', path, body),
}

/**
 * Consume a server-sent event stream from a POST endpoint. fetch + a manual
 * parser rather than EventSource, because EventSource cannot POST or carry
 * the cookie-scoped credentials mode we need.
 */
export async function streamSSE(
  path: string,
  handlers: { onData: (data: string) => void; onEvent: (event: string, data: string) => void },
  signal?: AbortSignal,
): Promise<void> {
  const res = await fetch(path, { method: 'POST', credentials: 'same-origin', signal })
  if (res.status === 401) {
    onUnauthorized()
    throw new ApiError(401, 'unauthorized')
  }
  if (!res.body) throw new ApiError(res.status, 'no response body')
  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let event = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let idx: number
    while ((idx = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, idx).replace(/\r$/, '')
      buffer = buffer.slice(idx + 1)
      if (line.startsWith('event: ')) {
        event = line.slice(7)
      } else if (line.startsWith('data: ')) {
        const data = line.slice(6)
        if (event) handlers.onEvent(event, data)
        else handlers.onData(data)
        event = ''
      } else if (line === '') {
        event = ''
      }
    }
  }
}

export function metricsText(): Promise<string> {
  return fetch('/metrics', { credentials: 'same-origin' }).then((r) => {
    if (r.status === 401) onUnauthorized()
    return r.text()
  })
}
