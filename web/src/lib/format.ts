export function relTime(iso: string | null | undefined): string {
  if (!iso) return ''
  const t = new Date(iso).getTime()
  if (Number.isNaN(t)) return ''
  const diff = Date.now() - t
  const abs = Math.abs(diff)
  const units: [number, string][] = [
    [60_000, 'm'],
    [3_600_000, 'h'],
    [86_400_000, 'd'],
  ]
  if (abs < 60_000) return diff >= 0 ? 'just now' : 'in <1m'
  let label = ''
  for (let i = units.length - 1; i >= 0; i--) {
    if (abs >= units[i][0]) {
      label = `${Math.floor(abs / units[i][0])}${units[i][1]}`
      break
    }
  }
  return diff >= 0 ? `${label} ago` : `in ${label}`
}

export function shortDate(iso: string | null | undefined): string {
  if (!iso) return ''
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

export function host(url: string): string {
  try {
    return new URL(url).host.replace(/^www\./, '')
  } catch {
    return url
  }
}
