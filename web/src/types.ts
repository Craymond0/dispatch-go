// Mirrors the Go JSON shapes. Keep in sync with internal/tracker and internal/queue.

export interface Posting {
  id: number
  company_id: number
  company: string
  source: string
  external_id: string
  url: string
  title: string
  location: string
  category: string
  posted_at: string | null
  first_seen_at: string
  last_seen_at: string
  state: 'open' | 'closed'
  closed_at: string | null
  tracked: boolean
}

export interface Company {
  id: number
  name: string
  slug: string
  board_type: 'greenhouse' | 'lever' | 'ashby' | 'feed' | 'manual'
  board_id: string
  followed: boolean
  board_error?: string
  added_at: string
}

export type AppStatus = 'applied' | 'oa' | 'phone' | 'onsite' | 'offer' | 'rejected' | 'ghosted' | 'withdrawn'
export const APP_STATUSES: AppStatus[] = ['applied', 'oa', 'phone', 'onsite', 'offer', 'rejected', 'ghosted', 'withdrawn']

export interface Application {
  id: number
  posting_id: number
  company: string
  title: string
  url: string
  posting_state: 'open' | 'closed'
  applied_on: string
  resume_ref: string
  status: AppStatus
  last_contact_on: string | null
  notes: string
  updated_at: string
}

export interface FollowUp {
  application_id: number
  posting_id: number
  company: string
  title: string
  status: AppStatus
  days_quiet: number
}

export interface Digest {
  sweep_id: number
  since: string
  at: string
  new: Posting[] | null
  closed: Posting[] | null
  changed: Posting[] | null
  follow_ups: FollowUp[] | null
  failed_sources: string[] | null
}

export interface Status {
  email_configured: boolean
  llm_configured: boolean
  resume_stored: boolean
  open_postings: number
  tracked_postings: number
  followed_companies: number
  active_applications: number
  last_digest_at?: string
  next_sweep_at?: string
  sweep_interval_seconds?: number
  jobs_running: number
  jobs_queued: number
}

export interface Job {
  id: number
  type: string
  state: 'queued' | 'running' | 'succeeded' | 'failed'
  attempts: number
  pending_deps: number
  payload: unknown
  result: unknown
  error: string
}

export interface FitReport {
  posting_id: number
  report: string
  model: string
  input_tokens: number
  output_tokens: number
  created_at: string
}
