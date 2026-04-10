export interface PipelineEvent {
  type: string
  run_id?: number
  query?: string
  city?: string
  index?: number
  total?: number
  count?: number
  company?: string
  industry?: string
  distance_miles?: number | null
  ownership_type?: string | null
  inserted?: number
  skipped_geo?: number
  skipped_dupe?: number
  total_leads?: number
  inserted_so_far?: number
  skipped_geo_so_far?: number
  skipped_dupe_so_far?: number
  batch_inserted?: number
  batch_rejected?: number
  processed?: number
  total_pending?: number
  skipped_so_far?: number
  message?: string
  detail?: string
  industries?: string[]
  cities?: string[]
  total_queries?: number
  // outreach events
  pushed?: number
  skipped?: number
  failed?: number
  // enrichment / generation events
  sources?: string[]
  lead_id?: number
  cost?: number
  // step-level enrichment events
  step?: string
  elapsed?: number
  fields_filled?: string[]
  field_values?: Record<string, string | null>
  field_sources?: Record<string, string | null>
  tier?: string
  tier_reason?: string
  kept?: number
  removed?: number
  error?: string
  // email generation
  generated_subject?: string
  generated_email?: string
}

export interface RunRecord {
  id: number
  started_at: string
  finished_at: string | null
  status: 'running' | 'completed' | 'failed'
  inserted: number
  skipped_geo: number
  skipped_dupe: number
  total_leads: number | null
  error_message: string | null
  triggered_by: string | null
  cost: number | null
}

export interface Stats {
  total_leads: number
  by_industry: { industry: string; count: number }[]
  by_ownership_type: { ownership_type: string; count: number }[]
}

export interface Config {
  industries: string[]
  cities: string[]
  min_reviews: number
  min_rating: number
  geo_radius_miles: number
  max_leads_per_run: number
}

export interface StatusResponse {
  is_running: boolean
  is_paused: boolean
  active_run_id: number | null
  next_run_at: string | null
}

export interface CsvUploadResponse {
  run_id: number | null
  inserted: number
  skipped: number
  total: number
  mapping: Record<string, string>
  message?: string | null
}

export type NodeState = 'idle' | 'active' | 'complete' | 'error'

export type FieldState = 'empty' | 'loading' | 'filled' | 'failed'

export interface FieldCell {
  value: string | null
  source: string | null
  state: FieldState
  cost: number
  error?: string
}

export interface LeadRow {
  id: number
  company: string
  city: string
  industry: string
  tier: string | null
  tierReason: string | null
  status: 'inserted' | 'enriching' | 'enriched' | 'generated' | 'pushed' | 'error'
  fields: Record<string, FieldCell>
  totalCost: number
  generatedSubject: string | null
  generatedEmail: string | null
}

export const ENRICHABLE_FIELDS = [
  'google_maps_url',
  'owner_email',
  'owner_phone',
  'owner_linkedin',
  'employee_count',
  'key_staff',
  'year_established',
  'services_offered',
  'company_description',
  'revenue_estimate',
  'certifications',
  'review_summary',
  'facebook_url',
  'yelp_url',
] as const

export const FIELD_LABELS: Record<string, string> = {
  google_maps_url: 'Maps',
  owner_email: 'Email',
  owner_phone: 'Phone',
  owner_linkedin: 'LinkedIn',
  employee_count: 'Employees',
  key_staff: 'Staff',
  year_established: 'Est.',
  services_offered: 'Services',
  company_description: 'Description',
  revenue_estimate: 'Revenue',
  certifications: 'Certs',
  review_summary: 'Reviews',
  facebook_url: 'Facebook',
  yelp_url: 'Yelp',
}
