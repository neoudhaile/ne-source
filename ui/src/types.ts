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
  message?: string
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
  active_run_id: number | null
  next_run_at: string | null
}

export type NodeState = 'idle' | 'active' | 'complete' | 'error'
