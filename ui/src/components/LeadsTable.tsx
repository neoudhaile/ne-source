import { useState, useEffect, useRef } from 'react'
import type { PipelineEvent, LeadRow, FieldCell, DBLead } from '../types'
import { ENRICHABLE_FIELDS } from '../types'
import { ChevronRight, ChevronDown, Loader2 } from 'lucide-react'
import { getRunLeads } from '../api'

// ── Field groupings for lead enrichment details ────────────────────────────

const FIELD_GROUPS = [
  {
    label: 'Owner / Contact',
    fields: ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin'],
  },
  {
    label: 'Team',
    fields: ['employee_count', 'key_staff'],
  },
  {
    label: 'Business Detail',
    fields: ['services_offered', 'company_description', 'year_established', 'revenue_estimate', 'certifications'],
  },
  {
    label: 'Reputation & Links',
    fields: ['review_summary', 'google_maps_url', 'facebook_url', 'yelp_url'],
  },
]

const FIELD_LABELS: Record<string, string> = {
  owner_name: 'Owner',
  owner_email: 'Email',
  owner_phone: 'Phone',
  owner_linkedin: 'LinkedIn',
  employee_count: 'Employees',
  key_staff: 'Key Staff',
  year_established: 'Year Est.',
  services_offered: 'Services',
  company_description: 'Description',
  revenue_estimate: 'Revenue',
  certifications: 'Certifications',
  review_summary: 'Reviews',
  google_maps_url: 'Google Maps',
  facebook_url: 'Facebook',
  yelp_url: 'Yelp',
}

const SOURCE_BADGES: Record<string, { label: string; color: string }> = {
  google_places: { label: 'Places', color: 'bg-emerald-800/60 text-emerald-300' },
  domain_recovery: { label: 'Domain', color: 'bg-teal-800/60 text-teal-300' },
  openmart: { label: 'Openmart', color: 'bg-rose-800/60 text-rose-200' },
  hunter: { label: 'Hunter', color: 'bg-green-800/60 text-green-300' },
  apollo: { label: 'Apollo', color: 'bg-blue-800/60 text-blue-300' },
  sixtyfour: { label: 'Sixtyfour', color: 'bg-indigo-800/60 text-indigo-300' },
  fullenrich: { label: 'FullEnrich', color: 'bg-fuchsia-800/60 text-fuchsia-300' },
  scrape: { label: 'Scrape', color: 'bg-orange-800/60 text-orange-300' },
  direct: { label: 'Direct', color: 'bg-orange-800/60 text-orange-300' },
  zyte: { label: 'Zyte', color: 'bg-amber-800/60 text-amber-300' },
  direct_then_zyte: { label: 'Direct+Zyte', color: 'bg-orange-900/60 text-amber-200' },
  claude_inferred: { label: 'Claude', color: 'bg-purple-800/60 text-purple-300' },
  constructed: { label: 'Constructed', color: 'bg-gray-700/60 text-gray-300' },
  company_fallback: { label: 'Company', color: 'bg-slate-700/60 text-slate-200' },
  csv_import: { label: 'CSV', color: 'bg-cyan-800/60 text-cyan-300' },
}

// Step name → which fields it can fill
const STEP_FIELDS: Record<string, string[]> = {
  'Google Places': ['google_maps_url'],
  'Google Maps URL': ['google_maps_url'],
  'Domain recovery': ['google_maps_url'],
  'Openmart company enrich': ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin', 'key_staff'],
  'Hunter.io': ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin', 'key_staff'],
  'Apollo': ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin', 'employee_count', 'key_staff', 'year_established', 'revenue_estimate', 'company_description', 'facebook_url', 'services_offered'],
  'FullEnrich': ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin'],
  'Website scrape': ['owner_name', 'services_offered', 'year_established', 'company_description', 'certifications', 'facebook_url', 'yelp_url', 'employee_count'],
  'Review scrape': ['review_summary'],
  'Company fallback': ['owner_email', 'owner_phone'],
  'Claude failsafe': ['employee_count', 'key_staff', 'year_established', 'services_offered', 'company_description', 'revenue_estimate', 'certifications', 'review_summary', 'facebook_url', 'yelp_url'],
}

function emptyFields(): Record<string, FieldCell> {
  const fields: Record<string, FieldCell> = {}
  for (const f of ENRICHABLE_FIELDS) {
    fields[f] = { value: null, source: null, state: 'empty', cost: 0 }
  }
  return fields
}

function emptyRow(id: number, company: string, city: string, industry: string): LeadRow {
  return {
    id, company, city, industry,
    tier: null,
    tierReason: null,
    status: 'inserted',
    fields: emptyFields(),
    totalCost: 0,
    generatedSubject: null,
    generatedEmail: null,
  }
}

function sourceFromStep(step: string): string {
  if (step.includes('Google Places')) return 'google_places'
  if (step.includes('Domain recovery')) return 'domain_recovery'
  if (step.includes('Openmart')) return 'openmart'
  if (step.includes('Hunter')) return 'hunter'
  if (step.includes('Apollo')) return 'apollo'
  if (step.includes('Sixtyfour')) return 'sixtyfour'
  if (step.includes('FullEnrich')) return 'fullenrich'
  if (step.includes('Website scrape')) return 'scrape'
  if (step.includes('Review scrape')) return 'scrape'
  if (step.includes('Company fallback')) return 'company_fallback'
  if (step.includes('Claude failsafe')) return 'claude_inferred'
  if (step.includes('Google Maps')) return 'constructed'
  return 'claude_inferred'
}

function dbLeadToRow(lead: DBLead): LeadRow {
  const fields = emptyFields()
  const meta = lead.enrichment_meta || {}

  for (const f of ENRICHABLE_FIELDS) {
    const raw = lead[f as keyof DBLead]
    if (raw == null || raw === '' || (Array.isArray(raw) && raw.length === 0)) continue
    const display = Array.isArray(raw) ? raw.join(', ') : String(raw)
    const source = (meta[f] as { source?: string; provider?: string } | undefined)?.source
      || (meta[f] as { source?: string; provider?: string } | undefined)?.provider
      || null
    fields[f] = { value: display, source, state: 'filled', cost: 0 }
  }

  const hasEnrichment = Object.values(fields).some(f => f.state === 'filled')
  let status: LeadRow['status'] = 'inserted'
  if (lead.generated_email) status = 'generated'
  else if (hasEnrichment) status = 'enriched'

  return {
    id: lead.id,
    company: lead.company || '',
    city: lead.city || '',
    industry: lead.industry || '',
    tier: lead.tier || null,
    tierReason: lead.tier_reason || null,
    status,
    fields,
    totalCost: 0,
    generatedSubject: lead.generated_subject || null,
    generatedEmail: lead.generated_email || null,
  }
}

function applyEvent(rows: Map<number, LeadRow>, e: PipelineEvent): boolean {
  if (e.type === 'start') {
    rows.clear()
    return true
  }

  if (e.type === 'insert' && e.lead_id != null) {
    if (!rows.has(e.lead_id)) {
      const row = emptyRow(e.lead_id, e.company || '', e.city || '', e.industry || '')
      const fieldValues = e.field_values || {}
      const fieldSources = e.field_sources || {}
      for (const [field, value] of Object.entries(fieldValues)) {
        if (!row.fields[field]) continue
        row.fields[field].state = 'filled'
        row.fields[field].value = value
        row.fields[field].source = fieldSources[field] || 'csv_import'
      }
      if (e.generated_subject || e.generated_email) {
        row.status = 'generated'
        row.generatedSubject = e.generated_subject || null
        row.generatedEmail = e.generated_email || null
      }
      rows.set(e.lead_id, row)
    }
    return true
  }

  if (e.type === 'tier_lead' && e.lead_id != null) {
    const row = rows.get(e.lead_id)
    if (!row) return false
    row.tier = e.tier || null
    row.tierReason = e.tier_reason || null
    return true
  }

  if (e.type === 'lead_removed' && e.lead_id != null) {
    rows.delete(e.lead_id)
    return true
  }

  if (e.type === 'enrich_step_start' && e.lead_id != null) {
    const row = rows.get(e.lead_id)
    if (!row) return false
    row.status = 'enriching'
    const stepFields = STEP_FIELDS[e.step || ''] || []
    for (const f of stepFields) {
      if (row.fields[f]?.state === 'empty') {
        row.fields[f].state = 'loading'
      }
    }
    return true
  }

  if (e.type === 'enrich_step_done' && e.lead_id != null) {
    const row = rows.get(e.lead_id)
    if (!row) return false
    const filled = e.fields_filled || []
    const fieldValues = e.field_values || {}
    const stepFields = STEP_FIELDS[e.step || ''] || []
    for (const f of filled) {
      if (row.fields[f]) {
        row.fields[f].state = 'filled'
        row.fields[f].source = (e.field_sources || {})[f] || sourceFromStep(e.step || '')
        row.fields[f].value = fieldValues[f] ?? null
        row.fields[f].cost = (e.cost || 0) / Math.max(filled.length, 1)
      }
    }
    for (const f of stepFields) {
      if (row.fields[f]?.state === 'loading') {
        row.fields[f].state = 'empty'
      }
    }
    row.totalCost += e.cost || 0
    return true
  }

  if (e.type === 'enrich_step_error' && e.lead_id != null) {
    const row = rows.get(e.lead_id)
    if (!row) return false
    const stepFields = STEP_FIELDS[e.step || ''] || []
    for (const f of stepFields) {
      if (row.fields[f]?.state === 'loading') {
        row.fields[f].state = 'failed'
        row.fields[f].error = e.error
      }
    }
    return true
  }

  if (e.type === 'enrich_done') {
    for (const row of rows.values()) {
      if (row.status === 'enriching') row.status = 'enriched'
    }
    return true
  }

  if (e.type === 'generate_lead' && e.lead_id != null) {
    const row = rows.get(e.lead_id)
    if (row) {
      row.status = 'generated'
      row.generatedSubject = e.generated_subject || null
      row.generatedEmail = e.generated_email || null
    }
    return true
  }

  if (e.type === 'outreach_done') {
    for (const row of rows.values()) {
      if (row.status === 'generated') row.status = 'pushed'
    }
    return true
  }

  return false
}

function tierBadge(tier: string | null) {
  if (tier === 'tier_1') return 'bg-emerald-900/40 text-emerald-300'
  if (tier === 'tier_2') return 'bg-blue-900/40 text-blue-300'
  if (tier === 'tier_3') return 'bg-amber-900/40 text-amber-300'
  return null
}

function tierLabel(tier: string | null) {
  if (tier === 'tier_1') return 'Tier 1'
  if (tier === 'tier_2') return 'Tier 2'
  if (tier === 'tier_3') return 'Tier 3'
  return null
}

// ── Source badge (goes next to field name) ─────────────────────────────────

function SourceBadge({ cell }: { cell: FieldCell }) {
  if (cell.state === 'loading') {
    return <Loader2 size={10} className="animate-spin text-cyan-500" />
  }
  if (cell.state === 'failed') {
    return <span className="text-red-500 text-[10px]">✗</span>
  }
  if (cell.state === 'filled' && cell.source) {
    const badge = SOURCE_BADGES[cell.source] || SOURCE_BADGES.constructed
    return (
      <span className={`px-1 py-0.5 rounded text-[9px] font-medium ${badge.color}`}>
        {badge.label}
      </span>
    )
  }
  return null
}

// ── Field value (goes on the right side) ──────────────────────────────────

function FieldValueText({ cell }: { cell: FieldCell }) {
  if (cell.state === 'loading') {
    return <span className="text-gray-600 text-[11px]">...</span>
  }
  if (cell.state === 'failed') {
    return (
      <span className="text-red-400/70 text-[11px] truncate" title={cell.error}>
        {cell.error ? cell.error.slice(0, 50) : 'Failed'}
      </span>
    )
  }
  if (cell.state === 'filled' && cell.value) {
    const display = cell.value.length > 60 ? cell.value.slice(0, 60) + '...' : cell.value
    return <span className="text-gray-200 text-[11px]" title={cell.value}>{display}</span>
  }
  return <span className="text-gray-700 text-[11px]">—</span>
}

// ── Single lead card ───────────────────────────────────────────────────────

function LeadCard({ row }: { row: LeadRow }) {
  const [open, setOpen] = useState(false)

  const totalFields = ENRICHABLE_FIELDS.length
  const filledCount = Object.values(row.fields).filter(f => f.state === 'filled').length
  const loadingCount = Object.values(row.fields).filter(f => f.state === 'loading').length
  const failedCount = Object.values(row.fields).filter(f => f.state === 'failed').length
  const allFilled = filledCount === totalFields

  const isActive = row.status === 'enriching' || loadingCount > 0

  const countColor = allFilled
    ? 'text-green-400'
    : failedCount > 0
      ? 'text-red-400'
      : filledCount > 0
        ? 'text-yellow-400'
        : 'text-gray-500'

  return (
    <div className={`border rounded-lg transition-colors ${
      isActive ? 'border-cyan-800/50 bg-gray-900/80' : 'border-gray-800/50 bg-gray-900/40'
    }`}>
      {/* Collapsed header */}
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-3 px-3 py-2 text-left hover:bg-gray-800/30 transition-colors rounded-lg"
      >
        {open
          ? <ChevronDown size={14} className="text-gray-500 flex-shrink-0" />
          : <ChevronRight size={14} className="text-gray-500 flex-shrink-0" />}

        {isActive && <Loader2 size={12} className="animate-spin text-cyan-500 flex-shrink-0" />}

        <span className="text-white text-sm font-medium truncate flex-1">{row.company}</span>

        <span className="text-gray-500 text-xs flex-shrink-0">{row.city}</span>
        <span className="text-gray-600 text-xs flex-shrink-0">·</span>
        <span className="text-gray-500 text-xs flex-shrink-0">{row.industry}</span>

        {row.tier && (
          <span
            className={`text-[10px] font-medium px-1.5 py-0.5 rounded flex-shrink-0 ${tierBadge(row.tier)}`}
            title={row.tierReason || undefined}
          >
            {tierLabel(row.tier)}
          </span>
        )}

        <span className={`text-xs font-mono font-medium flex-shrink-0 min-w-[50px] text-right ${countColor}`}>
          {filledCount}/{totalFields}
        </span>

        {row.totalCost > 0 && (
          <span className="text-amber-500 text-xs font-mono flex-shrink-0 min-w-[45px] text-right">
            ${row.totalCost.toFixed(3)}
          </span>
        )}

        {row.status === 'generated' && (
          <span className="text-purple-400 text-[10px] font-medium px-1.5 py-0.5 rounded bg-purple-900/40 flex-shrink-0">
            EMAIL
          </span>
        )}
        {row.status === 'pushed' && (
          <span className="text-green-400 text-[10px] font-medium px-1.5 py-0.5 rounded bg-green-900/40 flex-shrink-0">
            SENT
          </span>
        )}
      </button>

      {/* Expanded detail */}
      {open && (
        <div className="px-4 pb-3 pt-1 space-y-3 border-t border-gray-800/50">
          {row.tier && (
            <div className="flex items-start justify-between gap-3 rounded bg-gray-800/30 px-3 py-2">
              <div>
                <div className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold mb-1">
                  Tier
                </div>
                <div className="text-sm text-white">{tierLabel(row.tier)}</div>
              </div>
              {row.tierReason && (
                <div className="text-[11px] text-gray-300 max-w-[75%] text-right">
                  {row.tierReason}
                </div>
              )}
            </div>
          )}

          {FIELD_GROUPS.map(group => (
            <div key={group.label}>
              <div className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold mb-1.5">
                {group.label}
              </div>
              <div className="space-y-0.5">
                {group.fields.map(f => {
                  const cell = row.fields[f]
                  if (!cell) return null
                  return (
                    <div key={f} className="flex items-center gap-2 py-0.5">
                      <span className="text-gray-400 text-[11px] w-[80px] flex-shrink-0">{FIELD_LABELS[f] || f}</span>
                      <span className="flex-shrink-0 w-[75px] flex justify-start">
                        <SourceBadge cell={cell} />
                      </span>
                      <span className="flex-1 min-w-0 truncate">
                        <FieldValueText cell={cell} />
                      </span>
                    </div>
                  )
                })}
              </div>
            </div>
          ))}

          {/* Generated Email */}
          {(row.generatedSubject || row.generatedEmail) && (
            <div>
              <div className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold mb-1.5">
                Generated Email
              </div>
              {row.generatedSubject && (
                <div className="text-[11px] text-purple-300 font-medium mb-1">
                  {row.generatedSubject}
                </div>
              )}
              {row.generatedEmail && (
                <div className="text-[11px] text-gray-300 whitespace-pre-wrap leading-relaxed bg-gray-800/40 rounded p-2">
                  {row.generatedEmail}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────

export function LeadsTable({ events, runId }: { events: PipelineEvent[]; runId: number | null }) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const rowMapRef = useRef<Map<number, LeadRow>>(new Map())
  const lastProcessedRef = useRef<number>(0)
  const [, forceUpdate] = useState(0)

  // Seed from DB whenever runId changes
  useEffect(() => {
    rowMapRef.current = new Map()
    lastProcessedRef.current = 0
    forceUpdate(n => n + 1)

    if (runId == null) return

    getRunLeads(runId).then(dbLeads => {
      const map = new Map<number, LeadRow>()
      for (const raw of dbLeads) {
        const lead = raw as unknown as DBLead
        map.set(lead.id, dbLeadToRow(lead))
      }
      rowMapRef.current = map
      forceUpdate(n => n + 1)
    }).catch(() => {
      // DB seed failed — events will still populate the table
    })
  }, [runId])

  // Process only new events incrementally (using _seq to survive array trimming)
  useEffect(() => {
    let changed = false
    for (const e of events) {
      const seq = (e as any)._seq || 0
      if (seq <= lastProcessedRef.current) continue
      if (applyEvent(rowMapRef.current, e)) changed = true
      lastProcessedRef.current = seq
    }
    if (changed) forceUpdate(n => n + 1)
  }, [events])

  const rows = Array.from(rowMapRef.current.values())
  const totalCost = rows.reduce((sum, r) => sum + r.totalCost, 0)
  const totalFilled = rows.reduce((sum, r) =>
    sum + Object.values(r.fields).filter(f => f.state === 'filled').length, 0)
  const totalPossible = rows.length * ENRICHABLE_FIELDS.length

  // Auto-scroll to bottom when new rows appear
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [rows.length])

  return (
    <div className="h-full flex flex-col bg-gray-950 rounded-lg">
      {/* Summary bar */}
      {rows.length > 0 && (
        <div className="flex items-center justify-between px-3 py-1.5 border-b border-gray-800 text-xs flex-shrink-0">
          <span className="text-gray-500">
            {rows.length} leads · {totalFilled}/{totalPossible} datapoints
          </span>
          <span className="text-amber-500 font-mono">${totalCost.toFixed(3)} total</span>
        </div>
      )}

      {/* Cards list */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-2 space-y-1.5">
        {rows.length === 0 && (
          <p className="text-gray-600 text-sm text-center mt-8">
            No leads yet — run the pipeline to populate
          </p>
        )}
        {rows.map(row => (
          <LeadCard key={row.id} row={row} />
        ))}
      </div>
    </div>
  )
}
