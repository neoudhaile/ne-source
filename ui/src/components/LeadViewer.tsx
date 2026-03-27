import { useState, useEffect } from 'react'
import { X, ChevronDown, ChevronUp } from 'lucide-react'
import { getLeads } from '../api'

interface Lead {
  id: number
  company: string
  owner_name: string | null
  email: string | null
  owner_email: string | null
  phone: string | null
  city: string | null
  state: string | null
  industry: string | null
  rating: number | null
  review_count: number | null
  ownership_type: string | null
  distance_miles: number | null
  website: string | null
  google_maps_url: string | null
  owner_title: string | null
  owner_phone: string | null
  owner_linkedin: string | null
  employee_count: number | null
  key_staff: string[] | null
  year_established: number | null
  services_offered: string[] | null
  company_description: string | null
  revenue_estimate: string | null
  certifications: string[] | null
  review_summary: string | null
  facebook_url: string | null
  yelp_url: string | null
  enrichment_meta: Record<string, { source: string }> | null
  generated_subject: string | null
  generated_email: string | null
  created_at: string
  [key: string]: unknown
}

function FieldRow({ label, value, source }: { label: string; value: unknown; source?: string }) {
  if (value === null || value === undefined || value === '') return null
  const display = Array.isArray(value) ? value.join(', ') : String(value)
  return (
    <div className="flex items-start gap-2 py-1">
      <span className="text-gray-500 text-xs w-36 flex-shrink-0">{label}</span>
      <span className="text-gray-200 text-xs flex-1 break-all">{display}</span>
      {source && (
        <span className={`text-[10px] px-1.5 py-0.5 rounded flex-shrink-0 ${
          source === 'claude_inferred' ? 'bg-purple-900/50 text-purple-300' : 'bg-gray-800 text-gray-400'
        }`}>{source}</span>
      )}
    </div>
  )
}

function LeadCard({ lead }: { lead: Lead }) {
  const [expanded, setExpanded] = useState(false)
  const meta = lead.enrichment_meta || {}

  return (
    <div className="bg-gray-800 rounded-lg overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-4 py-3 flex items-center gap-3 text-left hover:bg-gray-750 transition-colors"
      >
        <div className="flex-1 min-w-0">
          <div className="text-sm text-white font-medium truncate">{lead.company}</div>
          <div className="text-xs text-gray-400 mt-0.5">
            {lead.industry} · {lead.city}, {lead.state}
            {lead.distance_miles != null && ` · ${lead.distance_miles} mi`}
          </div>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          {lead.generated_email && <span className="text-[10px] px-1.5 py-0.5 bg-green-900/50 text-green-300 rounded">Email</span>}
          {lead.enrichment_meta && <span className="text-[10px] px-1.5 py-0.5 bg-cyan-900/50 text-cyan-300 rounded">Enriched</span>}
          {expanded ? <ChevronUp size={14} className="text-gray-400" /> : <ChevronDown size={14} className="text-gray-400" />}
        </div>
      </button>

      {expanded && (
        <div className="px-4 pb-3 border-t border-gray-700 pt-2 space-y-3">
          {/* Basic Info */}
          <div>
            <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Basic Info</div>
            <FieldRow label="Owner" value={lead.owner_name} />
            <FieldRow label="Email" value={lead.email} />
            <FieldRow label="Phone" value={lead.phone} />
            <FieldRow label="Website" value={lead.website} />
            <FieldRow label="Rating" value={lead.rating != null ? `${lead.rating} (${lead.review_count} reviews)` : null} />
            <FieldRow label="Ownership" value={lead.ownership_type} />
            <FieldRow label="Google Maps" value={lead.google_maps_url} />
          </div>

          {/* Enriched Contact */}
          <div>
            <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Enriched Contact</div>
            <FieldRow label="Owner Email" value={lead.owner_email} source={meta.owner_email?.source} />
            <FieldRow label="Owner Phone" value={lead.owner_phone} source={meta.owner_phone?.source} />
            <FieldRow label="Owner Title" value={lead.owner_title} source={meta.owner_title?.source} />
            <FieldRow label="Owner LinkedIn" value={lead.owner_linkedin} source={meta.owner_linkedin?.source} />
            <FieldRow label="Employee Count" value={lead.employee_count} source={meta.employee_count?.source} />
            <FieldRow label="Key Staff" value={lead.key_staff} source={meta.key_staff?.source} />
          </div>

          {/* Company Intel */}
          <div>
            <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Company Intel</div>
            <FieldRow label="Year Established" value={lead.year_established} source={meta.year_established?.source} />
            <FieldRow label="Services" value={lead.services_offered} source={meta.services_offered?.source} />
            <FieldRow label="Description" value={lead.company_description} source={meta.company_description?.source} />
            <FieldRow label="Revenue Est." value={lead.revenue_estimate} source={meta.revenue_estimate?.source} />
            <FieldRow label="Certifications" value={lead.certifications} source={meta.certifications?.source} />
          </div>

          {/* Reputation */}
          <div>
            <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Reputation</div>
            <FieldRow label="Review Summary" value={lead.review_summary} source={meta.review_summary?.source} />
            <FieldRow label="Facebook" value={lead.facebook_url} source={meta.facebook_url?.source} />
            <FieldRow label="Yelp" value={lead.yelp_url} source={meta.yelp_url?.source} />
          </div>

          {/* Generated Email */}
          {lead.generated_subject && (
            <div>
              <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Generated Email</div>
              <div className="bg-gray-900 rounded p-3 space-y-2">
                <div className="text-xs text-gray-300"><span className="text-gray-500">Subject:</span> {lead.generated_subject}</div>
                <div className="text-xs text-gray-300 whitespace-pre-wrap">{lead.generated_email}</div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export function LeadViewer({ onClose }: { onClose: () => void }) {
  const [leads, setLeads] = useState<Lead[]>([])
  const [loading, setLoading] = useState(true)
  const [offset, setOffset] = useState(0)
  const PAGE_SIZE = 50

  useEffect(() => {
    setLoading(true)
    getLeads(PAGE_SIZE, offset)
      .then(data => setLeads(data as Lead[]))
      .finally(() => setLoading(false))
  }, [offset])

  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed right-0 top-0 h-full w-[480px] bg-gray-900 border-l border-gray-700 z-50 flex flex-col">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h2 className="text-white font-semibold">Leads ({leads.length})</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white"><X size={18} /></button>
        </div>

        <div className="flex-1 overflow-y-auto p-3 space-y-2">
          {loading && <p className="text-gray-500 text-sm text-center mt-8">Loading...</p>}
          {!loading && leads.length === 0 && <p className="text-gray-600 text-sm text-center mt-8">No leads yet</p>}
          {leads.map(lead => <LeadCard key={lead.id} lead={lead} />)}
        </div>

        {leads.length === PAGE_SIZE && (
          <div className="px-4 py-2 border-t border-gray-700 flex justify-between">
            <button
              onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              disabled={offset === 0}
              className="text-sm text-gray-400 hover:text-white disabled:opacity-30"
            >Previous</button>
            <button
              onClick={() => setOffset(offset + PAGE_SIZE)}
              className="text-sm text-gray-400 hover:text-white"
            >Next</button>
          </div>
        )}
      </div>
    </>
  )
}
