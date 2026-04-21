import type { PipelineEvent } from '../types'

function formatEvent(e: PipelineEvent): { text: string; color: string } | null {
  switch (e.type) {
    case 'start':
      if (e.total_queries === 0 && e.industries?.[0] === 'csv_import') {
        return { text: 'CSV import pipeline started', color: 'text-blue-400' }
      }
      return { text: `Pipeline started — ${e.industries?.length} industries × ${e.cities?.length} cities (${e.total_queries} queries)`, color: 'text-blue-400' }
    case 'csv_imported':
      return { text: `CSV import ready — ${e.count} leads queued for enrichment`, color: 'text-blue-400' }
    case 'tier_start':
      return { text: `Tiering ${e.count} leads...`, color: 'text-blue-400' }
    case 'tier_lead': {
      const label = e.tier === 'tier_1' ? 'Tier 1' : e.tier === 'tier_2' ? 'Tier 2' : e.tier === 'tier_3' ? 'Tier 3' : 'Hard Remove'
      return { text: `  ${e.company} → ${label}${e.tier_reason ? ` · ${e.tier_reason}` : ''}`, color: e.tier === 'hard_remove' ? 'text-red-400' : 'text-cyan-400' }
    }
    case 'lead_removed':
      return { text: `  Removed ${e.company}${e.tier_reason ? ` · ${e.tier_reason}` : ''}`, color: 'text-red-400' }
    case 'tier_skip':
      return { text: `  Skipping ${e.company}${e.tier_reason ? ` · ${e.tier_reason}` : ''}`, color: 'text-yellow-400' }
    case 'tier_done':
      return { text: `Tiering complete — ${e.kept} kept, ${e.removed} removed`, color: 'text-green-300' }
    case 'csv_batch_done':
      return {
        text: `CSV insert batch: ${e.processed}/${e.total_pending} processed · ${e.inserted_so_far} inserted · ${e.skipped_so_far} skipped`,
        color: 'text-gray-500',
      }
    case 'search':
      return { text: `Searching: ${e.query} in ${e.city} (${e.index}/${e.total})`, color: 'text-gray-400' }
    case 'results':
      return { text: `  Found ${e.count} results`, color: 'text-gray-500' }
    case 'geo':
      return { text: `  Geo filtering...`, color: 'text-gray-500' }
    case 'normalizing':
      return null
    case 'inserting':
      return null
    case 'insert':
      return {
        text: `+ ${e.company} — ${e.distance_miles != null ? `${e.distance_miles} mi` : '?'} · ${e.ownership_type ?? 'Unknown'}`,
        color: 'text-green-400',
      }
    case 'skip_dupe':
      return { text: `~ ${e.company} — duplicate`, color: 'text-yellow-500' }
    case 'progress':
      return {
        text: `  Batch done (${e.index}/${e.total}): ${e.inserted_so_far} inserted, ${e.skipped_geo_so_far?.toLocaleString()} geo-filtered`,
        color: 'text-gray-500',
      }
    case 'done':
      return {
        text: `Run complete — Inserted: ${e.inserted} | Geo: ${e.skipped_geo?.toLocaleString()} | Dupes: ${e.skipped_dupe}`,
        color: 'text-green-300',
      }
    case 'export_start':
      return { text: `Exporting ${e.count} leads to Notion...`, color: 'text-blue-400' }
    case 'export_lead':
      return { text: `  Exported ${e.company} (${e.index}/${e.total})`, color: 'text-cyan-400' }
    case 'export_skip':
      return { text: `  Skipped ${e.company ?? 'lead'}${e.reason ? ` · ${e.reason}` : ''}`, color: 'text-yellow-400' }
    case 'export_done':
      return {
        text: `Notion export complete — ${e.exported ?? 0} exported, ${e.skipped ?? 0} skipped, ${e.errors ?? 0} errors`,
        color: 'text-green-300',
      }
    case 'export_error':
      return { text: `Notion export error: ${e.error ?? e.message}`, color: 'text-red-400' }
    case 'search_capped':
      return { text: `Search cap reached — ${e.count} leads inserted`, color: 'text-yellow-400' }
    case 'enrich_start':
      return { text: `Enriching ${e.count} leads...`, color: 'text-blue-400' }
    case 'enrich_step_start':
      return { text: `  ⟳ ${e.company} → ${e.step}`, color: 'text-gray-500' }
    case 'enrich_step_done':
      return {
        text: `  ✓ ${e.company} → ${e.step} (${e.elapsed}s, $${(e.cost as number)?.toFixed(3) ?? '0'})${(e.fields_filled as string[])?.length ? ' — ' + (e.fields_filled as string[]).join(', ') : ''}`,
        color: 'text-cyan-400',
      }
    case 'enrich_step_skip':
      return {
        text: `  · ${e.company} → ${e.step}${e.detail ? ` — ${e.detail}` : ''}`,
        color: 'text-gray-500',
      }
    case 'enrich_step_error':
      return { text: `  ✗ ${e.company} → ${e.step} (${e.elapsed}s): ${e.error}`, color: 'text-red-400' }
    case 'enrich_lead':
      return { text: `  Enriched ${e.company} (${e.index}/${e.total}) — ${e.sources?.join(', ') || 'no sources'}`, color: 'text-cyan-400' }
    case 'enrich_done':
      return { text: e.message || `Enrichment complete — ${e.count} leads enriched`, color: 'text-green-300' }
    case 'enrich_error':
      return { text: `Enrichment error: ${e.message}`, color: 'text-red-400' }
    case 'paused':
      return { text: 'Run paused', color: 'text-yellow-400' }
    case 'resumed':
      return { text: 'Run resumed', color: 'text-blue-400' }
    case 'insufficient_funds':
      return { text: `⚠ ${e.message || `Insufficient USDC balance (have: $${e.balance?.toFixed(2) ?? '?'}, need: ~$${e.estimated_cost?.toFixed(2) ?? '?'})`}`, color: 'text-red-400' }
    case 'error':
      return { text: `Error: ${e.message}`, color: 'text-red-400' }
    default:
      return null
  }
}

export function LiveFeed({ events }: { events: PipelineEvent[] }) {
  // Reverse so newest events appear at the top
  const visible = [...events]
    .reverse()
    .map((e, i) => ({ i, fmt: formatEvent(e) }))
    .filter((x): x is { i: number; fmt: { text: string; color: string } } => x.fmt !== null)

  return (
    <div className="h-full overflow-y-auto font-mono text-xs bg-gray-950 rounded-lg p-3 space-y-0.5">
      {visible.length === 0 && (
        <p className="text-gray-600 text-center mt-8">Waiting for pipeline run...</p>
      )}
      {visible.map(({ i, fmt }) => (
        <div key={i} className={`${fmt.color} leading-5`}>{fmt.text}</div>
      ))}
    </div>
  )
}
