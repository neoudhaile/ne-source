import type { PipelineEvent } from '../types'

function formatEvent(e: PipelineEvent): { text: string; color: string } | null {
  switch (e.type) {
    case 'start':
      return { text: `Pipeline started — ${e.industries?.length} industries × ${e.cities?.length} cities (${e.total_queries} queries)`, color: 'text-blue-400' }
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
    case 'outreach_start':
      return { text: `Pushing ${e.count} leads to Instantly...`, color: 'text-blue-400' }
    case 'outreach_done':
      return {
        text: `Outreach complete — ${e.pushed} pushed, ${e.skipped} skipped (no email), ${e.failed} failed`,
        color: 'text-green-300',
      }
    case 'outreach_error':
      return { text: `Outreach error: ${e.message}`, color: 'text-red-400' }
    case 'search_capped':
      return { text: `Search cap reached — ${e.count} leads inserted`, color: 'text-yellow-400' }
    case 'enrich_start':
      return { text: `Enriching ${e.count} leads...`, color: 'text-blue-400' }
    case 'enrich_lead':
      return { text: `  Enriched ${e.company} (${e.index}/${e.total}) — ${e.sources?.join(', ') || 'no sources'}`, color: 'text-cyan-400' }
    case 'enrich_done':
      return { text: `Enrichment complete — ${e.count} leads enriched`, color: 'text-green-300' }
    case 'enrich_error':
      return { text: `Enrichment error: ${e.message}`, color: 'text-red-400' }
    case 'generate_start':
      return { text: `Generating emails for ${e.count} leads...`, color: 'text-blue-400' }
    case 'generate_lead':
      return { text: `  Email generated for ${e.company} (${e.index}/${e.total})`, color: 'text-purple-400' }
    case 'generate_done':
      return { text: `Email generation complete — ${e.count} emails`, color: 'text-green-300' }
    case 'generate_error':
      return { text: `Email generation error: ${e.message}`, color: 'text-red-400' }
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
