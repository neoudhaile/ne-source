import type { Stats } from '../types'

function Bar({ count, max, color }: { count: number; max: number; color: string }) {
  const pct = max > 0 ? (count / max) * 100 : 0
  return (
    <div className="flex-1 bg-gray-800 rounded-sm h-2 overflow-hidden">
      <div className={`h-full ${color} rounded-sm`} style={{ width: `${pct}%` }} />
    </div>
  )
}

export function LeadStats({ stats }: { stats: Stats | null }) {
  if (!stats) return <div className="text-gray-600 text-sm text-center mt-8">Loading stats...</div>

  const maxIndustry = Math.max(...stats.by_industry.map(r => r.count), 1)
  const maxOwnership = Math.max(...stats.by_ownership_type.map(r => r.count), 1)

  return (
    <div className="space-y-6 text-sm">
      <div className="text-center">
        <div className="text-5xl font-bold text-white">{stats.total_leads.toLocaleString()}</div>
        <div className="text-gray-500 mt-1">total leads</div>
      </div>

      <div>
        <div className="text-gray-400 font-semibold mb-2 uppercase tracking-wide text-xs">By Industry</div>
        <div className="space-y-1.5">
          {stats.by_industry.map(row => (
            <div key={row.industry} className="flex items-center gap-2">
              <span className="text-gray-400 w-32 truncate text-xs">{row.industry}</span>
              <Bar count={row.count} max={maxIndustry} color="bg-blue-500" />
              <span className="text-gray-300 w-6 text-right text-xs">{row.count}</span>
            </div>
          ))}
        </div>
      </div>

      <div>
        <div className="text-gray-400 font-semibold mb-2 uppercase tracking-wide text-xs">By Ownership</div>
        <div className="space-y-1.5">
          {stats.by_ownership_type.map(row => (
            <div key={row.ownership_type} className="flex items-center gap-2">
              <span className={`w-24 text-xs ${row.ownership_type === 'FAMILY' ? 'text-amber-400 font-semibold' : 'text-gray-400'}`}>
                {row.ownership_type}
              </span>
              <Bar count={row.count} max={maxOwnership} color={row.ownership_type === 'FAMILY' ? 'bg-amber-500' : 'bg-blue-500'} />
              <span className="text-gray-300 w-6 text-right text-xs">{row.count}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
