import { Download, X } from 'lucide-react'
import type { RunRecord } from '../types'

function duration(run: RunRecord): string {
  if (!run.finished_at) return '—'
  const ms = new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()
  const s = Math.floor(ms / 1000)
  return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`
}

function StatusDot({ status }: { status: RunRecord['status'] }) {
  if (status === 'running') return <span className="inline-block w-2 h-2 rounded-full bg-blue-400 animate-pulse" />
  if (status === 'completed') return <span className="inline-block w-2 h-2 rounded-full bg-green-400" />
  return <span className="inline-block w-2 h-2 rounded-full bg-red-400" />
}

export function RunHistoryDrawer({
  runs,
  onClose,
  onDownloadTier1,
}: {
  runs: RunRecord[]
  onClose: () => void
  onDownloadTier1: (runId: number) => void
}) {
  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed left-0 top-0 h-full w-80 bg-gray-900 border-r border-gray-700 z-50 flex flex-col">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h2 className="text-white font-semibold">Run History</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X size={18} />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-3 space-y-3">
          {runs.length === 0 && (
            <p className="text-gray-600 text-sm text-center mt-8">No runs yet</p>
          )}
          {runs.map(run => (
            <div key={run.id} className="bg-gray-800 rounded-lg p-3 space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-gray-300 text-sm font-medium">Run #{run.id}</span>
                <div className="flex items-center gap-1.5">
                  <StatusDot status={run.status} />
                  <span className="text-xs text-gray-400">{run.status}</span>
                </div>
              </div>
              <div className="text-xs text-gray-500">
                {new Date(run.started_at).toLocaleString('en-US', {
                  month: 'short', day: 'numeric', year: 'numeric',
                  hour: '2-digit', minute: '2-digit',
                })}
                {' · '}{duration(run)}
              </div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                <div className="text-gray-400">Inserted <span className="text-green-400 font-medium">{run.inserted}</span></div>
                <div className="text-gray-400">Total leads <span className="text-white font-medium">{run.total_leads ?? '—'}</span></div>
                <div className="text-gray-400">Geo skip <span className="text-gray-300">{run.skipped_geo.toLocaleString()}</span></div>
                <div className="text-gray-400">Dupes <span className="text-gray-300">{run.skipped_dupe}</span></div>
                {run.cost != null && run.cost > 0 && (
                  <div className="text-gray-400">Cost <span className="text-amber-400 font-medium">${Number(run.cost).toFixed(2)}</span></div>
                )}
              </div>
              {run.error_message && (
                <div className="text-red-400 text-xs">{run.error_message}</div>
              )}
              {run.status === 'completed' && (
                <button
                  onClick={() => onDownloadTier1(run.id)}
                  className="mt-2 inline-flex items-center gap-1.5 text-xs text-gray-300 hover:text-white border border-gray-600 hover:border-gray-400 rounded px-2 py-1 transition-colors"
                >
                  <Download size={12} />
                  Download Tier 1 Brief
                </button>
              )}
            </div>
          ))}
        </div>
      </div>
    </>
  )
}
