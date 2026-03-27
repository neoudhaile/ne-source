import { useState, useEffect, useCallback } from 'react'
import { Menu, Play, Loader2, GripHorizontal } from 'lucide-react'
import { triggerRun, getRuns, getStats, getStatus, getConfig } from './api'
import { usePipelineSocket } from './hooks/usePipelineSocket'
import { useDragResize } from './hooks/useDragResize'
import { PipelineGraph } from './components/PipelineGraph'
import { LiveFeed } from './components/LiveFeed'
import { LeadStats } from './components/LeadStats'
import { RunHistoryDrawer } from './components/RunHistoryDrawer'
import { ConfigPanel } from './components/ConfigPanel'
import { LeadViewer } from './components/LeadViewer'
import type { RunRecord, Stats, Config, StatusResponse, PipelineEvent } from './types'

export default function App() {
  const [runs, setRuns] = useState<RunRecord[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [config, setConfig] = useState<Config | null>(null)
  const [status, setStatus] = useState<StatusResponse>({ is_running: false, active_run_id: null, next_run_at: null })
  const [showHistory, setShowHistory] = useState(false)
  const [showConfig, setShowConfig] = useState(false)
  const [showLeads, setShowLeads] = useState(false)
  const [triggering, setTriggering] = useState(false)

  const { events, isRunning, connect } = usePipelineSocket()
  const { height: panelHeight, onMouseDown: onDragStart } = useDragResize(360, 80, 700)

  const refresh = useCallback(async () => {
    const [r, s, st, cfg] = await Promise.all([getRuns(), getStats(), getStatus(), getConfig()])
    setRuns(r)
    setStats(s)
    setStatus(st)
    setConfig(cfg)
  }, [])

  useEffect(() => { refresh() }, [refresh])

  async function handleRun() {
    setTriggering(true)
    try {
      const { run_id } = await triggerRun()
      setStatus(s => ({ ...s, is_running: true, active_run_id: run_id }))
      connect(run_id, () => {
        setStatus(s => ({ ...s, is_running: false, active_run_id: null }))
        refresh()
      })
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : 'Failed to start run')
    } finally {
      setTriggering(false)
    }
  }

  const running = isRunning || status.is_running
  const progress = (() => {
    const last = [...events].reverse().find(e => e.type === 'progress' || e.type === 'search')
    if (!last) return null
    return { index: last.index ?? 0, total: last.total ?? 1 }
  })()

  function cadenceLabel() {
    if (running) return (
      <span className="flex items-center gap-1.5 text-blue-400 text-sm">
        <span className="w-2 h-2 rounded-full bg-blue-400 animate-pulse inline-block" /> Running
      </span>
    )
    const lastDone = events.findLast((e: PipelineEvent) => e.type === 'done')
    if (lastDone) return <span className="text-gray-500 text-sm">Last run complete — {lastDone.inserted} leads added</span>
    if (status.next_run_at) return <span className="text-gray-500 text-sm">Next run: {status.next_run_at}</span>
    return <span className="text-gray-600 text-sm">No schedule set</span>
  }

  return (
    <div className="h-screen bg-gray-950 text-white flex flex-col overflow-hidden">
      {/* Header */}
      <header className="border-b border-gray-800 px-4 py-3 flex items-center justify-between flex-shrink-0">
        <div className="flex items-center gap-3">
          <button onClick={() => setShowHistory(true)} className="text-gray-400 hover:text-white p-1">
            <Menu size={20} />
          </button>
          <span className="font-semibold text-white">Ne'Source</span>
        </div>
        <div className="flex-1 flex justify-center">{cadenceLabel()}</div>
        <div className="flex items-center gap-2">
        <button
          onClick={() => setShowLeads(true)}
          className="text-gray-400 hover:text-white text-sm px-3 py-2 rounded-lg border border-gray-700 hover:border-gray-500 transition-colors"
        >Leads</button>
        <button
          onClick={handleRun}
          disabled={running || triggering}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-medium px-4 py-2 rounded-lg transition-colors"
        >
          {running || triggering
            ? <><Loader2 size={15} className="animate-spin" /> Running</>
            : <><Play size={15} /> Run Pipeline</>}
        </button>
        </div>
      </header>

      {/* Graph section — fills remaining space above the bottom panel */}
      <div className="flex-1 min-h-0 relative">
        <PipelineGraph events={events} onConfigClick={() => setShowConfig(true)} />
        {progress && (
          <div className="absolute bottom-4 left-6 right-6 z-10">
            <div className="flex justify-between text-xs text-gray-500 mb-1">
              <span>Progress</span>
              <span>{progress.index} / {progress.total} queries</span>
            </div>
            <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
              <div
                className="h-full bg-blue-500 rounded-full transition-all duration-500"
                style={{ width: `${(progress.index / progress.total) * 100}%` }}
              />
            </div>
          </div>
        )}
      </div>

      {/* Drag handle */}
      <div
        onMouseDown={onDragStart}
        className="flex-shrink-0 h-4 flex items-center justify-center cursor-row-resize hover:bg-gray-800 transition-colors group"
      >
        <GripHorizontal size={16} className="text-gray-600 group-hover:text-gray-400 transition-colors" />
      </div>

      {/* Bottom panel — resizable */}
      <div
        className="flex-shrink-0 grid grid-cols-2 gap-4 px-6 pb-6"
        style={{ height: panelHeight }}
      >
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-2 font-semibold">Live Feed</div>
          <div className="flex-1 min-h-0">
            <LiveFeed events={events} />
          </div>
        </div>
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-2 font-semibold">Lead Stats</div>
          <div className="flex-1 min-h-0 overflow-y-auto bg-gray-900 rounded-lg p-4">
            <LeadStats stats={stats} />
          </div>
        </div>
      </div>

      {/* Overlays */}
      {showHistory && <RunHistoryDrawer runs={runs} onClose={() => setShowHistory(false)} />}
      {showConfig && config && (
        <ConfigPanel
          config={config}
          onClose={() => setShowConfig(false)}
          onSave={(updated) => setConfig(updated)}
        />
      )}
      {showLeads && <LeadViewer onClose={() => setShowLeads(false)} />}
    </div>
  )
}
