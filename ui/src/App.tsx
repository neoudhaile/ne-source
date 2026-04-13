import { useState, useEffect, useCallback, useRef } from 'react'
import { Menu, Play, Pause, Loader2, GripHorizontal, Upload } from 'lucide-react'
import { triggerRun, getRuns, getStatus, getConfig, uploadCsv, pauseRun, resumeRun, downloadTier1Export } from './api'
import { usePipelineSocket } from './hooks/usePipelineSocket'
import { useDragResize } from './hooks/useDragResize'
import { PipelineGraph } from './components/PipelineGraph'
import { LiveFeed } from './components/LiveFeed'
import { LeadsTable } from './components/LeadsTable'
import { RunHistoryDrawer } from './components/RunHistoryDrawer'
import { ConfigPanel } from './components/ConfigPanel'
import { LeadViewer } from './components/LeadViewer'
import type { RunRecord, Config, StatusResponse, PipelineEvent } from './types'

export default function App() {
  const [runs, setRuns] = useState<RunRecord[]>([])
  const [config, setConfig] = useState<Config | null>(null)
  const [status, setStatus] = useState<StatusResponse>({ is_running: false, is_paused: false, active_run_id: null, next_run_at: null })
  const [showHistory, setShowHistory] = useState(false)
  const [showConfig, setShowConfig] = useState(false)
  const [showLeads, setShowLeads] = useState(false)
  const [triggering, setTriggering] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [togglingPause, setTogglingPause] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [fundsAlert, setFundsAlert] = useState<{ balance: number; estimated_cost: number } | null>(null)
  const fundsDismissedRef = useRef(false)

  const { events, isRunning, connect, activeRunIdRef } = usePipelineSocket()
  const { height: panelHeight, onMouseDown: onDragStart } = useDragResize(360, 80, 700)

  const refresh = useCallback(async () => {
    const [r, st, cfg] = await Promise.all([getRuns(), getStatus(), getConfig()])
    setRuns(r)
    setStatus(st)
    setConfig(cfg)
  }, [])

  useEffect(() => { refresh() }, [refresh])

  useEffect(() => {
    if (!status.is_running || status.active_run_id == null) return
    if (activeRunIdRef.current === status.active_run_id) return

    connect(status.active_run_id, () => {
      setStatus(s => ({ ...s, is_running: false, is_paused: false, active_run_id: null }))
      refresh()
    }, false)
  }, [status.is_running, status.active_run_id, connect, refresh, activeRunIdRef])

  useEffect(() => {
    if (fundsDismissedRef.current) return
    const fundsEvent = events.find(e => e.type === 'insufficient_funds')
    if (fundsEvent && fundsEvent.balance !== undefined && fundsEvent.estimated_cost !== undefined && !fundsAlert) {
      setFundsAlert({ balance: fundsEvent.balance, estimated_cost: fundsEvent.estimated_cost })
      pauseRun().then(() => setStatus(s => ({ ...s, is_paused: true }))).catch(() => {})
    }
  }, [events])

  async function handleRun() {
    setTriggering(true)
    try {
      const { run_id } = await triggerRun()
      fundsDismissedRef.current = false
      setStatus(s => ({ ...s, is_running: true, is_paused: false, active_run_id: run_id }))
      connect(run_id, () => {
        setStatus(s => ({ ...s, is_running: false, is_paused: false, active_run_id: null }))
        refresh()
      })
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : 'Failed to start run')
    } finally {
      setTriggering(false)
    }
  }

  async function handleCsvUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    setUploading(true)
    try {
      const result = await uploadCsv(file)
      if (result.run_id == null) {
        await refresh()
        alert(result.message || `CSV import complete: ${result.inserted} inserted, ${result.skipped} skipped.`)
        return
      }
      // Connect to WebSocket for real-time enrichment progress
      fundsDismissedRef.current = false
      setStatus(s => ({ ...s, is_running: true, is_paused: false, active_run_id: result.run_id }))
      connect(result.run_id, () => {
        setStatus(s => ({ ...s, is_running: false, is_paused: false, active_run_id: null }))
        refresh()
      })
    } catch (err: unknown) {
      alert(err instanceof Error ? err.message : 'CSV upload failed')
    } finally {
      setUploading(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  async function handlePauseToggle() {
    setTogglingPause(true)
    try {
      const nextStatus = status.is_paused ? await resumeRun() : await pauseRun()
      setStatus(nextStatus)
    } catch (err: unknown) {
      alert(err instanceof Error ? err.message : 'Failed to update run state')
    } finally {
      setTogglingPause(false)
    }
  }

  const running = isRunning || status.is_running
  const paused = status.is_running && status.is_paused
  const progress = (() => {
    const last = [...events].reverse().find(e => e.type === 'progress' || e.type === 'search')
    if (!last) return null
    return { index: last.index ?? 0, total: last.total ?? 1 }
  })()

  function cadenceLabel() {
    if (paused) return (
      <span className="flex items-center gap-1.5 text-yellow-400 text-sm">
        <span className="w-2 h-2 rounded-full bg-yellow-400 inline-block" /> Paused
      </span>
    )
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
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv"
          onChange={handleCsvUpload}
          className="hidden"
        />
        <button
          onClick={() => fileInputRef.current?.click()}
          disabled={uploading}
          className="flex items-center gap-1.5 text-gray-400 hover:text-white text-sm px-3 py-2 rounded-lg border border-gray-700 hover:border-gray-500 disabled:opacity-50 transition-colors"
        >
          {uploading
            ? <><Loader2 size={14} className="animate-spin" /> Importing...</>
            : <><Upload size={14} /> Upload CSV</>}
        </button>
        <button
          onClick={() => setShowLeads(true)}
          className="text-gray-400 hover:text-white text-sm px-3 py-2 rounded-lg border border-gray-700 hover:border-gray-500 transition-colors"
        >Leads</button>
        {running && (
          <button
            onClick={handlePauseToggle}
            disabled={togglingPause}
            className="flex items-center gap-1.5 text-gray-400 hover:text-white text-sm px-3 py-2 rounded-lg border border-gray-700 hover:border-gray-500 disabled:opacity-50 transition-colors"
          >
            {togglingPause
              ? <><Loader2 size={14} className="animate-spin" /> {paused ? 'Resuming...' : 'Pausing...'}</>
              : paused
                ? <><Play size={14} /> Resume</>
                : <><Pause size={14} /> Pause</>}
          </button>
        )}
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
        className="flex-shrink-0 grid gap-4 px-6 pb-6"
        style={{ height: panelHeight, gridTemplateColumns: '1fr 2fr' }}
      >
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-2 font-semibold">Live Feed</div>
          <div className="flex-1 min-h-0">
            <LiveFeed events={events} />
          </div>
        </div>
        <div className="flex flex-col min-h-0">
          <div className="text-xs text-gray-500 uppercase tracking-wide mb-2 font-semibold">Leads</div>
          <div className="flex-1 min-h-0">
            <LeadsTable events={events} runId={status.active_run_id} />
          </div>
        </div>
      </div>

      {/* Overlays */}
      {showHistory && (
        <RunHistoryDrawer
          runs={runs}
          onClose={() => setShowHistory(false)}
          onDownloadTier1={(runId) => downloadTier1Export(runId)}
        />
      )}
      {showConfig && config && (
        <ConfigPanel
          config={config}
          onClose={() => setShowConfig(false)}
          onSave={(updated) => setConfig(updated)}
        />
      )}
      {showLeads && <LeadViewer onClose={() => setShowLeads(false)} />}

      {fundsAlert && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
          <div className="bg-gray-900 border border-red-500/50 rounded-2xl p-8 max-w-md mx-4 text-center space-y-4">
            <div className="text-4xl">💸</div>
            <h2 className="text-xl font-bold text-red-400">get ur money up</h2>
            <p className="text-gray-300">
              You don't have enough USDC in your Base wallet to complete this run.
            </p>
            <div className="bg-gray-800 rounded-lg p-4 space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-400">Balance:</span>
                <span className="text-red-400 font-mono">${fundsAlert.balance.toFixed(2)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Estimated need:</span>
                <span className="text-white font-mono">~${fundsAlert.estimated_cost.toFixed(2)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Wallet:</span>
                <span className="text-gray-500 font-mono text-xs">0x254f...FBC1</span>
              </div>
            </div>
            <p className="text-gray-500 text-xs">
              Fund your wallet or continue with partial enrichment.
            </p>
            <div className="flex gap-3 justify-center mt-2">
              <button
                onClick={() => {
                  fundsDismissedRef.current = true
                  setFundsAlert(null)
                  resumeRun().then(() => setStatus(s => ({ ...s, is_paused: false }))).catch(() => {})
                }}
                className="px-6 py-2 bg-emerald-700 hover:bg-emerald-600 text-white rounded-lg transition-colors"
              >
                Continue Anyway
              </button>
              <button
                onClick={() => {
                  fundsDismissedRef.current = true
                  setFundsAlert(null)
                }}
                className="px-6 py-2 bg-gray-700 hover:bg-gray-600 text-white rounded-lg transition-colors"
              >
                Dismiss
              </button>
            </div>
          </div>
        </div>
      )}

    </div>
  )
}
