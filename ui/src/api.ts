import type { RunRecord, Stats, Config, StatusResponse, PipelineEvent, CsvUploadResponse } from './types'

export async function triggerRun(): Promise<{ run_id: number }> {
  const res = await fetch('/api/runs', { method: 'POST' })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function getRuns(limit = 20): Promise<RunRecord[]> {
  const res = await fetch(`/api/runs?limit=${limit}`)
  return res.json()
}

export async function getStats(): Promise<Stats> {
  const res = await fetch('/api/stats')
  return res.json()
}

export async function getStatus(): Promise<StatusResponse> {
  const res = await fetch('/api/status')
  return res.json()
}

export async function pauseRun(): Promise<StatusResponse> {
  const res = await fetch('/api/runs/pause', { method: 'POST' })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function resumeRun(): Promise<StatusResponse> {
  const res = await fetch('/api/runs/resume', { method: 'POST' })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function getConfig(): Promise<Config> {
  const res = await fetch('/api/config')
  return res.json()
}

export async function updateConfig(payload: {
  industries?: string[]
  cities?: string[]
  min_reviews?: number
  min_rating?: number
  geo_radius_miles?: number
  max_leads_per_run?: number
}): Promise<Config> {
  const res = await fetch('/api/config', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  return res.json()
}

export async function getLeads(limit = 50, offset = 0): Promise<Record<string, unknown>[]> {
  const res = await fetch(`/api/leads?limit=${limit}&offset=${offset}`)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function getLead(id: number): Promise<Record<string, unknown>> {
  const res = await fetch(`/api/leads/${id}`)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function getRunLeads(runId: number): Promise<Record<string, unknown>[]> {
  const res = await fetch(`/api/runs/${runId}/leads`)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function uploadCsv(file: File): Promise<CsvUploadResponse> {
  const form = new FormData()
  form.append('file', file)
  const res = await fetch('/api/upload-csv', { method: 'POST', body: form })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text)
  }
  return res.json()
}

export function openRunSocket(runId: number, onEvent: (e: PipelineEvent) => void): WebSocket {
  const ws = new WebSocket(`ws://127.0.0.1:8000/ws/runs/${runId}`)
  ws.onmessage = (msg) => {
    const event = JSON.parse(msg.data) as PipelineEvent
    if (event.type !== 'ping') onEvent(event)
  }
  return ws
}

export function downloadTier1Export(runId: number) {
  window.open(`/api/runs/${runId}/tier1-export`, '_blank')
}

export function downloadRunLogs(runId: number) {
  window.open(`/api/runs/${runId}/logs`, '_blank')
}
