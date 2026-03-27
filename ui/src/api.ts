import type { RunRecord, Stats, Config, StatusResponse, PipelineEvent } from './types'

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
  return res.json()
}

export async function getLead(id: number): Promise<Record<string, unknown>> {
  const res = await fetch(`/api/leads/${id}`)
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
