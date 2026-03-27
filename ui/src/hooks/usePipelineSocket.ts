import { useState, useRef, useCallback } from 'react'
import { openRunSocket } from '../api'
import type { PipelineEvent } from '../types'

export function usePipelineSocket() {
  const [events, setEvents] = useState<PipelineEvent[]>([])
  const [isRunning, setIsRunning] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)

  const connect = useCallback((runId: number, onDone: () => void) => {
    setEvents([])
    setIsRunning(true)

    const ws = openRunSocket(runId, (event) => {
      setEvents(prev => {
        const next = [...prev, event]
        return next.length > 500 ? next.slice(-500) : next
      })

      if (event.type === 'done' || event.type === 'error') {
        setIsRunning(false)
        ws.close()
        onDone()
      }
    })

    wsRef.current = ws
  }, [])

  return { events, isRunning, connect }
}
