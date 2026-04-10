import { useState, useRef, useCallback } from 'react'
import { openRunSocket } from '../api'
import type { PipelineEvent } from '../types'

export function usePipelineSocket() {
  const [events, setEvents] = useState<PipelineEvent[]>([])
  const [isRunning, setIsRunning] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)
  const activeRunIdRef = useRef<number | null>(null)

  const connect = useCallback((runId: number, onDone: () => void, resetEvents = false) => {
    if (wsRef.current) {
      wsRef.current.close()
    }

    if (resetEvents) {
      setEvents([])
    }
    setIsRunning(true)
    activeRunIdRef.current = runId

    const ws = openRunSocket(runId, (event) => {
      setEvents(prev => {
        const next = [...prev, event]
        return next.length > 500 ? next.slice(-500) : next
      })

      if (event.type === 'done' || event.type === 'error') {
        setIsRunning(false)
        activeRunIdRef.current = null
        ws.close()
        onDone()
      }
    })

    ws.onclose = () => {
      if (activeRunIdRef.current === runId) {
        setIsRunning(false)
        activeRunIdRef.current = null
        onDone()
      }
    }

    ws.onerror = () => {
      if (activeRunIdRef.current === runId) {
        setIsRunning(false)
      }
    }

    wsRef.current = ws
  }, [])

  return { events, isRunning, connect, activeRunIdRef }
}
