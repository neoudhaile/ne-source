import { useState, useCallback, useRef } from 'react'

export function useDragResize(initialPx: number, min: number, max: number) {
  const [height, setHeight] = useState(initialPx)
  const dragging = useRef(false)
  const startY = useRef(0)
  const startH = useRef(0)

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    dragging.current = true
    startY.current = e.clientY
    startH.current = height

    const onMove = (me: MouseEvent) => {
      if (!dragging.current) return
      // dragging the handle up increases panel height
      const delta = startY.current - me.clientY
      setHeight(Math.min(max, Math.max(min, startH.current + delta)))
    }

    const onUp = () => {
      dragging.current = false
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }, [height, min, max])

  return { height, onMouseDown }
}
