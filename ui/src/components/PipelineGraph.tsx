import { ReactFlow, Background, Handle, Position, useNodesState, useEdgesState, type NodeTypes, type Node } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import type { NodeState, PipelineEvent } from '../types'

interface PipelineNodeData extends Record<string, unknown> {
  label: string
  state: NodeState
  subText?: string
  isFirst?: boolean
  isLast?: boolean
}

function PipelineNode({ data }: { data: PipelineNodeData }) {
  const borderColor = {
    idle: 'border-gray-600',
    active: 'border-blue-500 node-active',
    complete: 'border-green-500',
    error: 'border-red-500',
  }[data.state]

  const labelColor = {
    idle: 'text-gray-400',
    active: 'text-blue-300',
    complete: 'text-green-400',
    error: 'text-red-400',
  }[data.state]

  return (
    <div
      style={data.isFirst ? { pointerEvents: 'all', cursor: 'pointer' } : undefined}
      className={`border-2 ${borderColor} rounded-xl px-5 py-3 bg-gray-900 w-[150px] text-center transition-all duration-300${data.isFirst ? ' hover:bg-gray-800 hover:border-gray-400' : ''}`}
    >
      {!data.isFirst && <Handle type="target" position={Position.Left} style={{ background: '#4b5563', border: 'none' }} />}
      <div className={`font-semibold text-sm ${labelColor}`}>{data.label}</div>
      <div className="h-8 flex items-center justify-center mt-1">
        <div className="text-gray-500 text-xs leading-4 w-full">{data.subText ?? ''}</div>
      </div>
      {!data.isLast && <Handle type="source" position={Position.Right} style={{ background: '#4b5563', border: 'none' }} />}
    </div>
  )
}

const nodeTypes: NodeTypes = { pipeline: PipelineNode as NodeTypes[string] }

interface NodeStates {
  config: NodeState; search: NodeState; enrich: NodeState; generate: NodeState; outreach: NodeState; done: NodeState
  searchSubText: string; enrichSubText: string; generateSubText: string; outreachSubText: string
}

function getNodeStates(events: PipelineEvent[]): NodeStates {
  const idle: NodeStates = {
    config: 'idle', search: 'idle', enrich: 'idle', generate: 'idle', outreach: 'idle', done: 'idle',
    searchSubText: '', enrichSubText: '', generateSubText: '', outreachSubText: '',
  }
  const last = events[events.length - 1]
  if (!last) return idle

  const type = last.type

  if (type === 'done')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'complete', outreach: 'complete', done: 'active',
      searchSubText: '', enrichSubText: '', generateSubText: '', outreachSubText: '' }
  if (type === 'outreach_done')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'complete', outreach: 'complete', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: '', outreachSubText: `${last.pushed} pushed` }
  if (type === 'outreach_error')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'complete', outreach: 'error', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: '', outreachSubText: 'Push failed' }
  if (type === 'outreach_start')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'complete', outreach: 'active', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: '', outreachSubText: `Pushing ${last.count} leads...` }

  // Generate stage
  if (type === 'generate_done')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'complete', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: `${last.count} emails`, outreachSubText: '' }
  if (type === 'generate_error')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'active', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: 'Error', outreachSubText: '' }
  if (type === 'generate_lead')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'active', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: `${last.index}/${last.total} emails`, outreachSubText: '' }
  if (type === 'generate_start')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'active', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: `Generating ${last.count}...`, outreachSubText: '' }

  // Enrich stage
  if (type === 'enrich_done')
    return { config: 'complete', search: 'complete', enrich: 'complete', generate: 'idle', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: `${last.count} enriched`, generateSubText: '', outreachSubText: '' }
  if (type === 'enrich_error')
    return { config: 'complete', search: 'complete', enrich: 'active', generate: 'idle', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: 'Error', generateSubText: '', outreachSubText: '' }
  if (type === 'enrich_lead')
    return { config: 'complete', search: 'complete', enrich: 'active', generate: 'idle', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: `${last.index}/${last.total} leads`, generateSubText: '', outreachSubText: '' }
  if (type === 'enrich_start')
    return { config: 'complete', search: 'complete', enrich: 'active', generate: 'idle', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: `Enriching ${last.count}...`, generateSubText: '', outreachSubText: '' }

  if (type === 'error')
    return { config: 'complete', search: 'error', enrich: 'idle', generate: 'idle', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: '', outreachSubText: '' }
  if (type === 'start')
    return { config: 'active', search: 'idle', enrich: 'idle', generate: 'idle', outreach: 'idle', done: 'idle',
      searchSubText: '', enrichSubText: '', generateSubText: '', outreachSubText: '' }
  if (type === 'search_capped')
    return { config: 'complete', search: 'complete', enrich: 'idle', generate: 'idle', outreach: 'idle', done: 'idle',
      searchSubText: `Capped at ${last.count}`, enrichSubText: '', generateSubText: '', outreachSubText: '' }

  const subMap: Record<string, string> = {
    search:      `Searching (${last.index}/${last.total})`,
    results:     `Found ${last.count} results`,
    geo:         'Geo filtering...',
    normalizing: 'Normalizing...',
    inserting:   'Inserting to database...',
    insert:      'Inserting to database...',
    skip_dupe:   'Inserting to database...',
    progress:    `Batch done (${last.index}/${last.total})`,
  }

  return { config: 'complete', search: 'active', enrich: 'idle', generate: 'idle', outreach: 'idle', done: 'idle',
    searchSubText: subMap[type] ?? '', enrichSubText: '', generateSubText: '', outreachSubText: '' }
}

type PNode = Node<PipelineNodeData>

export function PipelineGraph({ events, onConfigClick }: {
  events: PipelineEvent[]
  onConfigClick: () => void
}) {
  const s = getNodeStates(events)

  const initNodes: PNode[] = [
    { id: 'config',   type: 'pipeline', position: { x: 0,   y: 0 }, data: { label: 'Config',   state: 'idle', subText: 'Click to configure', isFirst: true } },
    { id: 'search',   type: 'pipeline', position: { x: 200, y: 0 }, data: { label: 'Search',   state: 'idle', subText: '' } },
    { id: 'enrich',   type: 'pipeline', position: { x: 400, y: 0 }, data: { label: 'Enrich',   state: 'idle', subText: '' } },
    { id: 'generate', type: 'pipeline', position: { x: 600, y: 0 }, data: { label: 'Generate', state: 'idle', subText: '' } },
    { id: 'outreach', type: 'pipeline', position: { x: 800, y: 0 }, data: { label: 'Outreach', state: 'idle', subText: '' } },
    { id: 'done',     type: 'pipeline', position: { x: 1000, y: 0 }, data: { label: 'Done',    state: 'idle', isLast: true } },
  ]

  const initEdges = [
    { id: 'e1', source: 'config',   target: 'search',   style: { stroke: '#4b5563' } },
    { id: 'e2', source: 'search',   target: 'enrich',   style: { stroke: '#4b5563' } },
    { id: 'e3', source: 'enrich',   target: 'generate', style: { stroke: '#4b5563' } },
    { id: 'e4', source: 'generate', target: 'outreach', style: { stroke: '#4b5563' } },
    { id: 'e5', source: 'outreach', target: 'done',     style: { stroke: '#4b5563' } },
  ]

  const [nodes, , onNodesChange] = useNodesState<PNode>(initNodes)
  const [edges, , onEdgesChange] = useEdgesState(initEdges)

  const updatedNodes: PNode[] = nodes.map(n => {
    if (n.id === 'config')   return { ...n, data: { ...n.data, state: s.config } }
    if (n.id === 'search')   return { ...n, data: { ...n.data, state: s.search,   subText: s.searchSubText } }
    if (n.id === 'enrich')   return { ...n, data: { ...n.data, state: s.enrich,   subText: s.enrichSubText } }
    if (n.id === 'generate') return { ...n, data: { ...n.data, state: s.generate, subText: s.generateSubText } }
    if (n.id === 'outreach') return { ...n, data: { ...n.data, state: s.outreach, subText: s.outreachSubText } }
    if (n.id === 'done')     return { ...n, data: { ...n.data, state: s.done } }
    return n
  })

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={updatedNodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        onNodeClick={(_e, node) => { if (node.id === 'config') onConfigClick() }}
        fitView
        fitViewOptions={{ padding: 0.4 }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        zoomOnScroll={false}
        panOnDrag={true}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1f2937" gap={20} />
      </ReactFlow>
    </div>
  )
}
