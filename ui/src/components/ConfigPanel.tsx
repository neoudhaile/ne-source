import { useState } from 'react'
import { X, Plus, Trash2 } from 'lucide-react'
import { updateConfig } from '../api'
import type { Config } from '../types'

export function ConfigPanel({ config, onClose, onSave }: {
  config: Config
  onClose: () => void
  onSave: (c: Config) => void
}) {
  const [industries, setIndustries] = useState<string[]>(config.industries)
  const [cities, setCities] = useState<string[]>(config.cities)
  const [minReviews, setMinReviews] = useState(config.min_reviews)
  const [minRating, setMinRating] = useState(config.min_rating)
  const [geoRadius, setGeoRadius] = useState(config.geo_radius_miles)
  const [maxLeads, setMaxLeads] = useState(config.max_leads_per_run)
  const [newIndustry, setNewIndustry] = useState('')
  const [newCity, setNewCity] = useState('')
  const [saving, setSaving] = useState(false)

  async function save() {
    setSaving(true)
    try {
      const updated = await updateConfig({
        industries,
        cities,
        min_reviews: minReviews,
        min_rating: minRating,
        geo_radius_miles: geoRadius,
        max_leads_per_run: maxLeads,
      })
      onSave(updated)
      onClose()
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed right-0 top-0 h-full w-96 bg-gray-900 border-l border-gray-700 z-50 flex flex-col">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h2 className="text-white font-semibold">Pipeline Config</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white"><X size={18} /></button>
        </div>

        <div className="flex-1 overflow-y-auto p-4 space-y-6">
          {/* Industries */}
          <div>
            <h3 className="text-gray-300 font-medium mb-2 text-sm">Industries</h3>
            <div className="space-y-1.5">
              {industries.map((ind, i) => (
                <div key={i} className="flex items-center gap-2 bg-gray-800 rounded px-3 py-1.5">
                  <span className="flex-1 text-gray-200 text-sm">{ind}</span>
                  <button onClick={() => setIndustries(industries.filter((_, j) => j !== i))}
                    className="text-gray-500 hover:text-red-400"><Trash2 size={14} /></button>
                </div>
              ))}
            </div>
            <div className="flex gap-2 mt-2">
              <input value={newIndustry} onChange={e => setNewIndustry(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter' && newIndustry.trim()) { setIndustries([...industries, newIndustry.trim()]); setNewIndustry('') }}}
                placeholder="Add industry..." className="flex-1 bg-gray-800 text-white text-sm rounded px-3 py-1.5 border border-gray-700 focus:outline-none focus:border-blue-500" />
              <button onClick={() => { if (newIndustry.trim()) { setIndustries([...industries, newIndustry.trim()]); setNewIndustry('') }}}
                className="text-blue-400 hover:text-blue-300"><Plus size={18} /></button>
            </div>
          </div>

          {/* Cities */}
          <div>
            <h3 className="text-gray-300 font-medium mb-2 text-sm">Cities</h3>
            <div className="space-y-1.5">
              {cities.map((city, i) => (
                <div key={i} className="flex items-center gap-2 bg-gray-800 rounded px-3 py-1.5">
                  <span className="flex-1 text-gray-200 text-sm">{city}</span>
                  <button onClick={() => setCities(cities.filter((_, j) => j !== i))}
                    className="text-gray-500 hover:text-red-400"><Trash2 size={14} /></button>
                </div>
              ))}
            </div>
            <div className="flex gap-2 mt-2">
              <input value={newCity} onChange={e => setNewCity(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter' && newCity.trim()) { setCities([...cities, newCity.trim()]); setNewCity('') }}}
                placeholder="Add city (e.g. Pasadena, CA)..." className="flex-1 bg-gray-800 text-white text-sm rounded px-3 py-1.5 border border-gray-700 focus:outline-none focus:border-blue-500" />
              <button onClick={() => { if (newCity.trim()) { setCities([...cities, newCity.trim()]); setNewCity('') }}}
                className="text-blue-400 hover:text-blue-300"><Plus size={18} /></button>
            </div>
          </div>

          {/* Filter Settings */}
          <div>
            <h3 className="text-gray-300 font-medium mb-2 text-sm">Filter Settings</h3>
            <div className="space-y-2">
              {([
                { label: 'Min reviews', value: minReviews, set: setMinReviews, step: 1, min: 0, max: 500 },
                { label: 'Min rating', value: minRating, set: setMinRating, step: 0.1, min: 0, max: 5 },
                { label: 'Geo radius (miles)', value: geoRadius, set: setGeoRadius, step: 1, min: 1, max: 200 },
                { label: 'Max leads per run', value: maxLeads, set: setMaxLeads, step: 1, min: 1, max: 500 },
              ] as const).map(({ label, value, set, step, min, max }) => (
                <div key={label} className="flex items-center gap-2 bg-gray-800 rounded px-3 py-1.5">
                  <span className="flex-1 text-gray-400 text-sm">{label}</span>
                  <input
                    type="number"
                    value={value}
                    step={step}
                    min={min}
                    max={max}
                    onChange={e => (set as (v: number) => void)(parseFloat(e.target.value))}
                    className="w-20 bg-gray-700 text-white text-sm rounded px-2 py-1 border border-gray-600 focus:outline-none focus:border-blue-500 text-right"
                  />
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="px-4 py-3 border-t border-gray-700">
          <button onClick={save} disabled={saving}
            className="w-full bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white rounded py-2 text-sm font-medium">
            {saving ? 'Saving...' : 'Save Changes'}
          </button>
        </div>
      </div>
    </>
  )
}
