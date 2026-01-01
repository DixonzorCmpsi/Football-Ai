import { useEffect, useState } from 'react';
import { usePlayerHistory } from '../hooks/useNflData';
import type { HistoryEntry } from '../hooks/useNflData';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis 
} from 'recharts';
import { Activity, Hexagon } from 'lucide-react';

interface Props {
  p1Id: string;
  p2Id: string;
  p1Name: string;
  p2Name: string;
  p1Color: string;
  p2Color: string;
  p1OU: number; 
  p2OU: number;
  p1Proj: number;
  p2Proj: number;
}

// --- HELPER 1: Get Opposite Color (Invert Hex for contrast) ---
const getOppositeColor = (hex: string) => {
    const clean = hex.replace('#', '');
    const num = parseInt(clean, 16);
    const inverted = 0xFFFFFF ^ num;
    return '#' + inverted.toString(16).padStart(6, '0');
};

// --- HELPER 2: Ensure Visibility (Brighten dark colors) ---
const ensureContrast = (hex: string) => {
    const c = hex.replace('#', '');
    const r = parseInt(c.substring(0, 2), 16);
    const g = parseInt(c.substring(2, 4), 16);
    const b = parseInt(c.substring(4, 6), 16);
    
    // Perceived brightness formula (standard W3C)
    const brightness = ((r * 299) + (g * 587) + (b * 114)) / 1000;
    
    // If brightness is too low (very dark), return a brighter fallback
    // Threshold of 70 catches dark navy/black/deep purple
    if (brightness < 70) {
        // If it's mainly blue (common in NFL), return bright electric blue
        if (b > r && b > g) return '#60a5fa'; // Tailwind Blue-400
        // If it's mainly red/dark red, return bright red
        if (r > b && r > g) return '#f87171'; // Tailwind Red-400
        // Default bright fallback
        return '#cbd5e1'; // Slate-300
    }
    return hex;
};

export default function ComparisonHistory({ 
    p1Id, p2Id, p1Name, p2Name, p1Color, p2Color, p1OU, p2OU, p1Proj, p2Proj 
}: Props) {
  const { history: h1, loadingHistory: l1 } = usePlayerHistory(p1Id);
  const { history: h2, loadingHistory: l2 } = usePlayerHistory(p2Id);
  
  const [chartData, setChartData] = useState<Array<Record<string, number | string>>>([]);
  const [radarData, setRadarData] = useState<Array<Record<string, number | string>>>([]);
  const [activeTab, setActiveTab] = useState<'LINE' | 'RADAR'>('RADAR'); 

  // 1. First ensure the base colors are visible against dark mode
  const safeP1Color = ensureContrast(p1Color);
  let safeP2Color = ensureContrast(p2Color);

  // 2. Then check for collision (Same Team)
  // We use the ORIGINAL colors to check equality, then assign a high-contrast opposite to P2
  if (p1Color === p2Color) {
      safeP2Color = getOppositeColor(p1Color);
  }

  useEffect(() => {
    if (h1.length > 0 || h2.length > 0) {
      // --- 1. PREPARE LINE CHART DATA ---
      const weeks = Array.from(new Set([...h1.map(x => x.week), ...h2.map(x => x.week)])).sort((a, b) => a - b);
      const merged = weeks.map(week => {
        const stats1 = h1.find(x => x.week === week);
        const stats2 = h2.find(x => x.week === week);
        return {
          week: `Wk ${week}`,
          [p1Name]: stats1?.points || 0,
          [p2Name]: stats2?.points || 0,
        };
      });
      Promise.resolve().then(() => setChartData(merged));

      // --- 2. PREPARE RADAR CHART DATA ---
      const calcStats = (hist: HistoryEntry[], ou: number, proj: number) => {
          if (!hist.length) return { avg: 0, tds: 0, snapPct: 0, script: 0, proj: 0 };
          
          const points = hist.map(h => h.points || 0);
          const snaps = hist.map(h => h.snap_percentage || 0); 
          const touchdowns = hist.map(h => h.touchdowns || 0);

          const avg = points.reduce((a, b) => a + b, 0) / points.length;
          const totalTDs = touchdowns.reduce((a, b) => a + b, 0);
          const avgSnap = (snaps.reduce((a, b) => a + b, 0) / snaps.length) * 100;
          const script = ou || 0;

          return { avg, tds: totalTDs, snapPct: avgSnap, script, proj };
      };

      const s1 = calcStats(h1, p1OU, p1Proj);
      const s2 = calcStats(h2, p2OU, p2Proj);

      // Normalize Touchdowns relative to the higher of the two players (avoid hard-coded multipliers)
      const maxTds = Math.max(s1.tds, s2.tds, 1);

      Promise.resolve().then(() => setRadarData([
        { subject: 'Avg Points', A: Math.min(s1.avg * 4, 100), B: Math.min(s2.avg * 4, 100), fullMark: 100 },
        { subject: 'Touchdowns', A: Math.min((s1.tds / maxTds) * 100, 100), B: Math.min((s2.tds / maxTds) * 100, 100), fullMark: 100 }, 
        { subject: 'Snap %', A: Math.min(s1.snapPct, 100), B: Math.min(s2.snapPct, 100), fullMark: 100 },
        { subject: 'Game Script', A: Math.min((s1.script / 60) * 100, 100), B: Math.min((s2.script / 60) * 100, 100), fullMark: 100 }, 
        { subject: 'Projection', A: Math.min(s1.proj * 4, 100), B: Math.min(s2.proj * 4, 100), fullMark: 100 },
      ]));
    }
  }, [h1, h2, p1Name, p2Name, p1OU, p2OU, p1Proj, p2Proj]);

  if (l1 || l2) return <div className="h-48 flex items-center justify-center text-slate-400 animate-pulse text-xs font-bold uppercase tracking-widest">Crunching Numbers...</div>;

  return (
    <div className="mt-8 animate-in fade-in slide-in-from-bottom-8 duration-700">
      <div className="bg-white dark:bg-slate-800 rounded-2xl shadow-sm border border-slate-200 dark:border-slate-700 p-6 transition-colors duration-300">
        
        <div className="flex items-center justify-between mb-6">
            <h3 className="text-sm font-black text-slate-500 dark:text-slate-400 uppercase tracking-widest">
              Performance Analysis
            </h3>
            <div className="flex bg-slate-100 dark:bg-slate-900 p-1 rounded-lg">
                <button onClick={() => setActiveTab('LINE')} className={`p-2 rounded-md transition-all ${activeTab === 'LINE' ? 'bg-white dark:bg-slate-700 shadow-sm text-blue-600 dark:text-blue-400' : 'text-slate-400'}`}>
                    <Activity size={16} />
                </button>
                <button onClick={() => setActiveTab('RADAR')} className={`p-2 rounded-md transition-all ${activeTab === 'RADAR' ? 'bg-white dark:bg-slate-700 shadow-sm text-blue-600 dark:text-blue-400' : 'text-slate-400'}`}>
                    <Hexagon size={16} />
                </button>
            </div>
        </div>
        
        <div className="h-[350px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            {activeTab === 'LINE' ? (
                <LineChart data={chartData} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#64748b" opacity={0.2} />
                <XAxis dataKey="week" tick={{ fill: '#94a3b8', fontSize: 10, fontWeight: 'bold' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 10, fontWeight: 'bold' }} axisLine={false} tickLine={false} />
                <Tooltip 
                    contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '8px', color: '#fff' }}
                    itemStyle={{ fontSize: '12px', fontWeight: 'bold' }}
                />
                <Legend iconType="circle" wrapperStyle={{ paddingTop: '20px' }} />
                
                {/* Use the SAFE colors calculated above */}
                <Line type="monotone" dataKey={p1Name} stroke={safeP1Color} strokeWidth={4} dot={{ r: 4, fill: safeP1Color, strokeWidth: 2, stroke: '#fff' }} activeDot={{ r: 6 }} />
                <Line type="monotone" dataKey={p2Name} stroke={safeP2Color} strokeWidth={4} dot={{ r: 4, fill: safeP2Color, strokeWidth: 2, stroke: '#fff' }} activeDot={{ r: 6 }} />
                </LineChart>
            ) : (
                <RadarChart cx="50%" cy="50%" outerRadius="80%" data={radarData}>
                    <PolarGrid stroke="#64748b" strokeOpacity={0.3} />
                    <PolarAngleAxis dataKey="subject" tick={{ fill: '#94a3b8', fontSize: 11, fontWeight: 'bold' }} />
                    <PolarRadiusAxis angle={30} domain={[0, 100]} tick={false} axisLine={false} />
                    
                    {/* Use the SAFE colors calculated above */}
                    <Radar name={p1Name} dataKey="A" stroke={safeP1Color} strokeWidth={3} fill={safeP1Color} fillOpacity={0.4} />
                    <Radar name={p2Name} dataKey="B" stroke={safeP2Color} strokeWidth={3} fill={safeP2Color} fillOpacity={0.4} />
                    
                    <Legend iconType="circle" />
                    <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '8px', color: '#fff' }} />
                </RadarChart>
            )}
          </ResponsiveContainer>
        </div>

        <div className="grid grid-cols-2 gap-4 mt-8 border-t border-slate-100 dark:border-slate-700 pt-6">
            <div className="text-center">
                <p className="text-[10px] text-slate-400 dark:text-slate-500 font-bold uppercase">Total Pts</p>
                <p className="text-2xl font-black" style={{ color: safeP1Color }}>
                    {h1.reduce((acc, curr) => acc + (curr.points || 0), 0).toFixed(1)}
                </p>
            </div>
            <div className="text-center">
                <p className="text-[10px] text-slate-400 dark:text-slate-500 font-bold uppercase">Total Pts</p>
                <p className="text-2xl font-black" style={{ color: safeP2Color }}>
                    {h2.reduce((acc, curr) => acc + (curr.points || 0), 0).toFixed(1)}
                </p>
            </div>
        </div>
      </div>
    </div>
  );
}