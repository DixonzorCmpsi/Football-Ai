import React, { useState, useEffect } from 'react';
import { Search, ArrowLeft, ArrowRight, BarChart2, Calendar } from 'lucide-react';
import { Card, CardHeader, CardContent } from '@/components/ui/card'; // Assuming shadcn/ui or similar

// --- MOCK DATA FOR VISUALIZATION (Replace with API calls) ---
const VIEW_MODES = {
  SCHEDULE: 'schedule',
  GAME: 'game',
  LOOKUP: 'lookup',
  COMPARE: 'compare'
};

const Dashboard = () => {
  const [viewMode, setViewMode] = useState(VIEW_MODES.SCHEDULE);
  const [currentWeek, setCurrentWeek] = useState(10); // Default to current
  const [selectedGame, setSelectedGame] = useState(null);

  // --- COMPONENT: The "Player Card" (From your wireframe) ---
  const PlayerCard = ({ player, showActual = false }) => (
    <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 flex flex-col items-center w-full transition-all hover:shadow-md">
      <div className="relative">
        <div className="w-24 h-24 rounded-full overflow-hidden bg-slate-100 border-2 border-slate-100 mb-3">
          <img src={player.image || "/api/placeholder/100/100"} alt={player.name} className="w-full h-full object-cover" />
        </div>
        <div className="absolute bottom-2 right-0 bg-blue-600 text-white text-xs font-bold px-2 py-0.5 rounded-full">
          {player.position}
        </div>
      </div>
      
      <h3 className="font-bold text-slate-800 text-lg leading-tight text-center">{player.name}</h3>
      <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">{player.team}</p>
      
      <div className="w-full bg-slate-50 rounded-lg p-2 flex justify-between items-center">
        <div className="text-center w-1/2 border-r border-slate-200">
          <span className="block text-[10px] text-slate-500 uppercase">Proj</span>
          <span className="block text-xl font-bold text-blue-600">{player.projectedPoints}</span>
        </div>
        <div className="text-center w-1/2">
          <span className="block text-[10px] text-slate-500 uppercase">Actual</span>
          {showActual ? (
             <span className={`block text-xl font-bold ${player.actualPoints >= player.projectedPoints ? 'text-green-500' : 'text-red-500'}`}>
               {player.actualPoints}
             </span>
          ) : (
             <span className="block text-xl font-bold text-slate-300">-</span>
          )}
        </div>
      </div>
    </div>
  );

  // --- VIEW: Schedule Grid ---
  const ScheduleView = () => (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-6">
      {[1,2,3,4].map((i) => ( // Map your real schedule data here
        <div 
          key={i}
          onClick={() => { setSelectedGame(i); setViewMode(VIEW_MODES.GAME); }} 
          className="bg-white p-4 rounded-lg border border-slate-200 cursor-pointer hover:border-blue-400 hover:shadow-md transition-all group"
        >
          <div className="flex justify-between items-center mb-2">
            <span className="font-bold text-slate-700">KC</span>
            <span className="text-xs text-slate-400 font-mono">@</span>
            <span className="font-bold text-slate-700">BUF</span>
          </div>
          <div className="text-xs text-slate-500 text-center group-hover:text-blue-500">View Matchup Analysis &rarr;</div>
        </div>
      ))}
    </div>
  );

  // --- VIEW: The "Broadcaster" Matchup Deep Dive ---
  const MatchupDeepDive = () => (
    <div className="p-6 space-y-8 animate-in fade-in zoom-in duration-300">
      <button 
        onClick={() => setViewMode(VIEW_MODES.SCHEDULE)}
        className="flex items-center text-sm text-slate-500 hover:text-slate-800 mb-4"
      >
        <ArrowLeft className="w-4 h-4 mr-1" /> Back to Schedule
      </button>

      <div className="text-center mb-8">
        <h2 className="text-2xl font-black text-slate-900 tracking-tight">THE GENERALS</h2>
        <p className="text-slate-500 text-sm mb-4">Top Quarterbacks</p>
        <div className="grid grid-cols-2 gap-6 max-w-lg mx-auto">
          {/* Use real data from /matchup endpoint */}
          <PlayerCard player={{name: "Patrick Mahomes", position: "QB", team: "KC", projectedPoints: 24.5}} />
          <PlayerCard player={{name: "Josh Allen", position: "QB", team: "BUF", projectedPoints: 22.1}} />
        </div>
      </div>

      <div className="border-t border-slate-100 pt-8">
        <h2 className="text-xl font-black text-slate-900 tracking-tight mb-4">THE BALL CARRIERS</h2>
        <div className="flex overflow-x-auto gap-4 pb-4">
           {/* Map Top 3 RBs here */}
           <div className="min-w-[200px]"><PlayerCard player={{name: "Isiah Pacheco", position: "RB", team: "KC", projectedPoints: 18.2}} /></div>
           <div className="min-w-[200px]"><PlayerCard player={{name: "James Cook", position: "RB", team: "BUF", projectedPoints: 15.4}} /></div>
           <div className="min-w-[200px]"><PlayerCard player={{name: "R. Johnson", position: "RB", team: "CHI", projectedPoints: 12.1}} /></div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="flex h-screen bg-slate-50 font-sans text-slate-900 overflow-hidden">
      
      {/* --- LEFT COLUMN: The Reality (Past) --- */}
      <aside className="w-64 bg-white border-r border-slate-200 flex flex-col hidden lg:flex">
        <div className="p-4 border-b border-slate-100 bg-slate-50">
          <h2 className="text-xs font-bold text-slate-400 uppercase tracking-widest">Last Week's Reality</h2>
          <div className="text-sm font-semibold text-slate-700">Week {currentWeek - 1} Top Performers</div>
        </div>
        <div className="flex-1 overflow-y-auto p-4 space-y-3">
          {/* Map GET /rankings/past/{week} here */}
          <div className="flex items-center justify-between p-2 bg-slate-50 rounded border-l-4 border-green-500">
            <div>
              <div className="font-bold text-sm">L. Jackson</div>
              <div className="text-xs text-slate-500">QB • BAL</div>
            </div>
            <div className="text-right">
              <div className="font-bold text-green-600">32.4</div>
              <div className="text-[10px] text-slate-400">Proj: 24.0</div>
            </div>
          </div>
        </div>
      </aside>

      {/* --- MIDDLE COLUMN: The Stage --- */}
      <main className="flex-1 flex flex-col relative min-w-0">
        {/* Navbar */}
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-6 z-10 shadow-sm">
          <div className="font-black text-xl tracking-tighter italic">NITTANY<span className="text-blue-600">ANALYTICS</span></div>
          <div className="flex gap-2">
            <button 
              onClick={() => setViewMode(VIEW_MODES.LOOKUP)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${viewMode === VIEW_MODES.LOOKUP ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`}
            >
              <Search className="w-4 h-4 inline mr-2"/>Player Lookup
            </button>
            <button 
              onClick={() => setViewMode(VIEW_MODES.COMPARE)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${viewMode === VIEW_MODES.COMPARE ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`}
            >
              <BarChart2 className="w-4 h-4 inline mr-2"/>Compare
            </button>
          </div>
        </header>

        {/* Dynamic Content Area */}
        <div className="flex-1 overflow-y-auto bg-white/50 scroll-smooth">
          {viewMode === VIEW_MODES.SCHEDULE && <ScheduleView />}
          {viewMode === VIEW_MODES.GAME && <MatchupDeepDive />}
          {viewMode === VIEW_MODES.LOOKUP && <div className="p-10 text-center text-slate-400">Lookup Search Bar UI Here</div>}
          {viewMode === VIEW_MODES.COMPARE && <div className="p-10 text-center text-slate-400">Comparison Split View UI Here</div>}
        </div>
      </main>

      {/* --- RIGHT COLUMN: The Prediction (Future) --- */}
      <aside className="w-64 bg-white border-l border-slate-200 flex flex-col hidden lg:flex">
        <div className="p-4 border-b border-slate-100 bg-slate-50">
          <h2 className="text-xs font-bold text-slate-400 uppercase tracking-widest">Crystal Ball</h2>
          <div className="text-sm font-semibold text-slate-700">Week {currentWeek} Projections</div>
        </div>
        <div className="flex-1 overflow-y-auto p-4 space-y-3">
          {/* Map GET /rankings/future/{week} here */}
          {[1,2,3,4,5,6,7,8,9,10].map((r) => (
            <div key={r} className="flex items-center gap-3 p-2 border-b border-slate-50 last:border-0">
              <div className="font-mono text-sm font-bold text-slate-300 w-4">#{r}</div>
              <div className="flex-1">
                <div className="font-bold text-sm">C. McCaffrey</div>
                <div className="text-xs text-slate-500">RB • SF</div>
              </div>
              <div className="font-bold text-blue-600">22.5</div>
            </div>
          ))}
        </div>
      </aside>

    </div>
  );
};

export default Dashboard;