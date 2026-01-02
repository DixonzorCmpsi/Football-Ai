import React, { useState, useEffect } from 'react';
import { getTeamColor } from '../utils/nflColors';
import type { PlayerData, HistoryItem } from '../types';

interface PlayerModalProps {
  player: PlayerData | null;
  onClose: () => void;
}

const PlayerModal: React.FC<PlayerModalProps> = ({ player, onClose }) => {
  const [activeTab, setActiveTab] = useState<'stats' | 'vegas'>('stats');
  const [history, setHistory] = useState<HistoryItem[]>([]);

  const teamColor = player ? getTeamColor(player.team) : '#1e293b';

  useEffect(() => {
    if (player) {
      import('../lib/api').then(({ fetchPlayerHistory }) => {
        fetchPlayerHistory(player.player_id)
          .then(data => setHistory(data))
          .catch(err => console.error(err));
      }).catch(console.error);
    }
  }, [player]);

  if (!player) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 dark:bg-black/90 backdrop-blur-sm" onClick={onClose}>
      <div className="bg-white dark:bg-slate-950 w-full max-w-4xl rounded-2xl border border-slate-200 dark:border-slate-800 shadow-2xl overflow-hidden flex flex-col max-h-[90vh]" onClick={e => e.stopPropagation()}>
        
        {/* Header with Team Gradient */}
        <div 
            className="p-6 border-b border-white/10 flex gap-5 shrink-0 relative"
            style={{ 
                background: `linear-gradient(135deg, ${teamColor} 0%, #1e293b 100%)` 
            }}
        >
          <img src={player.image} className="w-20 h-20 rounded-full border-4 border-white/20 bg-black/20 object-cover shadow-lg relative z-10" />
          
          <div className="flex-1 relative z-10 text-white">
            <h2 className="text-3xl font-black leading-none mb-1 drop-shadow-md">{player.player_name}</h2>
            <p className="opacity-90 font-bold text-sm tracking-wide mb-3 flex items-center gap-2">
                <span className="bg-black/30 px-2 py-0.5 rounded">{player.position}</span>
                <span>{player.team}</span>
            </p>
            <div className="flex gap-6 text-sm">
              <div className="flex flex-col">
                <span className="text-[10px] opacity-60 uppercase font-black">Projection</span>
                <span className="font-mono font-bold text-lg drop-shadow-sm">{player.prediction}</span>
              </div>
              <div className="flex flex-col">
                <span className="text-[10px] opacity-60 uppercase font-black">Avg Pts</span>
                <span className="opacity-90 font-mono font-bold text-lg">{player.average_points}</span>
              </div>
            </div>
          </div>
          <button onClick={onClose} className="absolute top-4 right-4 text-white/50 hover:text-white p-2 transition-colors z-20">✕</button>
        </div>

        {/* Tabs */}
        <div className="flex border-b border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/50 shrink-0">
          <button 
            onClick={() => setActiveTab('stats')}
            className={`flex-1 py-4 text-xs font-black uppercase tracking-widest transition-colors ${activeTab === 'stats' ? 'text-blue-600 dark:text-blue-400 border-b-2 border-blue-500 dark:border-blue-400 bg-white dark:bg-slate-900' : 'text-slate-500 hover:text-slate-800 dark:hover:text-slate-300'}`}
          >
            Game Log
          </button>
          <button 
            onClick={() => setActiveTab('vegas')}
            className={`flex-1 py-4 text-xs font-black uppercase tracking-widest transition-colors ${activeTab === 'vegas' ? 'text-green-600 dark:text-emerald-400 border-b-2 border-green-500 dark:border-emerald-400 bg-white dark:bg-slate-900' : 'text-slate-500 hover:text-slate-800 dark:hover:text-slate-300'}`}
          >
            Vegas Data
          </button>
        </div>

        {/* Content Area */}
        <div className="p-0 overflow-y-auto flex-1 bg-white dark:bg-slate-950 text-slate-800 dark:text-slate-300">
          
          {/* TAB: STATS HISTORY */}
          {activeTab === 'stats' && (
            <table className="w-full text-sm text-left whitespace-nowrap">
              <thead className="text-[10px] uppercase bg-slate-50 dark:bg-slate-900/90 backdrop-blur text-slate-500 dark:text-slate-500 font-bold sticky top-0 z-10 border-b border-slate-200 dark:border-slate-800">
                <tr>
                  <th className="px-4 py-3 sticky left-0 bg-slate-50 dark:bg-slate-900 border-r border-slate-200 dark:border-slate-800 shadow-sm">Wk</th>
                  <th className="px-4 py-3">Opp</th>
                  <th className="px-4 py-3 text-center">Snaps</th>
                  <th className="px-4 py-3 text-center">Rec/Tgt</th>
                  <th className="px-4 py-3 text-center">Rush Yds/Att</th>
                  <th className="px-4 py-3 text-right">Pass Yds</th>
                  <th className="px-4 py-3 text-right">Rec Yds</th>
                  <th className="px-4 py-3 text-right">TDs</th>
                  <th className="px-4 py-3 text-right bg-slate-100 dark:bg-slate-900/50">Pts</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                {history.map((game) => (
                  <tr key={game.week} className="hover:bg-slate-50 dark:hover:bg-slate-900/50 transition-colors">
                    <td className="px-4 py-3 font-mono text-slate-500 sticky left-0 bg-white dark:bg-slate-950 border-r border-slate-100 dark:border-slate-800">W{game.week}</td>
                    <td className="px-4 py-3 font-bold text-slate-700 dark:text-slate-400">{game.opponent}</td>
                    <td className="px-4 py-3 text-center">
                      <div className="flex flex-col items-center">
                        <span className="font-mono font-bold text-slate-700 dark:text-slate-300">{game.snap_percentage ? (game.snap_percentage * 100).toFixed(0) + '%' : '-'}</span>
                        <span className="text-[9px] text-slate-400 font-mono">{game.snap_count} / {game.team_total_snaps || "-"}</span>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-center font-mono">{game.receptions}/{game.targets}</td>
                    <td className="px-4 py-3 text-center font-mono text-slate-500 dark:text-slate-400">{game.rushing_yds || '-'} / {game.carries || '-'}</td>
                    <td className="px-4 py-3 text-right font-mono text-slate-500 dark:text-slate-400">{game.passing_yds || '-'}</td>
                    <td className="px-4 py-3 text-right font-mono text-slate-500 dark:text-slate-400">{game.receiving_yds || '-'}</td>
                    <td className="px-4 py-3 text-right font-mono text-slate-500 dark:text-slate-400">{game.touchdowns !== undefined && game.touchdowns !== null ? game.touchdowns : '-'}</td>
                    <td className="px-4 py-3 text-right bg-slate-50 dark:bg-slate-900/30">
                      <span className={`font-black ${game.points >= 15 ? 'text-green-600 dark:text-green-400' : 'text-slate-800 dark:text-white'}`}>
                        {game.points}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          {/* TAB: VEGAS PROPS */}
          {activeTab === 'vegas' && (
            <div className="p-8 space-y-8">
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-slate-50 dark:bg-slate-900 p-5 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm relative overflow-hidden">
                  <span className="block text-slate-400 dark:text-slate-500 text-xs font-bold uppercase mb-2">Line ({player.prop_line})</span>
                  <div className="flex items-baseline gap-2">
                    <span className={`text-3xl font-black ${player.prop_prob && player.prop_prob >= 55 ? 'text-green-600 dark:text-emerald-400' : 'text-slate-900 dark:text-white'}`}>
                      {player.prop_prob || '--'}%
                    </span>
                    <span className="text-xs font-bold text-slate-400 dark:text-slate-500">OVER PROB</span>
                  </div>
                </div>
                <div className="bg-slate-50 dark:bg-slate-900 p-5 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm relative overflow-hidden">
                  <span className="block text-slate-400 dark:text-slate-500 text-xs font-bold uppercase mb-2">Anytime TD</span>
                  <div className="flex items-baseline gap-2">
                    <span className={`text-3xl font-black ${player.anytime_td_prob && player.anytime_td_prob >= 35 ? 'text-green-600 dark:text-emerald-400' : 'text-slate-900 dark:text-white'}`}>
                      {player.anytime_td_prob || '--'}%
                    </span>
                    <span className="text-xs font-bold text-slate-400 dark:text-slate-500">IMPLIED PROB</span>
                  </div>
                </div>
              </div>
            </div>
          )}

        </div>
      </div>
    </div>
  );
};

export default PlayerModal;