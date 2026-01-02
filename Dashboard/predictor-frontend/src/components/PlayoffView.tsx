import { useState } from 'react';
import { getTeamColor } from '../utils/nflColors';
import { Trophy } from 'lucide-react';
import { useSchedule } from '../hooks/useNflData';
import type { ScheduleGame } from '../hooks/useNflData';

// Team to Conference Mapping
const TEAM_CONFERENCE: Record<string, 'AFC' | 'NFC'> = {
    'BAL': 'AFC', 'BUF': 'AFC', 'CIN': 'AFC', 'CLE': 'AFC', 
    'DEN': 'AFC', 'HOU': 'AFC', 'IND': 'AFC', 'JAX': 'AFC', 
    'KC': 'AFC', 'LAC': 'AFC', 'LV': 'AFC', 'MIA': 'AFC', 
    'NE': 'AFC', 'NYJ': 'AFC', 'PIT': 'AFC', 'TEN': 'AFC',
    'ARI': 'NFC', 'ATL': 'NFC', 'CAR': 'NFC', 'CHI': 'NFC', 
    'DAL': 'NFC', 'DET': 'NFC', 'GB': 'NFC', 'LA': 'NFC', 'LAR': 'NFC',
    'MIN': 'NFC', 'NO': 'NFC', 'NYG': 'NFC', 'PHI': 'NFC', 
    'SEA': 'NFC', 'SF': 'NFC', 'TB': 'NFC', 'WAS': 'NFC'
};

interface PlayoffGameProps {
    game?: ScheduleGame;
    label: string;
    isSuperBowl?: boolean;
}

const PlayoffGameCard = ({ game, label, isSuperBowl }: PlayoffGameProps) => {
    if (!game) {
        return (
            <div className={`flex flex-col justify-center p-3 border border-slate-200 dark:border-slate-700 rounded-lg bg-slate-50 dark:bg-slate-800/50 h-24 ${isSuperBowl ? 'h-32 border-yellow-400/50' : ''}`}>
                <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">{label}</span>
                <div className="text-xs text-slate-300 dark:text-slate-600 font-mono">TBD vs TBD</div>
            </div>
        );
    }

    const homeColor = getTeamColor(game.home_team);
    // const awayColor = getTeamColor(game.away_team);

    return (
        <div className={`relative flex flex-col border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 overflow-hidden shadow-sm hover:shadow-md transition-shadow ${isSuperBowl ? 'h-32 ring-1 ring-yellow-500/30' : 'h-24'}`}>
            <div className="absolute top-0 left-0 bottom-0 w-1" style={{ backgroundColor: homeColor }}></div>
            
            <div className="p-2 flex-1 flex flex-col justify-center">
                <span className="text-[9px] font-black text-slate-400 uppercase tracking-wider mb-1">{label}</span>
                
                {/* Home Team */}
                <div className="flex justify-between items-center mb-1">
                    <div className="flex items-center gap-2">
                        <span className="font-black text-sm">{game.home_team}</span>
                        <span className="text-[10px] text-slate-400">({game.moneyline_home || '-'})</span>
                    </div>
                    <span className="font-mono font-bold">{game.home_score !== null ? game.home_score : ''}</span>
                </div>

                {/* Away Team */}
                <div className="flex justify-between items-center">
                    <div className="flex items-center gap-2">
                        <span className="font-black text-sm">{game.away_team}</span>
                        <span className="text-[10px] text-slate-400">({game.moneyline_away || '-'})</span>
                    </div>
                    <span className="font-mono font-bold">{game.away_score !== null ? game.away_score : ''}</span>
                </div>
            </div>
            
            {game.gametime && (
                <div className="bg-slate-50 dark:bg-slate-900/50 px-2 py-1 text-[9px] text-slate-400 text-right">
                    {game.gameday} • {game.gametime}
                </div>
            )}
        </div>
    );
};

const PlayoffView = () => {
    const [conference, setConference] = useState<'NFC' | 'AFC'>('NFC');
    
    // Fetch all playoff weeks
    const { games: wcGames } = useSchedule(19);
    const { games: divGames } = useSchedule(20);
    const { games: confGames } = useSchedule(21);
    const { games: sbGames } = useSchedule(22);

    // Filter by conference (except Super Bowl which is both)
    const filterByConf = (games: ScheduleGame[]) => games.filter(g => 
        TEAM_CONFERENCE[g.home_team] === conference || TEAM_CONFERENCE[g.away_team] === conference
    );

    const currentWc = filterByConf(wcGames);
    const currentDiv = filterByConf(divGames);
    const currentConf = filterByConf(confGames);
    
    // Super Bowl is always shown
    const superBowl = sbGames[0];

    return (
        <div className="max-w-6xl mx-auto p-4">
            {/* Header / Toggle */}
            <div className="flex items-center justify-between mb-8">
                <div className="flex items-center gap-2">
                    <Trophy className="text-yellow-500" size={24} />
                    <h1 className="text-2xl font-black italic tracking-tighter">PLAYOFF BRACKET</h1>
                </div>
                
                <div className="flex bg-slate-200 dark:bg-slate-800 p-1 rounded-lg">
                    <button 
                        onClick={() => setConference('NFC')}
                        className={`px-4 py-1.5 rounded-md text-xs font-black transition-all ${conference === 'NFC' ? 'bg-blue-700 text-white shadow' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
                    >
                        NFC
                    </button>
                    <button 
                        onClick={() => setConference('AFC')}
                        className={`px-4 py-1.5 rounded-md text-xs font-black transition-all ${conference === 'AFC' ? 'bg-red-600 text-white shadow' : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'}`}
                    >
                        AFC
                    </button>
                </div>
            </div>

            {/* Bracket Grid */}
            <div className="grid grid-cols-4 gap-4 md:gap-8 relative">
                {/* Round 1: Wild Card */}
                <div className="space-y-8 flex flex-col justify-center">
                    <h3 className="text-xs font-bold text-slate-400 uppercase tracking-widest text-center mb-4">Wild Card</h3>
                    {/* 3 Games per conference usually */}
                    <PlayoffGameCard game={currentWc[0]} label="WC 1" />
                    <PlayoffGameCard game={currentWc[1]} label="WC 2" />
                    <PlayoffGameCard game={currentWc[2]} label="WC 3" />
                </div>

                {/* Round 2: Divisional */}
                <div className="space-y-16 flex flex-col justify-center pt-12">
                    <h3 className="text-xs font-bold text-slate-400 uppercase tracking-widest text-center mb-4">Divisional</h3>
                    <PlayoffGameCard game={currentDiv[0]} label="DIV 1" />
                    <PlayoffGameCard game={currentDiv[1]} label="DIV 2" />
                </div>

                {/* Round 3: Conference */}
                <div className="flex flex-col justify-center">
                    <h3 className="text-xs font-bold text-slate-400 uppercase tracking-widest text-center mb-4">Conf. Champ</h3>
                    <PlayoffGameCard game={currentConf[0]} label={`${conference} Championship`} />
                </div>

                {/* Round 4: Super Bowl */}
                <div className="flex flex-col justify-center">
                    <h3 className="text-xs font-bold text-yellow-500 uppercase tracking-widest text-center mb-4">Super Bowl</h3>
                    <PlayoffGameCard game={superBowl} label="Super Bowl LIX" isSuperBowl />
                </div>
                
                {/* Connecting Lines (Visual Decoration) */}
                <div className="absolute inset-0 pointer-events-none hidden md:block -z-10">
                    {/* Add SVG lines here if needed for true bracket look, 
                        but grid layout gives a decent structure already */}
                </div>
            </div>
            
            <div className="mt-12 text-center">
                <p className="text-xs text-slate-400">
                    * Playoff matchups will populate automatically as the schedule is updated.
                </p>
            </div>
        </div>
    );
};

export default PlayoffView;
