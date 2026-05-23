import { Shield } from 'lucide-react';
import { getTeamColor } from '../utils/nflColors';

type Conference = 'AFC' | 'NFC';

interface TeamInfo {
  abbr: string;
  name: string;
  conference: Conference;
  division: string;
}

const TEAMS: TeamInfo[] = [
  { abbr: 'BUF', name: 'Buffalo Bills', conference: 'AFC', division: 'East' },
  { abbr: 'MIA', name: 'Miami Dolphins', conference: 'AFC', division: 'East' },
  { abbr: 'NE', name: 'New England Patriots', conference: 'AFC', division: 'East' },
  { abbr: 'NYJ', name: 'New York Jets', conference: 'AFC', division: 'East' },
  { abbr: 'BAL', name: 'Baltimore Ravens', conference: 'AFC', division: 'North' },
  { abbr: 'CIN', name: 'Cincinnati Bengals', conference: 'AFC', division: 'North' },
  { abbr: 'CLE', name: 'Cleveland Browns', conference: 'AFC', division: 'North' },
  { abbr: 'PIT', name: 'Pittsburgh Steelers', conference: 'AFC', division: 'North' },
  { abbr: 'HOU', name: 'Houston Texans', conference: 'AFC', division: 'South' },
  { abbr: 'IND', name: 'Indianapolis Colts', conference: 'AFC', division: 'South' },
  { abbr: 'JAX', name: 'Jacksonville Jaguars', conference: 'AFC', division: 'South' },
  { abbr: 'TEN', name: 'Tennessee Titans', conference: 'AFC', division: 'South' },
  { abbr: 'DEN', name: 'Denver Broncos', conference: 'AFC', division: 'West' },
  { abbr: 'KC', name: 'Kansas City Chiefs', conference: 'AFC', division: 'West' },
  { abbr: 'LAC', name: 'Los Angeles Chargers', conference: 'AFC', division: 'West' },
  { abbr: 'LV', name: 'Las Vegas Raiders', conference: 'AFC', division: 'West' },
  { abbr: 'DAL', name: 'Dallas Cowboys', conference: 'NFC', division: 'East' },
  { abbr: 'NYG', name: 'New York Giants', conference: 'NFC', division: 'East' },
  { abbr: 'PHI', name: 'Philadelphia Eagles', conference: 'NFC', division: 'East' },
  { abbr: 'WAS', name: 'Washington Commanders', conference: 'NFC', division: 'East' },
  { abbr: 'CHI', name: 'Chicago Bears', conference: 'NFC', division: 'North' },
  { abbr: 'DET', name: 'Detroit Lions', conference: 'NFC', division: 'North' },
  { abbr: 'GB', name: 'Green Bay Packers', conference: 'NFC', division: 'North' },
  { abbr: 'MIN', name: 'Minnesota Vikings', conference: 'NFC', division: 'North' },
  { abbr: 'ATL', name: 'Atlanta Falcons', conference: 'NFC', division: 'South' },
  { abbr: 'CAR', name: 'Carolina Panthers', conference: 'NFC', division: 'South' },
  { abbr: 'NO', name: 'New Orleans Saints', conference: 'NFC', division: 'South' },
  { abbr: 'TB', name: 'Tampa Bay Buccaneers', conference: 'NFC', division: 'South' },
  { abbr: 'ARI', name: 'Arizona Cardinals', conference: 'NFC', division: 'West' },
  { abbr: 'LA', name: 'Los Angeles Rams', conference: 'NFC', division: 'West' },
  { abbr: 'SEA', name: 'Seattle Seahawks', conference: 'NFC', division: 'West' },
  { abbr: 'SF', name: 'San Francisco 49ers', conference: 'NFC', division: 'West' },
];

const CONFERENCES: Conference[] = ['AFC', 'NFC'];
const DIVISIONS = ['East', 'North', 'South', 'West'];

interface Props {
  onOpenTeam: (team: string) => void;
}

const TeamCard = ({ team, onOpenTeam }: { team: TeamInfo; onOpenTeam: (team: string) => void }) => {
  const color = getTeamColor(team.abbr);

  return (
    <button
      onClick={() => onOpenTeam(team.abbr)}
      className="group relative overflow-hidden rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-left shadow-sm transition hover:-translate-y-0.5 hover:border-blue-300 dark:hover:border-blue-600 hover:shadow-md focus:outline-none focus:ring-2 focus:ring-blue-500/40"
    >
      <div className="absolute inset-x-0 top-0 h-1.5" style={{ backgroundColor: color }} />
      <div className="p-4 pt-5">
        <div className="flex items-center justify-between gap-3">
          <div
            className="w-11 h-11 rounded-lg flex items-center justify-center text-white text-base font-black shrink-0 shadow-sm"
            style={{ backgroundColor: color }}
          >
            {team.abbr}
          </div>
          <div className="w-8 h-8 rounded-lg bg-slate-50 dark:bg-slate-900/60 border border-slate-200 dark:border-slate-700 flex items-center justify-center text-slate-400 group-hover:text-blue-500 transition">
            <Shield size={15} />
          </div>
        </div>

        <div className="mt-3 min-w-0">
          <div className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
            {team.conference} {team.division}
          </div>
          <div className="mt-1 text-sm font-black text-slate-800 dark:text-slate-100 truncate">
            {team.name}
          </div>
        </div>
      </div>
    </button>
  );
};

const TeamsView = ({ onOpenTeam }: Props) => {
  return (
    <div className="max-w-7xl mx-auto">
      <div className="flex items-center gap-2 mb-5">
        <Shield className="text-blue-600 dark:text-blue-400" size={19} />
        <h1 className="text-xl md:text-2xl font-black italic tracking-tighter text-slate-900 dark:text-slate-100">
          TEAMS
        </h1>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
        {CONFERENCES.map((conference) => (
          <section key={conference} className="space-y-4">
            <div className="flex items-center justify-between border-b border-slate-200 dark:border-slate-800 pb-2">
              <h2 className="text-xs font-black uppercase tracking-widest text-slate-500 dark:text-slate-400">
                {conference}
              </h2>
              <span className="text-[10px] font-mono text-slate-400 dark:text-slate-500">16</span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {DIVISIONS.map((division) => {
                const teams = TEAMS.filter((t) => t.conference === conference && t.division === division);
                return (
                  <div key={`${conference}-${division}`} className="space-y-2">
                    <div className="text-[10px] font-black uppercase tracking-widest text-slate-400 dark:text-slate-500">
                      {division}
                    </div>
                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-1 2xl:grid-cols-2 gap-2">
                      {teams.map((team) => (
                        <TeamCard key={team.abbr} team={team} onOpenTeam={onOpenTeam} />
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          </section>
        ))}
      </div>
    </div>
  );
};

export default TeamsView;
