import React, { createContext, useContext, useState, useCallback, useRef } from 'react';

export interface Pick {
  id: string;
  playerId: string;
  playerName: string;
  propType: string;
  line: number;
  choice: 'OVER' | 'UNDER';
  week: number;
  timestamp: number;
  status?: 'PENDING' | 'HIT' | 'MISS' | 'PUSH';
  actual?: number;
}

interface PicksContextType {
  picks: Pick[];
  addPick: (pick: Omit<Pick, 'id' | 'timestamp'>) => void;
  removePick: (id: string) => void;
  getPick: (playerId: string, propType: string, week: number) => Pick | undefined;
  updatePickResult: (id: string, actual: number) => void;
  refreshPickStatuses: (currentWeek: number) => Promise<void>;
}

const PicksContext = createContext<PicksContextType | undefined>(undefined);

// Helper to map propType to the stat field in history
const getStatFromPropType = (propType: string, historyEntry: any): number | null => {
  const pt = propType.toLowerCase();
  if (pt.includes('pass') && pt.includes('yd')) return historyEntry.passing_yds ?? null;
  if (pt.includes('rush') && pt.includes('yd')) return historyEntry.rushing_yds ?? null;
  if (pt.includes('rec') && pt.includes('yd')) return historyEntry.receiving_yds ?? null;
  if (pt.includes('reception')) return historyEntry.receptions ?? null;
  if (pt.includes('pass') && pt.includes('td')) return historyEntry.passing_tds ?? historyEntry.touchdowns ?? null;
  if (pt.includes('pass') && pt.includes('att')) return historyEntry.pass_attempts ?? null;
  if (pt.includes('rush') && pt.includes('att')) return historyEntry.carries ?? null;
  return null;
};

// Determine if pick hit, missed, or pushed
const determineStatus = (choice: 'OVER' | 'UNDER', line: number, actual: number): 'HIT' | 'MISS' | 'PUSH' => {
  if (actual === line) return 'PUSH';
  if (choice === 'OVER') return actual > line ? 'HIT' : 'MISS';
  return actual < line ? 'HIT' : 'MISS';
};

export const PicksProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [picks, setPicks] = useState<Pick[]>(() => {
    try {
      const stored = localStorage.getItem('user_picks');
      return stored ? JSON.parse(stored) : [];
    } catch (e) {
      console.error("Failed to parse picks", e);
      return [];
    }
  });
  
  // Use ref to track if refresh is in progress to avoid loops
  const isRefreshing = useRef(false);
  // Use ref to access latest picks in refresh function without triggering re-creation
  const picksRef = useRef(picks);
  picksRef.current = picks;

  const addPick = useCallback((pick: Omit<Pick, 'id' | 'timestamp'>) => {
    const id = `${pick.playerId}-${pick.propType}-${pick.week}`;
    const newPick: Pick = { ...pick, id, timestamp: Date.now(), status: 'PENDING' };
    
    // Remove existing pick for same prop/player/week if exists
    setPicks(prev => {
      const filtered = prev.filter(p => p.id !== id);
      const updated = [newPick, ...filtered];
      localStorage.setItem('user_picks', JSON.stringify(updated));
      return updated;
    });
  }, []);

  const removePick = useCallback((id: string) => {
    setPicks(prev => {
      const updated = prev.filter(p => p.id !== id);
      localStorage.setItem('user_picks', JSON.stringify(updated));
      return updated;
    });
  }, []);

  const getPick = useCallback((playerId: string, propType: string, week: number) => {
    return picksRef.current.find(p => p.playerId === playerId && p.propType === propType && p.week === week);
  }, []);

  const updatePickResult = useCallback((id: string, actual: number) => {
    setPicks(prev => {
      const updated = prev.map(p => {
        if (p.id !== id) return p;
        const status = determineStatus(p.choice, p.line, actual);
        return { ...p, actual, status };
      });
      localStorage.setItem('user_picks', JSON.stringify(updated));
      return updated;
    });
  }, []);

  // Fetch history for each pending pick and update status
  // Use empty dependency array to create stable function reference
  const refreshPickStatuses = useCallback(async (currentWeek: number) => {
    // Prevent concurrent refreshes
    if (isRefreshing.current) return;
    
    const currentPicks = picksRef.current;
    const pendingPicks = currentPicks.filter(p => p.status === 'PENDING' && p.week < currentWeek);
    if (pendingPicks.length === 0) return;
    
    isRefreshing.current = true;

    const API_BASE_URL = (typeof window !== 'undefined' && (window as any).__env?.API_BASE_URL) || '/api';
    
    const updates: Pick[] = [...currentPicks];
    let hasChanges = false;
    
    for (const pick of pendingPicks) {
      try {
        const res = await fetch(`${API_BASE_URL}/player/history/${pick.playerId}`);
        if (!res.ok) continue;
        
        const history = await res.json();
        const weekEntry = history.find((h: any) => h.week === pick.week);
        
        if (weekEntry) {
          const actual = getStatFromPropType(pick.propType, weekEntry);
          if (actual !== null) {
            const idx = updates.findIndex(p => p.id === pick.id);
            if (idx !== -1) {
              updates[idx] = {
                ...updates[idx],
                actual,
                status: determineStatus(pick.choice, pick.line, actual)
              };
              hasChanges = true;
            }
          }
        }
      } catch (err) {
        console.error(`Failed to fetch history for ${pick.playerId}:`, err);
      }
    }
    
    if (hasChanges) {
      setPicks(updates);
      localStorage.setItem('user_picks', JSON.stringify(updates));
    }
    
    isRefreshing.current = false;
  }, []);

  return (
    <PicksContext.Provider value={{ picks, addPick, removePick, getPick, updatePickResult, refreshPickStatuses }}>
      {children}
    </PicksContext.Provider>
  );
};

export const usePicks = () => {
  const context = useContext(PicksContext);
  if (!context) {
    throw new Error('usePicks must be used within a PicksProvider');
  }
  return context;
};
