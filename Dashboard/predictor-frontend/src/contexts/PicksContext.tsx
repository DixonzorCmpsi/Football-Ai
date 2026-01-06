import React, { createContext, useContext, useState } from 'react';

export interface Pick {
  id: string;
  playerId: string;
  playerName: string;
  propType: string;
  line: number;
  choice: 'OVER' | 'UNDER';
  week: number;
  timestamp: number;
  status?: 'PENDING' | 'HIT' | 'MISS';
  actual?: number;
}

interface PicksContextType {
  picks: Pick[];
  addPick: (pick: Omit<Pick, 'id' | 'timestamp'>) => void;
  removePick: (id: string) => void;
  getPick: (playerId: string, propType: string, week: number) => Pick | undefined;
}

const PicksContext = createContext<PicksContextType | undefined>(undefined);

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

  const savePicks = (newPicks: Pick[]) => {
    setPicks(newPicks);
    localStorage.setItem('user_picks', JSON.stringify(newPicks));
  };

  const addPick = (pick: Omit<Pick, 'id' | 'timestamp'>) => {
    const id = `${pick.playerId}-${pick.propType}-${pick.week}`;
    const newPick: Pick = { ...pick, id, timestamp: Date.now(), status: 'PENDING' };
    
    // Remove existing pick for same prop/player/week if exists
    const filtered = picks.filter(p => p.id !== id);
    savePicks([newPick, ...filtered]);
  };

  const removePick = (id: string) => {
    savePicks(picks.filter(p => p.id !== id));
  };

  const getPick = (playerId: string, propType: string, week: number) => {
    return picks.find(p => p.playerId === playerId && p.propType === propType && p.week === week);
  };

  return (
    <PicksContext.Provider value={{ picks, addPick, removePick, getPick }}>
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
