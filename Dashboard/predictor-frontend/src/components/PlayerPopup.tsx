import { useEffect } from 'react';
import { X } from 'lucide-react';
import PlayerHistory from './PlayerHistory';
import type { PlayerTab } from '../lib/appUrl';

/** The full player view (game log, depth chart, seasons, visuals, news, bio, Vegas) in a popup. */
const PlayerPopup: React.FC<{
  playerId: string;
  compareList: string[];
  onToggleCompare: (id: string) => void;
  onClose: () => void;
  /** Controlled tab, so a link or the assistant can open the card on a given tab. */
  tab?: PlayerTab;
  onTabChange?: (tab: PlayerTab) => void;
}> = ({ playerId, compareList, onToggleCompare, onClose, tab, onTabChange }) => {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      // A story popup opened from this card sits on top; Escape closes that one first.
      if (document.querySelectorAll('[role="dialog"]').length > 1) return;
      onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div
      role="dialog"
      aria-modal="true"
      data-testid="player-modal"
      className="fixed inset-0 z-[60] flex p-4 bg-black/50 dark:bg-black/80 backdrop-blur-sm overflow-y-auto"
      onClick={onClose}
    >
      <div
        className="w-full max-w-5xl m-auto rounded-2xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex justify-end px-3 pt-3">
          <button
            onClick={onClose}
            aria-label="Close"
            className="p-1.5 rounded-full text-slate-500 hover:text-slate-900 dark:hover:text-white hover:bg-slate-200 dark:hover:bg-slate-800 transition"
          >
            <X size={18} />
          </button>
        </div>
        <div className="px-4 pb-4">
          <PlayerHistory
            playerId={playerId}
            onBack={onClose}
            compareList={compareList}
            onToggleCompare={onToggleCompare}
            view={tab}
            onViewChange={onTabChange}
          />
        </div>
      </div>
    </div>
  );
};

export default PlayerPopup;
