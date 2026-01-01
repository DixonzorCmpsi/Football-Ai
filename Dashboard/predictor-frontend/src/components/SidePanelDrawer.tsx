import React from 'react';

interface Props { isOpen: boolean; onClose: () => void; children?: React.ReactNode }

export default function SidePanelDrawer({ isOpen, onClose, children }: Props) {
  return (
    <div className={`fixed inset-0 z-50 transition-opacity ${isOpen ? 'opacity-100 pointer-events-auto' : 'opacity-0 pointer-events-none'}`} onClick={onClose}>
      <div className={`absolute right-0 top-0 h-full w-80 bg-white dark:bg-slate-900 border-l border-slate-200 dark:border-slate-700 shadow-lg transform ${isOpen ? 'translate-x-0' : 'translate-x-full'} transition-transform`} onClick={(e)=>e.stopPropagation()}>
        <div className="p-4">
          <button onClick={onClose} className="p-2 text-slate-500 hover:text-blue-600">Close</button>
          <div className="mt-4">{children}</div>
        </div>
      </div>
    </div>
  );
}
