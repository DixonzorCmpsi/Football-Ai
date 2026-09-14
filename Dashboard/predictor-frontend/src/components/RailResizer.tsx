/**
 * Drag-to-resize for the side rails.
 *
 * Each rail keeps its own width, remembered in this browser. The handle is a
 * thin strip on the rail's inner edge: drag it, use the arrow keys when it has
 * focus, or double-click to go back to the default.
 */

import { useEffect, useRef, useState } from 'react';
import { RAIL_DEFAULT, RAIL_MAX, RAIL_MIN } from '../hooks/useRailWidth';

const KEY_STEP = 24;

export default function RailResizer({ side, width, onResize, label }: {
  /** Which rail this handle belongs to; the handle sits on that rail's inner edge. */
  side: 'left' | 'right';
  width: number;
  onResize: (width: number) => void;
  label: string;
}) {
  const [dragging, setDragging] = useState(false);
  const start = useRef<{ x: number; width: number } | null>(null);

  useEffect(() => {
    if (!dragging) return;
    const move = (e: PointerEvent) => {
      if (!start.current) return;
      const dx = e.clientX - start.current.x;
      // The left rail grows as you drag right; the right rail grows as you drag left.
      onResize(start.current.width + (side === 'left' ? dx : -dx));
    };
    const up = () => { setDragging(false); start.current = null; };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
    window.addEventListener('pointercancel', up);
    // Keep the cursor and stop text selection while dragging across the page.
    const { cursor, userSelect } = document.body.style;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    return () => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      window.removeEventListener('pointercancel', up);
      document.body.style.cursor = cursor;
      document.body.style.userSelect = userSelect;
    };
  }, [dragging, side, onResize]);

  return (
    <div
      role="separator"
      aria-orientation="vertical"
      aria-label={label}
      aria-valuemin={RAIL_MIN}
      aria-valuemax={RAIL_MAX}
      aria-valuenow={width}
      tabIndex={0}
      data-testid={`rail-resizer-${side}`}
      title={`${label}: drag, or double-click to reset`}
      onPointerDown={(e) => {
        e.preventDefault();
        start.current = { x: e.clientX, width };
        setDragging(true);
      }}
      onDoubleClick={() => onResize(RAIL_DEFAULT)}
      onKeyDown={(e) => {
        const grow = side === 'left' ? 'ArrowRight' : 'ArrowLeft';
        const shrink = side === 'left' ? 'ArrowLeft' : 'ArrowRight';
        if (e.key === grow) { e.preventDefault(); onResize(width + KEY_STEP); }
        else if (e.key === shrink) { e.preventDefault(); onResize(width - KEY_STEP); }
        else if (e.key === 'Home') { e.preventDefault(); onResize(RAIL_DEFAULT); }
      }}
      className={`group absolute inset-y-0 ${side === 'left' ? '-right-1' : '-left-1'} z-30 w-2 cursor-col-resize outline-none`}
    >
      <span
        className={`absolute inset-y-0 left-1/2 -translate-x-1/2 w-0.5 rounded-full transition-colors ${
          dragging ? 'bg-blue-500' : 'bg-transparent group-hover:bg-blue-400/70 group-focus-visible:bg-blue-500'
        }`}
      />
    </div>
  );
}
