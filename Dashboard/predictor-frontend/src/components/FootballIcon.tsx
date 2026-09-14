/**
 * The assistant's mark: a football, tilted like it's mid-throw.
 *
 * Motion lives in index.css (`.football-*`): a spiral when the button under it
 * is hovered or focused, a steady spin while the assistant is working, and
 * nothing at all for people who ask for reduced motion.
 */

export default function FootballIcon({ size = 22, spinning = false }: { size?: number; spinning?: boolean }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      aria-hidden="true"
      className={`football-icon ${spinning ? 'football-icon--spinning' : ''}`}
    >
      {/* Ball: a lens pointing corner to corner. */}
      <path
        d="M20.2 3.8c.9 4.4-.6 9.1-4 12.5s-8.1 4.9-12.5 4C2.8 15.9 4.3 11.2 7.7 7.8s8.1-4.9 12.5-4Z"
        fill="#8a4b24"
        stroke="#fff"
        strokeWidth="1.4"
        strokeLinejoin="round"
      />
      {/* Stripes near each tip. */}
      <path d="M17.3 4.1c.6.9 1.2 1.6 2.6 2.6M4.1 17.3c.9.6 1.6 1.2 2.6 2.6" stroke="#fff" strokeWidth="1.3" strokeLinecap="round" fill="none" />
      {/* Laces. */}
      <g className="football-laces" stroke="#fff" strokeLinecap="round" fill="none">
        <path d="M9.2 14.8l5.6-5.6" strokeWidth="1.5" />
        <path d="M10.2 11.9l1.9 1.9M11.8 10.3l1.9 1.9M13.4 8.7l1.3 1.3M8.6 13.5l1.3 1.3" strokeWidth="1.2" />
      </g>
    </svg>
  );
}
