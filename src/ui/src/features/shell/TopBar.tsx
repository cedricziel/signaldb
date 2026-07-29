import "./TopBar.css";

export function TopBar() {
  return (
    <header className="topbar">
      <span className="topbar-mark">
        <svg width="18" height="14" viewBox="0 0 18 14" fill="none" aria-hidden="true">
          <path
            d="M1 7 L4 7 L6 2 L9 12 L12 4 L13.5 7 L17 7"
            stroke="var(--accent)"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
        signal<b>db</b>
      </span>
    </header>
  );
}
