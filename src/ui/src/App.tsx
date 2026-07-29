import { TopBar } from "./features/shell/TopBar";

export function App() {
  return (
    <div className="app-frame">
      <TopBar />
      <main className="app-main">
        <p className="placeholder">
          Explore is under construction — logs, traces, and metrics land here.
        </p>
      </main>
    </div>
  );
}
