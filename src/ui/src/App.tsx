import { ExploreView } from "./features/explore/ExploreView";
import { TopBar } from "./features/shell/TopBar";

export function App() {
  return (
    <div className="app-frame">
      <TopBar />
      <main className="app-main">
        <ExploreView />
      </main>
    </div>
  );
}
