// Everything above the route tree. The update banner lives here, not in the
// App shell, so no route can hide it: not the login or consent pages, and
// not the error page a crash swaps in for the whole shell.
import type { ComponentProps } from "react";
import { RouterProvider } from "react-router";
import { UpdateBanner } from "./features/shell/UpdateBanner";

export function AppRoot({
  router,
}: {
  router: ComponentProps<typeof RouterProvider>["router"];
}) {
  return (
    <>
      <UpdateBanner />
      <div className="app-root-main">
        <RouterProvider router={router} />
      </div>
    </>
  );
}
