// Heading + hint + content, layout-agnostic: the standalone login page
// (LoginRoute) wraps it in a <section>. `as="h1"` is the page's single
// top-level heading.

import type { ReactNode } from "react";

interface Props {
  title: string;
  hint?: string;
  as?: "h1" | "h2";
  children: ReactNode;
}

export function LoginCard({ title, hint, as = "h2", children }: Props) {
  const Heading = as;
  return (
    <>
      <Heading>{title}</Heading>
      {hint && <p className="login-hint">{hint}</p>}
      {children}
    </>
  );
}
