// Inline copy of public/favicon.svg — inlined (rather than <img src>) so it
// can sit in the login page's brand row without an extra request and adopt
// currentColor if the mark ever needs to follow the theme.

export function BrandMark() {
  return (
    <svg viewBox="0 0 18 18" aria-hidden="true">
      <rect width="18" height="18" rx="4" fill="#14181e" />
      <path
        d="M2 9 L5 9 L7 4 L10 14 L13 6 L14.5 9 L16 9"
        stroke="#f58a3c"
        strokeWidth="1.8"
        fill="none"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
