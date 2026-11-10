/** 查询前、无匹配记录与首次加载共用的结果占位。 */
export interface ResultStateContent {
  title: string;
  description: string;
}

interface ResultStateProps extends ResultStateContent {
  kind: "idle" | "empty" | "loading";
}

export function ResultState({ kind, title, description }: ResultStateProps) {
  return (
    <div class="result-state" data-kind={kind} role="status" aria-atomic="true">
      <div class="result-state__icon" aria-hidden="true">
        {kind === "loading" ? (
          <svg class="result-state__spinner" viewBox="0 0 40 40" fill="none">
            <circle cx="20" cy="20" r="15" stroke="currentColor" stroke-width="2" opacity="0.16" />
            <path d="M20 5a15 15 0 0 1 15 15" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
          </svg>
        ) : (
          <svg viewBox="0 0 64 64" fill="none" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round">
            <path
              class="result-state__document"
              d="M29 9H17a4 4 0 0 0-4 4v34a4 4 0 0 0 4 4h20a4 4 0 0 0 4-4V21L29 9m0 0v12h12M21 29h12m-12 7h8m-8 7h5"
            />
            <circle cx="43" cy="40" r="10" />
            <path d="m50 47 7 7M39 40h8" />
            {kind === "idle" && <path d="M43 36v8" />}
          </svg>
        )}
      </div>
      <h3 class="result-state__title">{title}</h3>
      <p class="result-state__description">{description}</p>
    </div>
  );
}
