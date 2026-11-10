import { useLocation } from "preact-iso";

export function NotFoundPage() {
  const location = useLocation();

  return (
    <main className="not-found" data-page="not-found">
      <div className="not-found__code" aria-hidden="true">
        404
      </div>
      <div className="not-found__content">
        <p className="not-found__eyebrow">Amberlog 工作台</p>
        <h1 id="not-found-title">页面不存在</h1>
        <p className="not-found__message">
          这个地址没有对应的工作台页面。你可以返回检索首页，或回到刚才浏览的位置。
        </p>
        <div className="not-found__actions">
          <button id="not-found-home" className="primary" type="button" onClick={() => location.route("/ui/")}>
            返回检索
          </button>
          <button
            id="not-found-back"
            type="button"
            onClick={() => {
              if (window.history.length > 1) location.back();
              else location.route("/ui/");
            }}
          >
            返回上一页
          </button>
        </div>
      </div>
    </main>
  );
}
