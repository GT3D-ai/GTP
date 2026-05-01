// Helpers for property-shared assets on category gallery pages.
// Used by project-{images,plans,models,pointclouds,documents,videos}.html
// to render the corner ribbon, gate destructive delete, intercept the
// delete flow with a three-option modal, and toggle per-project hide.
//
// Loaded as a plain script (no module/import) since the gallery pages
// are vanilla; exposes window.sharedAssets.

(function () {
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[c]));
  }

  // Project metadata cache keyed by URL slug. project-info returns the
  // project.json contents — for migrated projects that includes projectId,
  // which we compare against item.ownerProjectId to decide whether the
  // current user can destructively delete a shared item.
  const _metaCache = new Map();
  async function getProjectMeta(project) {
    if (!project) return null;
    if (_metaCache.has(project)) return _metaCache.get(project);
    try {
      const r = await fetch(`/api/project-info?project=${encodeURIComponent(project)}`);
      const data = r.ok ? await r.json() : null;
      _metaCache.set(project, data);
      return data;
    } catch {
      _metaCache.set(project, null);
      return null;
    }
  }

  // True iff the user can destructively delete this shared item.
  // Admin always can. Otherwise, only when the current project IS the
  // owner project — requireProjectRole has already verified editor role
  // on the current project, and the server-side authorizeSharedDelete
  // enforces the same rule.
  async function canDeleteShared(project, item) {
    if (!item || !item.shared) return true;
    const me = window.me ? await window.me.getMe() : null;
    if (me && me.isAdmin) return true;
    const meta = await getProjectMeta(project);
    if (meta && meta.projectId && meta.projectId === item.ownerProjectId) return true;
    return false;
  }

  // Render a small corner ribbon HTML string for a shared item. Returns
  // an empty string if the item isn't shared so callers can blindly
  // template-concat without conditional logic.
  function ribbonHtml(item) {
    if (!item || !item.shared) return "";
    return '<div class="shared-ribbon" title="Shared"></div>';
  }

  // Show the shared-asset delete modal. Resolves to "delete" / "hide" /
  // "cancel". When canDelete is false the destructive option is hidden
  // and the only positive action is per-project hide.
  function confirmSharedDelete({ displayName, canDelete }) {
    return new Promise((resolve) => {
      const overlay = document.createElement("div");
      overlay.className = "shared-delete-overlay";
      overlay.style.cssText = "position:fixed; inset:0; background:rgba(0,0,0,0.55); z-index:9999; display:flex; align-items:center; justify-content:center;";
      const body = canDelete
        ? `<p style="margin:0 0 var(--space-md); color:var(--ink-muted); font-size:0.9375rem;"><strong style="color:var(--ink); font-weight:500;">${esc(displayName)}</strong> is shared with every project on this property. Deleting will remove it everywhere.</p>`
        : `<p style="margin:0 0 var(--space-md); color:var(--ink-muted); font-size:0.9375rem;"><strong style="color:var(--ink); font-weight:500;">${esc(displayName)}</strong> was added by another project on this property &mdash; only its editors can delete it. You can hide it on this project instead.</p>`;
      const deleteBtn = canDelete
        ? `<button type="button" data-act="delete" class="btn" style="background:var(--danger); color:#fff; border-color:var(--danger);">Delete from all projects</button>`
        : "";
      overlay.innerHTML = `
        <div role="dialog" aria-modal="true" style="background:var(--bg-elevated); border:1px solid var(--line); padding:var(--space-lg); max-width:440px; width:90%; box-shadow:0 8px 32px rgba(0,0,0,0.3);">
          <h3 style="margin:0 0 var(--space-sm); font-family:var(--font-display); font-size:1.125rem; font-weight:600; letter-spacing:-0.01em;">Shared asset</h3>
          ${body}
          <div style="display:flex; flex-direction:column; gap:0.5rem;">
            ${deleteBtn}
            <button type="button" data-act="hide" class="btn btn-outline">Hide on this project</button>
            <button type="button" data-act="cancel" class="btn btn-outline" style="border-color:transparent;">Cancel</button>
          </div>
        </div>
      `;
      document.body.appendChild(overlay);
      function close(act) {
        if (overlay.parentNode) document.body.removeChild(overlay);
        document.removeEventListener("keydown", onKey);
        resolve(act);
      }
      function onKey(e) { if (e.key === "Escape") close("cancel"); }
      overlay.addEventListener("click", (e) => {
        const btn = e.target.closest("[data-act]");
        if (btn) return close(btn.dataset.act);
        if (e.target === overlay) close("cancel");
      });
      document.addEventListener("keydown", onKey);
    });
  }

  // POST to /api/project/shared-visibility to add/remove a shared file
  // from this project's hiddenSharedAssets[].
  async function setSharedHidden(project, file, hidden) {
    const r = await fetch("/api/project/shared-visibility", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ project, file, hidden: !!hidden }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || `Shared visibility failed (HTTP ${r.status})`);
    return data;
  }

  window.sharedAssets = {
    getProjectMeta,
    canDeleteShared,
    confirmSharedDelete,
    ribbonHtml,
    setSharedHidden,
  };
})();
