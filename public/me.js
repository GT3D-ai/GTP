/* Shared helper: fetch /api/me once and cache on window. Use on every page
   that needs to know who's logged in or whether they can edit a project. */
(function () {
  let mePromise = null;
  function getMe() {
    if (!mePromise) {
      mePromise = fetch("/api/me", { credentials: "same-origin" })
        .then((r) => r.ok ? r.json() : { email: null, authorized: false, isAdmin: false, projects: {} })
        .catch(() => ({ email: null, authorized: false, isAdmin: false, projects: {} }));
    }
    return mePromise;
  }
  async function isAdmin()            { return (await getMe()).isAdmin === true; }
  async function canView(project)     { const m = await getMe(); return m.isAdmin || !!m.projects?.[project]; }
  async function canEdit(project)     { const m = await getMe(); return m.isAdmin || m.projects?.[project] === "editor"; }

  // Render a "user chip" into the given container — email + admin pill + logout.
  async function renderUserChip(el) {
    if (!el) return;
    const m = await getMe();
    if (!m.email) { el.innerHTML = ""; return; }
    const adminPill = m.isAdmin ? '<span class="admin-pill">Admin</span>' : "";
    const initial = (m.name || m.email)[0]?.toUpperCase() || "?";
    el.innerHTML = `
      <div class="user-chip" title="${m.email}">
        <div class="user-chip-avatar">${initial}</div>
        <div class="user-chip-body">
          <div class="user-chip-email">${m.email}</div>
          ${adminPill}
        </div>
      </div>`;
  }

  // Auto-mount: find .site-header .header-inner and append a chip if not
  // already present. Also inject a Users link for admins if the page has a
  // .nav-primary (only on pages that have a nav).
  async function autoMount() {
    const inner = document.querySelector(".site-header .header-inner");
    if (!inner) return;

    // Ensure the chip appears last in the flex row
    let chipEl = inner.querySelector("#userChip");
    if (!chipEl) {
      chipEl = document.createElement("div");
      chipEl.id = "userChip";
      inner.appendChild(chipEl);
    }
    await renderUserChip(chipEl);

    // Admin-only "Users" nav link
    const nav = inner.querySelector(".nav-primary");
    if (nav) {
      const admin = await isAdmin();
      if (admin && !nav.querySelector('a[href="/users.html"]')) {
        const a = document.createElement("a");
        a.href = "/users.html";
        a.textContent = "Users";
        nav.appendChild(a);
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", autoMount);
  } else {
    autoMount();
  }

  window.me = { getMe, isAdmin, canView, canEdit, renderUserChip };
})();
