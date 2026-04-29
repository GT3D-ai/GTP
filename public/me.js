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

  // Render a "user chip" into the given container — just the user's
  // display name (falls back to email when no name is set on the
  // roster). Email is kept on the title attribute so admins can still
  // hover to see the underlying account.
  async function renderUserChip(el) {
    if (!el) return;
    const m = await getMe();
    if (!m.email) { el.innerHTML = ""; return; }
    const label = (m.name && String(m.name).trim()) || m.email;
    const safe = String(label).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[c]));
    el.innerHTML = `<div class="user-chip user-chip--name-only" title="${m.email}">${safe}</div>`;
  }

  // Standard admin hamburger menu items, used by injectAdminHamburger.
  // Project-scoped destinations (Documents/Videos uploaders) come without
  // a project param here — those pages already gracefully prompt for one.
  const ADMIN_MENU = [
    { href: "/projects",            label: "Projects" },
    { href: "/new-project.html",    label: "New Project" },
    { href: "/document-upload.html", label: "Documents" },
    { href: "/video-upload.html",   label: "Videos" },
    { href: "/users.html",          label: "Users" },
  ];

  function injectAdminHamburger(inner, header) {
    const btn = document.createElement("button");
    btn.className = "menu-toggle";
    btn.id = "menuBtn";
    btn.setAttribute("aria-label", "Menu");
    btn.setAttribute("aria-expanded", "false");
    btn.setAttribute("aria-controls", "menuDropdown");
    btn.innerHTML = '<span class="bar"></span>';
    inner.appendChild(btn);

    const panel = document.createElement("div");
    panel.className = "dropdown-panel";
    panel.id = "menuDropdown";
    const itemsHtml = ADMIN_MENU.map((m) => `<a href="${m.href}">${m.label}</a>`).join("");
    panel.innerHTML = `<div class="dropdown-inner">${itemsHtml}</div>`;
    header.appendChild(panel);

    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const open = panel.classList.toggle("is-open");
      btn.setAttribute("aria-expanded", open);
    });
    document.addEventListener("click", (e) => {
      if (!e.target.closest("#menuDropdown") && !e.target.closest("#menuBtn")) {
        panel.classList.remove("is-open");
        btn.setAttribute("aria-expanded", "false");
      }
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        panel.classList.remove("is-open");
        btn.setAttribute("aria-expanded", "false");
      }
    });
  }

  // Auto-mount: find .site-header .header-inner and place a chip there.
  // On admin pages (body.admin-layout) the chip sits to the LEFT of the
  // hamburger to form a left-aligned [Logo] [Chip] [Hamburger] cluster;
  // on every other page it goes at the end of the flex row, after the
  // existing nav.
  async function autoMount() {
    const inner = document.querySelector(".site-header .header-inner");
    if (!inner) return;
    const header = inner.parentElement;

    const isAdminLayout = document.body.classList.contains("admin-layout");

    // Inject a standard admin hamburger on admin-layout pages that don't
    // already have one, so older single-asset upload pages get the same
    // nav cluster without per-page boilerplate.
    if (isAdminLayout && !inner.querySelector(".menu-toggle")) {
      injectAdminHamburger(inner, header);
    }

    let chipEl = inner.querySelector("#userChip");
    if (!chipEl) {
      chipEl = document.createElement("div");
      chipEl.id = "userChip";
    }
    const menuBtn = inner.querySelector(".menu-toggle");
    if (isAdminLayout && menuBtn) {
      // Insert before the hamburger so the chip reads left-of-hamburger.
      if (chipEl.parentElement !== inner || chipEl.nextElementSibling !== menuBtn) {
        inner.insertBefore(chipEl, menuBtn);
      }
    } else if (!chipEl.parentElement) {
      inner.appendChild(chipEl);
    }
    await renderUserChip(chipEl);

    // Admin-only nav links — Documents and Videos (admin upload +
    // management pages) and Users (admin user roster). Hidden from
    // non-admins so the documents/videos features and their existence
    // don't leak to viewers. Documents and Videos are per-project, so
    // they're omitted on the projects index page where each project tile
    // owns its own actions; Users stays everywhere.
    const nav = inner.querySelector(".nav-primary");
    if (nav) {
      const admin = await isAdmin();
      if (admin) {
        const path = window.location.pathname;
        const onProjectsIndex = path === "/" || path === "/projects" || path === "/projects.html";
        if (!onProjectsIndex && !nav.querySelector('a[href="/document-upload.html"]')) {
          const a = document.createElement("a");
          a.href = "/document-upload.html";
          a.textContent = "Documents";
          nav.appendChild(a);
        }
        if (!onProjectsIndex && !nav.querySelector('a[href="/video-upload.html"]')) {
          const a = document.createElement("a");
          a.href = "/video-upload.html";
          a.textContent = "Videos";
          nav.appendChild(a);
        }
        if (!nav.querySelector('a[href="/users.html"]')) {
          const a = document.createElement("a");
          a.href = "/users.html";
          a.textContent = "Users";
          nav.appendChild(a);
        }
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
