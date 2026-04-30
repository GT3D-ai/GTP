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

  // Per-page memo so multiple consumers on the same page don't refetch.
  const projectInfoCache = new Map();

  // Fetch /api/project-info for a project, cached. The server enriches
  // the response with the property's address/name/cover for migrated
  // projects, so this also surfaces the human-readable display name —
  // which is the short, original name (e.g. "500-treat-ave") rather
  // than the URL's compound canonical slug.
  async function getProjectInfo(slug) {
    if (!slug) return null;
    if (projectInfoCache.has(slug)) return projectInfoCache.get(slug);
    try {
      const res = await fetch("/api/project-info?project=" + encodeURIComponent(slug), { credentials: "same-origin" });
      if (!res.ok) return null;
      const data = await res.json();
      projectInfoCache.set(slug, data);
      return data;
    } catch {
      return null;
    }
  }

  // Apply the short project name to a header element. Sets the slug
  // immediately as a fallback so the element isn't blank during the
  // network round trip, then upgrades to info.name when it returns.
  async function applyProjectHeadline(slug, el) {
    if (!el) return;
    el.textContent = slug || "";
    const info = await getProjectInfo(slug);
    if (info && info.name) el.textContent = info.name;
  }

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

  // Detect the active project from the URL so admin pages can show a
  // project-scoped menu (matching the project-home editor menu) when one
  // is in context. Recognised URL shapes:
  //   ?project=<name> in the query string (most upload + edit pages)
  //   /<project> (catch-all project home)
  //   /<section>/<project> for section in the showcase set
  function detectProject() {
    try {
      const fromQuery = new URLSearchParams(location.search).get("project");
      if (fromQuery) return fromQuery;
    } catch {}
    const segments = location.pathname.split("/").filter(Boolean);
    const sectionPrefixes = new Set(["public", "map-viewer", "models", "pointclouds", "plans", "images", "documents"]);
    if (segments.length === 2 && sectionPrefixes.has(segments[0])) {
      try { return decodeURIComponent(segments[1]); } catch { return segments[1]; }
    }
    const reservedTop = new Set([
      "api", "projects", "public",
      "map-viewer", "models", "pointclouds", "plans", "images", "documents",
      "uploads.html", "users.html", "new-project.html", "edit-project.html",
      "document-upload.html", "video-upload.html", "model-upload.html",
      "pointcloud-upload.html", "plan-upload.html", "upload.html", "images.html",
      "image-viewer.html", "viewer.html", "model-viewer.html", "map-editor.html",
      "robots.txt", "tokens.css", "app.css", "me.js", "favicon.ico",
    ]);
    if (segments.length === 1 && !segments[0].includes(".") && !reservedTop.has(segments[0])) {
      try { return decodeURIComponent(segments[0]); } catch { return segments[0]; }
    }
    return null;
  }

  // Standard admin hamburger menu — mirrors the project-home editor menu
  // when an active project is detectable from the URL, falls back to a
  // global admin set otherwise (so users.html / new-project.html still
  // get a useful nav without inventing a project context out of thin air).
  function adminMenuItems() {
    const project = detectProject();
    if (project) {
      const enc = encodeURIComponent(project);
      return [
        { href: `/map-viewer/${enc}`, label: "Map" },
        { href: `/images/${enc}`, label: "2D Images" },
        { href: `/plans/${enc}`, label: "2D Plans" },
        { href: `/models/${enc}`, label: "Models" },
        { href: `/documents/${enc}`, label: "Documents" },
        { href: `/videos/${enc}`, label: "Videos" },
        { href: `/users.html?project=${enc}`, label: "Users" },
        { href: `/edit-project.html?project=${enc}`, label: "Edit Project Information" },
        { href: `/${enc}`, label: "Project Home" },
      ];
    }
    return [
      { href: "/projects", label: "All Projects" },
      { href: "/new-project.html", label: "New Project" },
      { href: "/users.html", label: "Users" },
    ];
  }

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
    const items = adminMenuItems();
    const itemsHtml = items.map((m) => `<a href="${m.href}">${m.label}</a>`).join("");
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
        if (!onProjectsIndex && !nav.querySelector('a[href^="/videos/"]')) {
          const a = document.createElement("a");
          const proj = detectProject();
          a.href = proj ? `/videos/${encodeURIComponent(proj)}` : "/video-upload.html";
          a.textContent = "Videos";
          nav.appendChild(a);
        }
        if (!nav.querySelector('a[href^="/users.html"]')) {
          const a = document.createElement("a");
          const proj = detectProject();
          a.href = proj ? `/users.html?project=${encodeURIComponent(proj)}` : "/users.html";
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

  window.me = { getMe, isAdmin, canView, canEdit, renderUserChip, getProjectInfo, applyProjectHeadline };
})();
