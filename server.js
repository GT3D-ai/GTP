const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { Storage } = require("@google-cloud/storage");
const { generateThumbnail, generateThumbnailFromGCS, getThumbPath, deleteThumbnail } = require("./thumbnail");
const createUserService = require("./user-service");
const createProjectResolver = require("./project-resolver");
const emailService = require("./email-service");

const BUCKET_NAME = "gt-platform-360-photos-bucket";
const PORT = process.env.PORT || 3000;

const app = express();
const upload = multer({ dest: path.join(__dirname, "tmp") });
const storage = new Storage();
const bucket = storage.bucket(BUCKET_NAME);

const IMAGE_BUCKET_NAME = "gt_platform_image_storage";
const imageBucket = storage.bucket(IMAGE_BUCKET_NAME);

const MODEL_BUCKET_NAME = "gt_platform_model_storage";
const modelBucket = storage.bucket(MODEL_BUCKET_NAME);

// Point clouds (LAZ, RCS, RCP, E57, etc.) — typically multi-GB scans.
// Same upload pattern as models: signed URL → direct PUT to GCS to bypass
// the Cloud Run request-size cap and to handle large files.
const POINTCLOUD_BUCKET_NAME = "gt_platform_pointcloud_storage";
const pointcloudBucket = storage.bucket(POINTCLOUD_BUCKET_NAME);

const userService = createUserService({ bucket });
const projectResolver = createProjectResolver({ bucket });

// Case-insensitive project URL canonicalization. Project folders in GCS
// are case-sensitive (so `gs://.../SFYC-Pilings/...` is distinct from
// `gs://.../sfyc-pilings/...`), but most users don't think about case in
// a URL bar. We cache the actual project list briefly and 302-redirect
// any page request whose case doesn't match the canonical folder name.
// API endpoints stay case-sensitive — by the time they're called from the
// page, the URL has been redirected to canonical case.
let projectNameCache = null;
let projectNameCacheTs = 0;
const PROJECT_NAME_CACHE_TTL_MS = 60 * 1000;

async function getProjectNameMap() {
  if (projectNameCache && (Date.now() - projectNameCacheTs) < PROJECT_NAME_CACHE_TTL_MS) {
    return projectNameCache;
  }
  try {
    const [, , apiResponse] = await bucket.getFiles({ delimiter: "/", autoPaginate: false });
    const prefixes = apiResponse.prefixes || [];
    const map = new Map();
    prefixes.forEach((p) => {
      const name = p.replace(/\/$/, "");
      if (name === "_thumbs" || name === "_platform") return;
      // Property-id folders (the post-migration physical prefix root)
      // are not projects — skip them so they don't get registered as
      // legacy projects on the next boot indexing.
      if (/^prop_[0-9a-f]{16}$/.test(name)) return;
      // Last write wins on the rare chance two folder names collide
      // case-insensitively (e.g. "Foo" and "foo"). The canonical winner
      // is whichever GCS returned later — acceptable for an edge case
      // we'd consider misconfiguration.
      map.set(name.toLowerCase(), name);
    });
    projectNameCache = map;
    projectNameCacheTs = Date.now();
    return map;
  } catch (err) {
    console.warn("[project-cache] refresh failed:", err.message);
    return projectNameCache || new Map();
  }
}
function invalidateProjectNameCache() {
  projectNameCache = null;
  projectNameCacheTs = 0;
}

async function resolveCanonicalProjectName(input) {
  if (!input) return null;
  const map = await getProjectNameMap();
  return map.get(String(input).toLowerCase()) || null;
}

// If the supplied project URL segment doesn't match the canonical case in
// GCS, send a 302 to the canonical URL. Returns true when a redirect was
// issued so the caller can short-circuit.
async function maybeRedirectCanonical(req, res, basePath, projectParam) {
  let input;
  try { input = decodeURIComponent(projectParam || ""); } catch { input = projectParam || ""; }
  if (!input) return false;
  // Slug index is the source of truth: it knows about both case-canonical
  // legacy names and post-migration compound slugs (via the alias map).
  // 301 on alias hit so old links eventually update; fall back to the
  // legacy lowercase->canonical lookup for projects not yet indexed.
  try {
    const proj = await projectResolver.resolveProject(input);
    if (proj && proj.isAlias) {
      res.redirect(301, `${basePath}/${encodeURIComponent(proj.canonicalSlug)}`);
      return true;
    }
    if (proj) return false;
  } catch (err) {
    console.warn("[resolver] redirect lookup failed:", err.message);
  }
  const canonical = await resolveCanonicalProjectName(input);
  if (canonical && canonical !== input) {
    const target = `${basePath}/${encodeURIComponent(canonical)}`;
    res.redirect(302, target);
    return true;
  }
  return false;
}

// Body parsing (JSON only; multer handles multipart routes)
app.use(express.json());

// Tell search engines not to index this app. The header applies to every response
// (HTML, API JSON, images) and is the only reliable way to stay out of the index
// if someone links to a URL. robots.txt (below) is an extra polite signal.
app.use((req, res, next) => {
  res.set("X-Robots-Tag", "noindex, nofollow, noarchive, nosnippet");
  next();
});

app.get("/robots.txt", (req, res) => {
  res.type("text/plain").send("User-agent: *\nDisallow: /\n");
});

app.use(express.static(path.join(__dirname, "public")));

// Project list (the editor/admin home). Used to live at /, but / now serves
// a public landing page so anonymous visitors can request access. Authorized
// users land at the welcome page and the JS auto-forwards them here.
app.get(["/projects", "/projects.html"], (req, res) => {
  res.sendFile(path.join(__dirname, "public", "projects.html"));
});

// Pretty URL for map viewer: /map-viewer/<project-name>
app.get("/map-viewer/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/map-viewer", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "map-viewer.html"));
});

// Public, per-project models showcase: /models/<project-name>
app.get("/models/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/models", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-models.html"));
});

// Public, per-project point clouds showcase: /pointclouds/<project-name>
app.get("/pointclouds/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/pointclouds", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-pointclouds.html"));
});

// Public, per-project plans showcase: /plans/<project-name>
app.get("/plans/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/plans", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-plans.html"));
});

// Public, per-project 2D images showcase: /images/<project-name>
app.get("/images/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/images", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-images.html"));
});

// Per-project documents listing: /documents/<project-name>. Anyone with
// the URL can land on the page; the document list is filtered by role
// inside /api/document/files (non-admins see only visibility=public).
app.get("/documents/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/documents", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-documents.html"));
});

// Public-view mirror of the project home page at /public/<project-name>.
// This is the stable share URL the URL map routes to the no-IAP backend so
// invited viewers can land on it without passing IAP. Serves the same
// project-home.html — the page detects the /public/ prefix client-side and
// forces a read-only view (no editor reveals, share button hidden) regardless
// of who's visiting.
app.get("/public/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/public", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-home.html"));
});

// Legacy /models(.html) → admin uploader
app.get(["/models", "/models.html"], (req, res) => {
  res.redirect(301, "/model-upload.html");
});

// ---- Auth: identify the current user via IAP, enforce roster membership ----
// IAP sets X-Goog-Authenticated-User-Email to "accounts.google.com:<email>".
// Local dev falls back to DEV_USER_EMAIL when the header is missing.
function resolveIapEmail(req) {
  const raw = req.get("X-Goog-Authenticated-User-Email");
  if (raw) return raw.split(":").pop().trim().toLowerCase();
  if (process.env.NODE_ENV !== "production" && process.env.DEV_USER_EMAIL) {
    return process.env.DEV_USER_EMAIL.trim().toLowerCase();
  }
  return null;
}

async function resolveUser(req, res, next) {
  const email = resolveIapEmail(req);
  // Anonymous requests are allowed — the LB lets public paths through without
  // IAP. Per-route guards (requireAdmin / requireProjectRole) still reject
  // anonymous writes with 403.
  if (!email) {
    req.userEmail = null;
    req.user = null;
    return next();
  }
  req.userEmail = email;
  try {
    const roster = await userService.loadRoster();
    if (Object.keys(roster.users).length === 0) {
      await userService.bootstrapIfEmpty(email);
    }
    const profile = await userService.getUser(email);
    // /api/me is the only endpoint that tolerates "not in roster" so the UI
    // can render a helpful message for users waiting to be invited.
    if (!profile && req.path !== "/me") {
      return res.status(403).json({ error: "Not authorized. Ask an admin to add you." });
    }
    req.user = profile;
    next();
  } catch (err) {
    console.error("resolveUser error:", err.message);
    res.status(500).json({ error: "Auth resolution failed" });
  }
}

function requireAdmin(req, res, next) {
  if (!req.user?.isAdmin) return res.status(403).json({ error: "Admin required" });
  next();
}

// Resolve the project from the request — in priority order:
// body.project -> query.project -> params.project -> first segment of file path
function resolveProjectFromRequest(req) {
  const raw = (req.body && req.body.project)
    || (req.query && req.query.project)
    || (req.params && req.params.project);
  if (raw) return String(raw);
  const file = (req.body && req.body.file) || (req.query && req.query.file);
  if (file) {
    const seg = String(file).split("/")[0];
    if (seg && seg !== "_thumbs" && seg !== "_platform") return seg;
  }
  return null;
}

// Express 5 made req.query a read-only getter — assignments silently
// fail. Replace it once with a plain object so the project mutation
// below actually persists for downstream handlers. req.body is a
// regular object and is mutable as-is. req.params is set per-route
// (after middleware), so middleware-level mutation doesn't propagate;
// URL-param routes handle the legacy/canonical redirect via
// maybeRedirectCanonical instead.
function makeQueryMutable(req) {
  if (req._queryMutable) return;
  const q = { ...req.query };
  Object.defineProperty(req, "query", { configurable: true, writable: true, value: q });
  req._queryMutable = true;
}

// One-shot per-request resolution: look up the project in the slug
// index, stash the canonical/display/path forms on the request, and
// mutate body/query.project to the physical prefix so existing
// `${project}/...` concatenation lands on the right GCS path for both
// layout:"old" and layout:"new" projects. Idempotent: subsequent calls
// on the same request are no-ops. Read-only handlers that need the
// pre-mutation slug should use `req.projectDisplay`.
async function ensureProjectResolved(req) {
  if (req.projectResolved !== undefined) return;
  const raw = resolveProjectFromRequest(req);
  if (!raw) {
    req.projectResolved = null;
    return;
  }
  let proj = null;
  try {
    proj = await projectResolver.resolveProject(raw);
  } catch (err) {
    console.warn("[resolver] resolution failed:", err.message);
  }
  if (!proj) {
    // Resolution miss — preserve raw input as both display and canonical
    // so handlers don't need to null-check before using these fields.
    req.projectResolved = null;
    req.projectDisplay = raw;
    req.projectCanonical = raw;
    return;
  }
  req.projectResolved = proj;
  req.projectDisplay = proj.name || raw;
  req.projectCanonical = proj.canonicalSlug;
  req.projectPaths = proj.paths;
  req.projectName = proj.paths.base;
  const physical = proj.paths.base;
  if (req.body && typeof req.body.project === "string") req.body.project = physical;
  if (req.query && typeof req.query.project === "string") {
    makeQueryMutable(req);
    req.query.project = physical;
  }
}

// 503 on writes whenever a project's metadata has `migrating: true`. The
// migration script sets this flag during its copy window and clears it
// when the slug-index flips to layout:"new". Reads pass through.
// Resolver miss (project not in index, or metadata read failed) is fail-
// open: an earlier draft tried to fail-closed during active migrations,
// but that incorrectly blocks newly-created projects which aren't in the
// index yet. The per-project flag is the load-bearing safety check.
async function blockedDuringMigration(req) {
  if (req.method === "GET" || req.method === "HEAD") return false;
  await ensureProjectResolved(req);
  const proj = req.projectResolved;
  return !!(proj && proj.migrating === true);
}

function requireProjectRole(minRole) {
  return async (req, res, next) => {
    if (!req.user) return res.status(401).json({ error: "Authentication required" });
    await ensureProjectResolved(req);
    if (req.user.isAdmin) {
      if (await blockedDuringMigration(req)) {
        res.set("Retry-After", "60");
        return res.status(503).json({ error: "Project is being migrated; try again shortly" });
      }
      return next();
    }
    const canonical = req.projectCanonical || resolveProjectFromRequest(req);
    if (!canonical) return res.status(400).json({ error: "project required" });
    if (canonical === "_thumbs" || canonical === "_platform") {
      return res.status(403).json({ error: "Access denied" });
    }
    if (await blockedDuringMigration(req)) {
      res.set("Retry-After", "60");
      return res.status(503).json({ error: "Project is being migrated; try again shortly" });
    }
    // Permission roster keys by canonical slug — pre-switchover that's
    // the project name (== canonical), post-switchover the compound slug.
    const ok = await userService.hasProjectAccess(req.user.email, canonical, minRole);
    if (!ok) return res.status(403).json({ error: `${minRole} access to ${req.projectDisplay || canonical} required` });
    req.projectName = req.projectPaths ? req.projectPaths.base : canonical;
    next();
  };
}

// All /api/* routes go through resolveUser then the project resolver.
// The project resolver runs before multer on multipart routes (where
// req.body isn't parsed yet) — requireProjectRole calls
// ensureProjectResolved a second time so multipart routes still get
// their project mutated/stashed. Idempotent.
app.use("/api", resolveUser);
app.use("/api", async (req, res, next) => {
  await ensureProjectResolved(req);
  next();
});

// List top-level project folders, filtered by what the current user can access.
// Anonymous callers get [] — they don't enumerate projects, they arrive on a
// specific /map-viewer/<project> URL and ask for scoped data directly.
// Admin-controlled presentation order for the projects list. Stored as a
// system-wide JSON file in the reserved _platform/ prefix; nothing else is
// in there today, but we already exclude that prefix from the project
// listing so it's a safe namespace for system metadata.
const PROJECT_ORDER_PATH = "_platform/project-order.json";
async function loadProjectOrder() {
  try {
    const f = bucket.file(PROJECT_ORDER_PATH);
    const [exists] = await f.exists();
    if (!exists) return [];
    const [content] = await f.download();
    const parsed = JSON.parse(content.toString());
    return Array.isArray(parsed.order) ? parsed.order.filter((s) => typeof s === "string") : [];
  } catch (err) {
    console.warn("[project-order] read failed:", err.message);
    return [];
  }
}
async function saveProjectOrder(order) {
  await bucket.file(PROJECT_ORDER_PATH).save(
    JSON.stringify({ order, updatedAt: new Date().toISOString() }, null, 2),
    { contentType: "application/json" }
  );
}

app.get("/api/projects", async (req, res) => {
  try {
    if (!req.user) return res.json([]);
    const [, , apiResponse] = await bucket.getFiles({ delimiter: "/", autoPaginate: false });
    const prefixes = apiResponse.prefixes || [];
    const allProjects = prefixes
      .map((p) => p.replace(/\/$/, ""))
      .filter((p) => p !== "_thumbs" && p !== "_platform");
    const accessible = await userService.accessibleProjects(req.user.email, allProjects);
    // Apply the admin-managed order: items in the saved order first (only
    // those still accessible), then any remaining accessible projects in
    // GCS's lexicographic order — so newly created projects land at the
    // end of the manual order without disrupting it.
    const savedOrder = await loadProjectOrder();
    const accessibleSet = new Set(accessible);
    const ordered = [];
    const placed = new Set();
    for (const name of savedOrder) {
      if (accessibleSet.has(name) && !placed.has(name)) {
        ordered.push(name);
        placed.add(name);
      }
    }
    for (const name of accessible) {
      if (!placed.has(name)) ordered.push(name);
    }
    res.json(ordered);
  } catch (err) {
    console.error("List projects error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: read or set the manual project presentation order. The full set
// of project names goes in `order`; sending an empty array clears the
// override and the listing falls back to alphabetical (GCS lex order).
app.get("/api/admin/project-order", requireAdmin, async (req, res) => {
  try {
    const order = await loadProjectOrder();
    res.json({ order });
  } catch (err) {
    console.error("project-order read error:", err.message);
    res.status(500).json({ error: err.message });
  }
});
app.post("/api/admin/project-order", requireAdmin, async (req, res) => {
  const { order } = req.body || {};
  if (order != null && !Array.isArray(order)) {
    return res.status(400).json({ error: "order must be an array of project names or null" });
  }
  const cleaned = Array.isArray(order)
    ? order
        .filter((s) => typeof s === "string" && s.trim() && !s.includes("/") && s !== "_thumbs" && s !== "_platform")
        .filter((s, i, arr) => arr.indexOf(s) === i)
    : [];
  try {
    await saveProjectOrder(cleaned);
    res.json({ success: true, order: cleaned });
  } catch (err) {
    console.error("project-order save error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// List levels (subfolders) within a project
app.get("/api/levels", async (req, res) => {
  const project = req.query.project;
  if (!project) {
    return res.status(400).json({ error: "project is required" });
  }
  try {
    const [, , apiResponse] = await bucket.getFiles({
      prefix: project + "/",
      delimiter: "/",
      autoPaginate: false,
    });
    const prefixes = apiResponse.prefixes || [];
    const levels = prefixes
      .map((p) => p.replace(project + "/", "").replace(/\/$/, ""))
      .filter((l) => l !== "_thumbs" && l !== "_platform");
    res.json(levels);
  } catch (err) {
    console.error("List levels error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Create a new project with default levels + metadata
const DEFAULT_LEVELS = ["level 0", "level 1", "level 2", "level 3", "attic", "garage", "exterior"];

app.post("/api/create-project", requireAdmin, upload.single("coverPhoto"), async (req, res) => {
  const name = (req.body.name || "").trim();
  const address = (req.body.address || "").trim();
  if (!name) {
    return res.status(400).json({ error: "Project name is required" });
  }

  try {
    // Create project folder in 360 bucket
    await bucket.file(`${name}/`).save("", { contentType: "application/x-directory" });

    // Create level folders. The client sends a "levels" field (JSON array of
     // selected level names); fall back to DEFAULT_LEVELS when absent.
     let levels = DEFAULT_LEVELS;
     if (typeof req.body.levels === "string" && req.body.levels.trim()) {
       try {
         const parsed = JSON.parse(req.body.levels);
         if (Array.isArray(parsed)) {
           levels = parsed
             .map((l) => String(l || "").trim().toLowerCase())
             .filter(Boolean)
             // Only allow levels from the default set (prevents arbitrary folder names)
             .filter((l) => DEFAULT_LEVELS.includes(l));
         }
       } catch { /* ignore bad JSON — use defaults */ }
     }
     for (const lvl of levels) {
       await bucket.file(`${name}/${lvl}/`).save("", { contentType: "application/x-directory" });
     }

    // Create project folder in 2D image bucket
    await imageBucket.file(`${name}/`).save("", { contentType: "application/x-directory" });

    // Also create folders in the model bucket (for consistency with new-project.js)
    try {
      const modelBucket = storage.bucket("gt-platform-model-storage");
      await modelBucket.file(`${name}/`).save("", { contentType: "application/x-directory" });
    } catch (e) { /* non-fatal */ }

    // Upload cover photo if provided
    let coverPhotoPath = null;
    if (req.file) {
      const ext = path.extname(req.file.originalname) || ".jpg";
      const coverName = `cover${ext}`;
      coverPhotoPath = `${name}/${coverName}`;
      const gcsFile = imageBucket.file(coverPhotoPath);
      await new Promise((resolve, reject) => {
        fs.createReadStream(req.file.path)
          .pipe(gcsFile.createWriteStream({
            resumable: false,
            metadata: { contentType: req.file.mimetype || "image/jpeg" },
          }))
          .on("error", reject)
          .on("finish", resolve);
      });
      fs.unlink(req.file.path, () => {});
    }

    // Save project metadata as JSON in 360 bucket
    const metadata = { name, address, coverPhoto: coverPhotoPath, createdAt: new Date().toISOString() };
    await bucket.file(`${name}/project.json`).save(JSON.stringify(metadata, null, 2), {
      contentType: "application/json",
    });

    console.log(`Created project: ${name} (with ${levels.length} levels)`);
    invalidateProjectNameCache();
    // Register the new project in the slug index so the resolver finds
    // it on the next request without waiting for a server restart.
    try {
      await projectResolver.buildLegacyIndex([name]);
    } catch (err) {
      console.warn(`[index] failed to register new project "${name}":`, err.message);
    }
    res.json({ success: true, project: name, metadata });
  } catch (err) {
    console.error("Create project error:", err.message);
    if (req.file) fs.unlink(req.file.path, () => {});
    res.status(500).json({ error: err.message });
  }
});

// Update project metadata (address + optional cover photo). Name is immutable.
app.post("/api/update-project", upload.single("coverPhoto"), requireProjectRole("editor"), async (req, res) => {
  const name = (req.body.name || "").trim();
  const address = (req.body.address || "").trim();
  if (!name) return res.status(400).json({ error: "Project name is required" });

  try {
    // Verify project exists
    const metaFile = bucket.file(`${name}/project.json`);
    const [metaExists] = await metaFile.exists();
    let existing = { name, address: null, coverPhoto: null };
    if (metaExists) {
      const [content] = await metaFile.download();
      try { existing = JSON.parse(content.toString()); } catch {}
    }

    // Upload new cover photo if provided
    let coverPhotoPath = existing.coverPhoto || null;
    if (req.file) {
      const ext = path.extname(req.file.originalname) || ".jpg";
      const coverName = `cover${ext}`;
      coverPhotoPath = `${name}/${coverName}`;
      const gcsFile = imageBucket.file(coverPhotoPath);
      await new Promise((resolve, reject) => {
        fs.createReadStream(req.file.path)
          .pipe(gcsFile.createWriteStream({
            resumable: false,
            metadata: { contentType: req.file.mimetype || "image/jpeg" },
          }))
          .on("error", reject)
          .on("finish", resolve);
      });
      fs.unlink(req.file.path, () => {});
    }

    const metadata = {
      name,
      address,
      coverPhoto: coverPhotoPath,
      createdAt: existing.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await metaFile.save(JSON.stringify(metadata, null, 2), { contentType: "application/json" });
    console.log(`Updated project: ${name}`);
    res.json({ success: true, project: name, metadata });
  } catch (err) {
    console.error("Update project error:", err.message);
    if (req.file) fs.unlink(req.file.path, () => {});
    res.status(500).json({ error: err.message });
  }
});

// ---- Rename / Delete project (admin only) ----

function isValidProjectName(s) {
  if (typeof s !== "string") return false;
  const t = s.trim();
  if (!t) return false;
  if (t.includes("/") || t.includes("\\")) return false;
  if (t.startsWith(".") || t.startsWith("_")) return false;
  return true;
}

async function moveAllInBucket(b, oldPrefix, newPrefix) {
  const [files] = await b.getFiles({ prefix: oldPrefix });
  for (const f of files) {
    const dest = newPrefix + f.name.slice(oldPrefix.length);
    await f.copy(b.file(dest));
    await f.delete();
  }
}

async function deleteAllInBucket(b, prefix) {
  const [files] = await b.getFiles({ prefix });
  for (const f of files) {
    try { await f.delete(); } catch (e) { console.error("Delete failed", f.name, e.message); }
  }
}

app.post("/api/rename-project", requireAdmin, async (req, res) => {
  const oldName = (req.body.oldName || req.body.name || "").trim();
  const newName = (req.body.newName || "").trim();
  if (!oldName) return res.status(400).json({ error: "oldName is required" });
  if (!isValidProjectName(newName)) return res.status(400).json({ error: "newName is invalid" });
  if (oldName === newName) return res.json({ success: true, project: newName });

  try {
    // Block when newName already exists
    const [collision] = await bucket.file(`${newName}/`).exists();
    if (collision) return res.status(409).json({ error: "A project with that name already exists" });

    // Move all GCS objects from <old>/* to <new>/* in each bucket
    const oldPrefix = `${oldName}/`;
    const newPrefix = `${newName}/`;
    await moveAllInBucket(bucket, oldPrefix, newPrefix);
    await moveAllInBucket(imageBucket, oldPrefix, newPrefix);
    await moveAllInBucket(modelBucket, oldPrefix, newPrefix);

    // Rewrite mappings.json — paths inside reference the old project name
    const mappingsFile = bucket.file(`${newName}/mappings.json`);
    const [mExists] = await mappingsFile.exists();
    if (mExists) {
      const [content] = await mappingsFile.download();
      let m = null;
      try { m = JSON.parse(content.toString()); } catch { m = null; }
      if (m) {
        const fix = (p) => typeof p === "string" && p.startsWith(oldPrefix)
          ? newPrefix + p.slice(oldPrefix.length) : p;
        if (m.floorPlanImage) m.floorPlanImage = fix(m.floorPlanImage);
        if (m.floorPlans && typeof m.floorPlans === "object") {
          for (const k of Object.keys(m.floorPlans)) m.floorPlans[k] = fix(m.floorPlans[k]);
        }
        if (Array.isArray(m.pins)) m.pins = m.pins.map((p) => ({ ...p, image360: fix(p.image360) }));
        await mappingsFile.save(JSON.stringify(m, null, 2), { contentType: "application/json" });
      }
    }

    // Update project.json metadata (name + cover photo path)
    const projectFile = bucket.file(`${newName}/project.json`);
    const [pExists] = await projectFile.exists();
    if (pExists) {
      const [content] = await projectFile.download();
      let meta = {};
      try { meta = JSON.parse(content.toString()); } catch { meta = {}; }
      meta.name = newName;
      meta.updatedAt = new Date().toISOString();
      if (typeof meta.coverPhoto === "string" && meta.coverPhoto.startsWith(oldPrefix)) {
        meta.coverPhoto = newPrefix + meta.coverPhoto.slice(oldPrefix.length);
      }
      await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    }

    // Roster: rename project key across all users
    await userService.renameProjectInRoster(oldName, newName);

    // Slug index: move the canonical entry, preserve the old slug as an
    // alias so any in-flight bookmarks/links 301 to the new URL. The
    // resolver invalidates its own caches on any successful mutation.
    try {
      await projectResolver.renameProject({ oldSlug: oldName, newSlug: newName });
    } catch (err) {
      // If the index didn't have the project yet (e.g. a project created
      // before boot indexing ran), just register the new name as legacy.
      console.warn(`[index] renameProject "${oldName}" -> "${newName}" failed: ${err.message}; registering newName as legacy`);
      try {
        await projectResolver.buildLegacyIndex([newName]);
      } catch (e2) {
        console.warn(`[index] fallback buildLegacyIndex failed:`, e2.message);
      }
    }

    console.log(`Renamed project: ${oldName} -> ${newName}`);
    invalidateProjectNameCache();
    res.json({ success: true, project: newName });
  } catch (err) {
    console.error("Rename project error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/delete-project", requireAdmin, async (req, res) => {
  const name = (req.body.name || "").trim();
  const confirm = req.body.confirm === true || req.body.confirm === "true";
  if (!name) return res.status(400).json({ error: "name is required" });
  if (!confirm) return res.status(400).json({ error: "confirmation required" });
  try {
    const prefix = `${name}/`;
    await deleteAllInBucket(bucket, prefix);
    await deleteAllInBucket(imageBucket, prefix);
    await deleteAllInBucket(modelBucket, prefix);
    await deleteAllInBucket(pointcloudBucket, prefix);
    await userService.removeProjectFromRoster(name);
    console.log(`Deleted project: ${name}`);
    invalidateProjectNameCache();
    res.json({ success: true });
  } catch (err) {
    console.error("Delete project error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: set or clear the thumbnail override for a project home card.
// Persists into project.json under `cardThumbnails[<card>]`. Pass an empty
// `file` to clear the override and fall back to the default.
// Editor: set or reset the manual presentation order for the project's
// 2D images. The order is an array of GCS file paths and is read by the
// public /images/<project> page; when omitted or empty, viewers fall back
// to chronological (newest-first) order. Stored on project.json next to
// the existing cover/cardThumbnails fields.
app.post("/api/2d/order", requireProjectRole("editor"), async (req, res) => {
  const { project, order } = req.body || {};
  if (!project) return res.status(400).json({ error: "project is required" });
  if (order != null && !Array.isArray(order)) {
    return res.status(400).json({ error: "order must be an array of file paths or null" });
  }
  // Sanitize: keep only string entries that look like project-scoped paths.
  // Filtering here means a stale entry (file deleted later) just gets
  // ignored on render — no need for the client to garbage-collect.
  const projectPrefix = `${project}/`;
  const cleaned = Array.isArray(order)
    ? order
        .filter((s) => typeof s === "string" && s.startsWith(projectPrefix))
        .filter((s, i, arr) => arr.indexOf(s) === i) // dedupe, first wins
    : [];
  try {
    const projectFile = bucket.file(`${project}/project.json`);
    let meta = {};
    const [exists] = await projectFile.exists();
    if (exists) {
      const [content] = await projectFile.download();
      try { meta = JSON.parse(content.toString()); } catch { meta = {}; }
    }
    if (cleaned.length === 0) delete meta.imageOrder;
    else meta.imageOrder = cleaned;
    meta.updatedAt = new Date().toISOString();
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, imageOrder: meta.imageOrder || [] });
  } catch (err) {
    console.error("2d order error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: set or reset the card order on the project home page. Stored on
// project.json under cardOrder; an empty array clears the override and the
// page falls back to its static layout. Only the known card IDs are
// accepted so a typo or hostile payload can't write garbage in.
app.post("/api/project-card-order", requireProjectRole("editor"), async (req, res) => {
  const { project, order } = req.body || {};
  if (!project) return res.status(400).json({ error: "project is required" });
  if (order != null && !Array.isArray(order)) {
    return res.status(400).json({ error: "order must be an array of card IDs or null" });
  }
  const validCards = new Set(["mainCard", "mapCard", "imageCard", "planCard", "modelCard", "pointcloudCard"]);
  const cleaned = Array.isArray(order)
    ? order
        .filter((s) => typeof s === "string" && validCards.has(s))
        .filter((s, i, arr) => arr.indexOf(s) === i)
    : [];
  try {
    const projectFile = bucket.file(`${project}/project.json`);
    let meta = {};
    const [exists] = await projectFile.exists();
    if (exists) {
      const [content] = await projectFile.download();
      try { meta = JSON.parse(content.toString()); } catch { meta = {}; }
    }
    if (cleaned.length === 0) delete meta.cardOrder;
    else meta.cardOrder = cleaned;
    meta.updatedAt = new Date().toISOString();
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, cardOrder: meta.cardOrder || [] });
  } catch (err) {
    console.error("project-card-order error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/project-thumbnail", requireProjectRole("editor"), async (req, res) => {
  const { project, card, file } = req.body;
  const validCards = new Set(["main", "images", "plans", "models", "pointclouds"]);
  if (!project || !card) return res.status(400).json({ error: "project and card are required" });
  if (!validCards.has(card)) return res.status(400).json({ error: "invalid card" });
  try {
    const projectFile = bucket.file(`${project}/project.json`);
    let meta = {};
    const [exists] = await projectFile.exists();
    if (exists) {
      const [content] = await projectFile.download();
      try { meta = JSON.parse(content.toString()); } catch { meta = {}; }
    }
    if (!meta.cardThumbnails || typeof meta.cardThumbnails !== "object") meta.cardThumbnails = {};
    if (file) meta.cardThumbnails[card] = file;
    else delete meta.cardThumbnails[card];
    meta.updatedAt = new Date().toISOString();
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, cardThumbnails: meta.cardThumbnails });
  } catch (err) {
    console.error("project-thumbnail error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Get project metadata
app.get("/api/project-info", async (req, res) => {
  const project = req.query.project;
  if (!project) return res.status(400).json({ error: "project is required" });
  try {
    const file = bucket.file(`${project}/project.json`);
    const [exists] = await file.exists();
    if (!exists) return res.json({ name: project, address: null, coverPhoto: null });
    const [content] = await file.download();
    res.json(JSON.parse(content.toString()));
  } catch (err) {
    console.error("Get project info error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Create a level (subfolder) within a project
app.post("/api/create-level", requireProjectRole("editor"), async (req, res) => {
  const { project, level } = req.body;
  if (!project || !level) {
    return res.status(400).json({ error: "project and level are required" });
  }
  try {
    const filePath = `${project}/${level}/`;
    const file = bucket.file(filePath);
    await file.save("", { contentType: "application/x-directory" });
    console.log(`Created level: gs://${BUCKET_NAME}/${filePath}`);
    res.json({ success: true, path: filePath });
  } catch (err) {
    console.error("Create level error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Rename a level — moves all 360 photos/thumbnails under the level prefix to
// the new prefix, and rewrites the project's mappings.json so floorPlans and
// pin paths follow the rename.
app.post("/api/rename-level", requireProjectRole("editor"), async (req, res) => {
  const { project, oldLevel, newLevel } = req.body;
  if (!project || !oldLevel || !newLevel) {
    return res.status(400).json({ error: "project, oldLevel, and newLevel are required" });
  }
  const o = String(oldLevel).trim().toLowerCase();
  const n = String(newLevel).trim().toLowerCase();
  if (!n || n.includes("/") || n.includes("\\")) {
    return res.status(400).json({ error: "newLevel is invalid" });
  }
  if (o === n) return res.json({ success: true });
  try {
    const oldPrefix = `${project}/${o}/`;
    const newPrefix = `${project}/${n}/`;
    // Block when the destination already exists (avoid silent merges)
    const [collision] = await bucket.file(newPrefix).exists();
    if (collision) return res.status(409).json({ error: "A level with that name already exists" });

    // Move every file under the old prefix
    const [files] = await bucket.getFiles({ prefix: oldPrefix });
    for (const f of files) {
      const dest = newPrefix + f.name.slice(oldPrefix.length);
      await f.copy(bucket.file(dest));
      await f.delete();
    }
    // Ensure the level "folder" exists at the new name (in case it had no files)
    await bucket.file(newPrefix).save("", { contentType: "application/x-directory" });

    // Rewrite mappings.json: floorPlans key + any pin image360 with the old prefix
    const mappingsFile = bucket.file(`${project}/mappings.json`);
    const [mExists] = await mappingsFile.exists();
    if (mExists) {
      const [content] = await mappingsFile.download();
      let m = null;
      try { m = JSON.parse(content.toString()); } catch { m = null; }
      if (m) {
        if (m.floorPlans && Object.prototype.hasOwnProperty.call(m.floorPlans, o)) {
          m.floorPlans[n] = m.floorPlans[o];
          delete m.floorPlans[o];
        }
        if (Array.isArray(m.pins)) {
          m.pins = m.pins.map((p) => {
            if (typeof p.image360 === "string" && p.image360.startsWith(oldPrefix)) {
              return { ...p, image360: newPrefix + p.image360.slice(oldPrefix.length) };
            }
            return p;
          });
        }
        await mappingsFile.save(JSON.stringify(m, null, 2), { contentType: "application/json" });
      }
    }

    console.log(`Renamed level: ${project}/${o} -> ${project}/${n}`);
    res.json({ success: true });
  } catch (err) {
    console.error("Rename level error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Delete a level — removes every object with the prefix and strips the level
// from mappings.json (pins under that level + floorPlans[level]).
app.post("/api/delete-level", requireProjectRole("editor"), async (req, res) => {
  const { project, level, confirm } = req.body;
  if (!project || !level) return res.status(400).json({ error: "project and level are required" });
  if (confirm !== true && confirm !== "true") return res.status(400).json({ error: "confirmation required" });
  const lvl = String(level).trim().toLowerCase();
  try {
    const prefix = `${project}/${lvl}/`;
    const [files] = await bucket.getFiles({ prefix });
    for (const f of files) {
      try { await f.delete(); } catch (e) { console.error("Delete failed", f.name, e.message); }
    }

    // Strip level references from mappings.json
    const mappingsFile = bucket.file(`${project}/mappings.json`);
    const [mExists] = await mappingsFile.exists();
    if (mExists) {
      const [content] = await mappingsFile.download();
      let m = null;
      try { m = JSON.parse(content.toString()); } catch { m = null; }
      if (m) {
        if (m.floorPlans && Object.prototype.hasOwnProperty.call(m.floorPlans, lvl)) {
          delete m.floorPlans[lvl];
        }
        if (Array.isArray(m.pins)) {
          m.pins = m.pins.filter((p) => !(typeof p.image360 === "string" && p.image360.startsWith(prefix)));
        }
        await mappingsFile.save(JSON.stringify(m, null, 2), { contentType: "application/json" });
      }
    }

    console.log(`Deleted level: ${project}/${lvl} (${files.length} files)`);
    res.json({ success: true });
  } catch (err) {
    console.error("Delete level error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Move a file to a level (copy + delete). Empty level moves to project root.
app.post("/api/assign-level", requireProjectRole("editor"), async (req, res) => {
  const { file: srcPath, project, level } = req.body;
  if (!srcPath || !project) {
    return res.status(400).json({ error: "file and project are required" });
  }
  try {
    const fileName = srcPath.split("/").pop();
    const destPath = level ? `${project}/${level}/${fileName}` : `${project}/${fileName}`;
    if (srcPath === destPath) {
      return res.json({ success: true, from: srcPath, to: destPath });
    }
    const srcFile = bucket.file(srcPath);
    const destFile = bucket.file(destPath);
    await srcFile.copy(destFile);
    await srcFile.delete();

    // Move thumbnail if it exists
    const oldThumbPath = getThumbPath(srcPath);
    const newThumbPath = getThumbPath(destPath);
    const thumbFile = bucket.file(oldThumbPath);
    const [thumbExists] = await thumbFile.exists();
    if (thumbExists) {
      await thumbFile.copy(bucket.file(newThumbPath));
      await thumbFile.delete();
    }

    console.log(`Moved: ${srcPath} -> ${destPath}`);
    res.json({ success: true, from: srcPath, to: destPath });
  } catch (err) {
    console.error("Assign level error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// List files, optionally scoped to a project
app.get("/api/files", async (req, res) => {
  try {
    const project = req.query.project;
    const level = req.query.level;
    const unassignedOnly = req.query.unassigned === "true";
    const options = {};
    let prefix = "";
    if (project && level) {
      prefix = `${project}/${level}/`;
    } else if (project) {
      prefix = project + "/";
    }
    if (prefix) options.prefix = prefix;
    // When listing unassigned files, use delimiter to get only direct children
    if (unassignedOnly && project && !level) {
      options.delimiter = "/";
      options.autoPaginate = false;
      const [files] = await bucket.getFiles(options);
      const list = files
        .filter((f) => !f.name.endsWith("/") && !f.name.startsWith("_thumbs/"))
        .map((f) => ({
          name: f.name,
          displayName: f.name.replace(prefix, ""),
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          unassigned: true,
        }));
      return res.json(list);
    }
    const [files] = await bucket.getFiles(options);
    const list = files
      .filter((f) => !f.name.endsWith("/") && !f.name.startsWith("_thumbs/"))
      .map((f) => {
        const relativePath = prefix ? f.name.replace(prefix, "") : f.name;
        return {
          name: f.name,
          displayName: prefix ? relativePath : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
        };
      });
    res.json(list);
  } catch (err) {
    console.error("List files error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Generate a V4 signed URL for direct browser-to-GCS upload.
// Client sends { bucket: "360"|"2d"|"model"|"plan", project, fileName, contentType, level? }
// Returns { uploadUrl, gcsPath }. The browser then PUTs the file bytes directly
// to uploadUrl with the matching Content-Type, bypassing the Cloud Run 32 MiB cap.
// Auth:
//   "360", "2d" — editor on the project
//   "model", "plan" — admin
// Storage:
//   "plan" writes to the image bucket at <project>/_plans/<fileName>
app.post("/api/upload-url", async (req, res) => {
  const { bucket: bucketKind, project, fileName, contentType, level } = req.body || {};
  if (!bucketKind || !fileName || !contentType) {
    return res.status(400).json({ error: "bucket, fileName, and contentType are required" });
  }
  if (!project) return res.status(400).json({ error: "project is required" });

  // Authorization
  if (bucketKind === "model" || bucketKind === "plan" || bucketKind === "pointcloud" || bucketKind === "document" || bucketKind === "video") {
    // Per-file thumbnails (<file>.thumb.jpg) for models and point clouds pair
    // with files editors already manage on the showcase pages, so they're
    // editor-gated. Everything else in the model/plan/pointcloud/document/video
    // buckets stays admin-only — documents and videos are admin-only across
    // the board.
    const isAssetThumbnail =
      (bucketKind === "model" || bucketKind === "pointcloud") &&
      fileName.endsWith(".thumb.jpg");
    if (isAssetThumbnail) {
      if (!req.user?.isAdmin) {
        const ok = await userService.hasProjectAccess(req.user?.email, project, "editor");
        if (!ok) return res.status(403).json({ error: `editor access to ${project} required` });
      }
    } else if (!req.user?.isAdmin) {
      return res.status(403).json({ error: `Admin required for ${bucketKind} uploads` });
    }
  } else {
    if (!req.user?.isAdmin) {
      const ok = await userService.hasProjectAccess(req.user?.email, project, "editor");
      if (!ok) return res.status(403).json({ error: `editor access to ${project} required` });
    }
  }

  let target, bucketName;
  if (bucketKind === "2d") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else if (bucketKind === "model") { target = modelBucket; bucketName = MODEL_BUCKET_NAME; }
  else if (bucketKind === "plan") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else if (bucketKind === "pointcloud") { target = pointcloudBucket; bucketName = POINTCLOUD_BUCKET_NAME; }
  else if (bucketKind === "document") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else if (bucketKind === "video") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else { target = bucket; bucketName = BUCKET_NAME; }

  let dest = fileName;
  if (bucketKind === "plan") {
    // Plans live under a reserved _plans/ prefix so they don't leak into the
    // regular 2D images list. The caller passes fileName without the prefix.
    dest = `${project}/_plans/${fileName}`;
  } else if (bucketKind === "document") {
    // Documents share the image bucket but live under _documents/ — same
    // separation pattern as plans, and they're admin-only end-to-end so they
    // never appear in any viewer-facing listing.
    dest = `${project}/_documents/${fileName}`;
  } else if (bucketKind === "video") {
    // Videos share the image bucket too, under _videos/. Admin-only across
    // the board; nothing in any viewer-facing list ever surfaces them.
    dest = `${project}/_videos/${fileName}`;
  } else if (project && level) dest = `${project}/${level}/${fileName}`;
  else if (project) dest = `${project}/${fileName}`;

  try {
    const [url] = await target.file(dest).getSignedUrl({
      version: "v4",
      action: "write",
      expires: Date.now() + 60 * 60 * 1000, // 60 minutes (large models/plans take time)
      contentType,
    });
    res.json({ uploadUrl: url, gcsPath: dest, bucket: bucketName });
  } catch (err) {
    console.error("Signed URL error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Upload file, optionally into a project folder
// Single file upload
app.post("/api/upload", upload.single("file"), requireProjectRole("editor"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file provided" });
  }

  const tmpPath = req.file.path;
  const baseName = req.body.destName || req.file.originalname;
  const project = req.body.project;
  const level = req.body.level;
  let destName = baseName;
  if (project && level) {
    destName = `${project}/${level}/${baseName}`;
  } else if (project) {
    destName = `${project}/${baseName}`;
  }
  const fileSize = req.file.size;

  try {
    const gcsFile = bucket.file(destName);
    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: true,
      metadata: { contentType: req.file.mimetype || "application/octet-stream" },
    });

    await new Promise((resolve, reject) => {
      readStream
        .pipe(writeStream)
        .on("error", (err) => {
          console.error("GCS upload error:", err.message);
          reject(err);
        })
        .on("finish", resolve);
    });

    const publicUrl = `https://storage.googleapis.com/${BUCKET_NAME}/${encodeURIComponent(destName)}`;
    console.log(`Uploaded: gs://${BUCKET_NAME}/${destName} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);

    try {
      await generateThumbnail(tmpPath, destName);
    } catch (thumbErr) {
      console.error("Thumbnail generation failed:", thumbErr.message);
    }

    res.json({ success: true, fileName: baseName, destName, fileSize, gcsPath: `gs://${BUCKET_NAME}/${destName}`, url: publicUrl, project: project || null });
  } catch (err) {
    console.error("Upload failed:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    fs.unlink(tmpPath, () => {});
  }
});

// Multi-file upload
app.post("/api/upload-multiple", upload.array("files", 50), requireProjectRole("editor"), async (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: "No files provided" });
  }

  const project = req.body.project;
  const results = [];

  for (const file of req.files) {
    const tmpPath = file.path;
    const baseName = file.originalname;
    const destName = project ? `${project}/${baseName}` : baseName;

    try {
      const gcsFile = bucket.file(destName);
      const readStream = fs.createReadStream(tmpPath);
      const writeStream = gcsFile.createWriteStream({
        resumable: true,
        metadata: { contentType: file.mimetype || "application/octet-stream" },
      });

      await new Promise((resolve, reject) => {
        readStream
          .pipe(writeStream)
          .on("error", reject)
          .on("finish", resolve);
      });

      const publicUrl = `https://storage.googleapis.com/${BUCKET_NAME}/${encodeURIComponent(destName)}`;
      console.log(`Uploaded: gs://${BUCKET_NAME}/${destName} (${(file.size / 1024 / 1024).toFixed(2)} MB)`);

      try {
        await generateThumbnail(tmpPath, destName);
      } catch (thumbErr) {
        console.error(`Thumbnail generation failed for ${baseName}:`, thumbErr.message);
      }

      results.push({ success: true, fileName: baseName, destName, url: publicUrl });
    } catch (err) {
      console.error(`Upload failed for ${baseName}:`, err.message);
      results.push({ success: false, fileName: baseName, error: err.message });
    } finally {
      fs.unlink(tmpPath, () => {});
    }
  }

  res.json({ results, project: project || null });
});

// Delete a 360 photo from GCS
app.post("/api/delete", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath } = req.body;
  if (!filePath) return res.status(400).json({ error: "file is required" });
  try {
    const file = bucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await file.delete();
    await deleteThumbnail(filePath);
    console.log(`Deleted: gs://${BUCKET_NAME}/${filePath}`);
    res.json({ success: true });
  } catch (err) {
    console.error("Delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Proxy image from GCS (supports paths with slashes)
app.get("/api/image", async (req, res) => {
  try {
    const filePath = req.query.file;
    const file = bucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) {
      return res.status(404).send("File not found");
    }
    const [metadata] = await file.getMetadata();
    res.set("Content-Type", metadata.contentType || "application/octet-stream");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("Image proxy error:", err.message);
    res.status(500).send(err.message);
  }
});

// Serve thumbnail for a 360 image
app.get("/api/thumbnail", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).send("file parameter required");

    const thumbPath = getThumbPath(filePath);
    const file = bucket.file(thumbPath);
    const [exists] = await file.exists();

    if (!exists) {
      try {
        await generateThumbnailFromGCS(filePath);
      } catch (err) {
        return res.status(404).send("Thumbnail not found and generation failed");
      }
    }

    res.set("Content-Type", "image/jpeg");
    res.set("Cache-Control", "public, max-age=86400");
    bucket.file(thumbPath).createReadStream().pipe(res);
  } catch (err) {
    console.error("Thumbnail proxy error:", err.message);
    res.status(500).send(err.message);
  }
});

// Generate thumbnails for all existing images that lack one
app.post("/api/generate-thumbnails", requireAdmin, async (req, res) => {
  try {
    const project = req.body.project;
    const options = {};
    if (project) options.prefix = project + "/";

    const [files] = await bucket.getFiles(options);
    const imageFiles = files.filter((f) => {
      const name = f.name;
      return !name.startsWith("_thumbs/")
        && !name.endsWith("/")
        && !name.endsWith(".json")
        && /\.(jpg|jpeg|png|webp|tiff?)$/i.test(name);
    });

    let generated = 0, skipped = 0, failed = 0;

    for (const f of imageFiles) {
      try {
        const result = await generateThumbnailFromGCS(f.name);
        if (result) generated++; else skipped++;
      } catch (err) {
        console.error(`Thumb failed for ${f.name}:`, err.message);
        failed++;
      }
    }

    console.log(`Thumbnail backfill complete: ${generated} generated, ${skipped} skipped, ${failed} failed`);
    res.json({ total: imageFiles.length, generated, skipped, failed });
  } catch (err) {
    console.error("Generate thumbnails error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---- Mappings (pin 360 images onto 2D floor plans) ----

app.get("/api/mappings", async (req, res) => {
  const project = req.query.project;
  if (!project) return res.status(400).json({ error: "project is required" });
  try {
    // _prefix is the physical GCS prefix for this project ("name/" pre-
    // migration, "propertyId/projectId/" post-migration). Frontends use
    // it for level filtering instead of assuming URL slug == prefix.
    const _prefix = `${project}/`;
    const file = bucket.file(`${project}/mappings.json`);
    const [exists] = await file.exists();
    // _generation is the GCS object generation. Editors send it back as
    // `expectedGeneration` on save so concurrent edits can be detected via
    // ifGenerationMatch — see the save handler below. 0 == "file does not
    // exist yet"; ifGenerationMatch:0 makes the first save atomic.
    if (!exists) return res.json({ floorPlanImage: null, pins: [], _generation: "0", _prefix });
    const [content] = await file.download();
    const [metadata] = await file.getMetadata();
    const data = JSON.parse(content.toString());
    res.json({ ...data, _generation: String(metadata.generation || "0"), _prefix });
  } catch (err) {
    console.error("Get mappings error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Save mappings — deliberately NOT at /api/mappings. The LB sends the
// /api/mappings path to the public (no-IAP) backend so the public map-viewer
// can read pins anonymously; saves live at a separate path that routes through
// the private (IAP-protected) backend.
//
// Optimistic concurrency control: clients send the generation they last saw
// in `expectedGeneration`. We use GCS ifGenerationMatch so the write fails
// atomically if another editor saved between load and save. On conflict we
// return 409 with the current state so the client can merge and retry.
const saveMappingsHandler = async (req, res) => {
  const { project, data, expectedGeneration } = req.body;
  if (!project || !data) return res.status(400).json({ error: "project and data are required" });

  // Strip the synthetic _generation field if a client accidentally echoed it
  // back with the data — it's metadata, not part of the persisted document.
  const toSave = { ...data };
  delete toSave._generation;

  try {
    const file = bucket.file(`${project}/mappings.json`);

    // If the client supplied a generation, use it as a precondition. Older
    // clients without the field fall back to the legacy blind-save path.
    const saveOpts = { contentType: "application/json" };
    if (expectedGeneration !== undefined && expectedGeneration !== null) {
      saveOpts.preconditionOpts = { ifGenerationMatch: String(expectedGeneration) };
    }

    let savedGeneration;
    try {
      await file.save(JSON.stringify(toSave, null, 2), saveOpts);
      const [metadata] = await file.getMetadata();
      savedGeneration = String(metadata.generation || "0");
    } catch (err) {
      // GCS returns 412 Precondition Failed when ifGenerationMatch doesn't
      // match (i.e., another editor's save landed in between). Surface the
      // current state so the client can merge.
      const status = err && (err.code || err.statusCode);
      if (status === 412) {
        try {
          const [exists] = await file.exists();
          if (!exists) {
            return res.status(409).json({
              error: "conflict",
              current: { floorPlanImage: null, pins: [], _generation: "0" },
            });
          }
          const [content] = await file.download();
          const [metadata] = await file.getMetadata();
          const current = JSON.parse(content.toString());
          return res.status(409).json({
            error: "conflict",
            current: { ...current, _generation: String(metadata.generation || "0") },
          });
        } catch (innerErr) {
          console.error("Conflict-recovery read failed:", innerErr.message);
          return res.status(500).json({ error: "Conflict detected but could not read current state" });
        }
      }
      throw err;
    }

    console.log(`Saved mappings: gs://${BUCKET_NAME}/${project}/mappings.json (gen ${savedGeneration})`);
    res.json({ success: true, generation: savedGeneration });
  } catch (err) {
    console.error("Save mappings error:", err.message);
    res.status(500).json({ error: err.message });
  }
};
app.post("/api/save-mappings", requireProjectRole("editor"), saveMappingsHandler);
// Keep POST /api/mappings for backward compat, guarded for the (rare) case it
// arrives through the private backend. If it arrives via the public backend
// with no auth, requireProjectRole now returns 401 cleanly (see middleware).
app.post("/api/mappings", requireProjectRole("editor"), saveMappingsHandler);

// ---- 2D Image endpoints (gt_platform_image_storage) ----

// Returns the set of image paths a project uses as floor plans — pulled
// from mappings.json (floorPlanImage + every value in floorPlans). Used
// by the 2D listing endpoints so floor plans don't double-show in the
// 2D images page (they're still reachable via /api/mappings for the
// map viewer / editor / project-home map card). Falls back to an empty
// set if the project has no mappings yet or the file can't be read.
async function getProjectFloorPlanPaths(project) {
  if (!project) return new Set();
  try {
    const f = bucket.file(`${project}/mappings.json`);
    const [exists] = await f.exists();
    if (!exists) return new Set();
    const [content] = await f.download();
    const mappings = JSON.parse(content.toString());
    const paths = new Set();
    if (typeof mappings.floorPlanImage === "string" && mappings.floorPlanImage) {
      paths.add(mappings.floorPlanImage);
    }
    if (mappings.floorPlans && typeof mappings.floorPlans === "object") {
      Object.values(mappings.floorPlans).forEach((p) => {
        if (typeof p === "string" && p) paths.add(p);
      });
    }
    return paths;
  } catch (err) {
    console.warn(`[floorPlanPaths] read failed for ${project}:`, err.message);
    return new Set();
  }
}

// List 2D images in a project (PUBLIC — hidden images filtered out)
app.get("/api/2d/files", async (req, res) => {
  try {
    const project = req.query.project;
    const options = {};
    let prefix = "";
    if (project) {
      prefix = project + "/";
      options.prefix = prefix;
    }
    const [files] = await imageBucket.getFiles(options);
    const floorPlans = await getProjectFloorPlanPaths(project);
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/")) // plans are managed separately
      .filter((f) => !f.name.includes("/_documents/"))
      .filter((f) => !f.name.includes("/_videos/")) // documents are admin-only, never surfaced here
      .filter((f) => !floorPlans.has(f.name)) // floor-plan images live on the map only
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
      .map((f) => ({
        name: f.name,
        displayName: prefix ? f.name.replace(prefix, "") : f.name,
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
      }));
    res.json(list);
  } catch (err) {
    console.error("List 2D files error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: list 2D images including hidden ones. Reached through the
// IAP-protected backend so only signed-in admins can call it.
app.get("/api/admin/2d/files", requireAdmin, async (req, res) => {
  try {
    const project = req.query.project;
    const options = {};
    let prefix = "";
    if (project) {
      prefix = project + "/";
      options.prefix = prefix;
    }
    const [files] = await imageBucket.getFiles(options);
    const floorPlans = await getProjectFloorPlanPaths(project);
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/"))
      .filter((f) => !f.name.includes("/_documents/"))
      .filter((f) => !f.name.includes("/_videos/"))
      .filter((f) => !floorPlans.has(f.name))
      .map((f) => ({
        name: f.name,
        displayName: prefix ? f.name.replace(prefix, "") : f.name,
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
      }));
    res.json(list);
  } catch (err) {
    console.error("List 2D admin files error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: toggle hidden flag on a 2D image
app.post("/api/admin/2d/visibility", requireAdmin, async (req, res) => {
  const { file: filePath, hidden } = req.body;
  if (!filePath) return res.status(400).json({ error: "file is required" });
  try {
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    // Passing null clears a custom metadata key in the Node SDK.
    await file.setMetadata({ metadata: { hidden: hidden ? "true" : null } });
    res.json({ success: true, hidden: !!hidden });
  } catch (err) {
    console.error("2D visibility toggle error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: list 2D images for a project including hidden ones, plus the
// hidden flag — used by project-images.html so editors can see what
// they've hidden and unhide it without bouncing to the admin uploader.
// Same shape as /api/admin/2d/files but project-scoped (the editor must
// have rights on the named project).
app.get("/api/2d/files-with-hidden", requireProjectRole("editor"), async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = project + "/";
    const [files] = await imageBucket.getFiles({ prefix });
    const floorPlans = await getProjectFloorPlanPaths(project);
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/"))
      .filter((f) => !f.name.includes("/_documents/"))
      .filter((f) => !f.name.includes("/_videos/"))
      .filter((f) => !floorPlans.has(f.name))
      .map((f) => ({
        name: f.name,
        displayName: f.name.replace(prefix, ""),
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
      }));
    res.json(list);
  } catch (err) {
    console.error("List 2D editor files error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: toggle the hidden flag on a 2D image. Same handler as the
// admin endpoint above, gated to project editors so they can manage
// what shows on /images/<project> directly from that page.
app.post("/api/2d/visibility", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, hidden } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  try {
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await file.setMetadata({ metadata: { hidden: hidden ? "true" : null } });
    res.json({ success: true, hidden: !!hidden });
  } catch (err) {
    console.error("2D editor visibility toggle error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: proxy any 2D image including hidden ones, so the editor view
// can render hidden thumbnails (the public /api/2d/image returns 404 for
// hidden files by design).
app.get("/api/2d/image-with-hidden", requireProjectRole("editor"), async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).send("file is required");
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).send("File not found");
    const [metadata] = await file.getMetadata();
    res.set("Content-Type", metadata.contentType || "application/octet-stream");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("2D editor image proxy error:", err.message);
    res.status(500).send(err.message);
  }
});

// Upload 2D image
app.post("/api/2d/upload", upload.single("file"), requireProjectRole("editor"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file provided" });
  }

  const tmpPath = req.file.path;
  const baseName = req.body.destName || req.file.originalname;
  const project = req.body.project;
  const destName = project ? `${project}/${baseName}` : baseName;
  const fileSize = req.file.size;

  try {
    const gcsFile = imageBucket.file(destName);
    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: true,
      metadata: { contentType: req.file.mimetype || "application/octet-stream" },
    });

    await new Promise((resolve, reject) => {
      readStream
        .pipe(writeStream)
        .on("error", (err) => {
          console.error("2D upload error:", err.message);
          reject(err);
        })
        .on("finish", resolve);
    });

    console.log(`Uploaded 2D: gs://${IMAGE_BUCKET_NAME}/${destName} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);
    res.json({ success: true, fileName: baseName, destName, fileSize, gcsPath: `gs://${IMAGE_BUCKET_NAME}/${destName}`, project: project || null });
  } catch (err) {
    console.error("2D upload failed:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    fs.unlink(tmpPath, () => {});
  }
});

// Delete 2D image
app.post("/api/2d/delete", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath } = req.body;
  if (!filePath) return res.status(400).json({ error: "file is required" });
  try {
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await file.delete();
    console.log(`Deleted 2D: gs://${IMAGE_BUCKET_NAME}/${filePath}`);
    res.json({ success: true });
  } catch (err) {
    console.error("2D delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Proxy 2D image from GCS (PUBLIC — hidden images return 404)
app.get("/api/2d/image", async (req, res) => {
  try {
    const filePath = req.query.file;
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) {
      return res.status(404).send("File not found");
    }
    const [metadata] = await file.getMetadata();
    if (metadata.metadata && metadata.metadata.hidden === "true") {
      return res.status(404).send("File not found");
    }
    res.set("Content-Type", metadata.contentType || "application/octet-stream");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("2D image proxy error:", err.message);
    res.status(500).send(err.message);
  }
});

// Admin: proxy any 2D image, including hidden ones
app.get("/api/admin/2d/image", requireAdmin, async (req, res) => {
  try {
    const filePath = req.query.file;
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).send("File not found");
    const [metadata] = await file.getMetadata();
    res.set("Content-Type", metadata.contentType || "application/octet-stream");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("2D admin image proxy error:", err.message);
    res.status(500).send(err.message);
  }
});

// ---- Plan endpoints (image bucket, {project}/_plans/ prefix) ----
// Parallel to models but stored inside the image bucket under a reserved
// prefix. Thumbnails stored as <file>.thumb.jpg sibling objects.

// List plans in a project — PUBLIC
app.get("/api/plan/files", async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_plans/`;
    const [files] = await imageBucket.getFiles({ prefix });
    const allNames = new Set(files.map((f) => f.name));
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        return {
          name: f.name,
          displayName: f.name.replace(prefix, ""),
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: allNames.has(thumbName) ? thumbName : null,
        };
      });
    res.json(list);
  } catch (err) {
    console.error("List plans error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Signed download URL for a plan — PUBLIC (15-min expiry)
app.get("/api/plan/download-url", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!filePath.includes("/_plans/")) return res.status(400).json({ error: "not a plan path" });
    const [url] = await imageBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `attachment; filename="${filePath.split("/").pop()}"`,
    });
    res.json({ downloadUrl: url });
  } catch (err) {
    console.error("Plan download-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Inline view URL for a plan — PUBLIC (15-min expiry). Same payload as
// download-url but with Content-Disposition: inline so the browser renders
// PDFs/images in an <iframe> instead of forcing a download.
app.get("/api/plan/view-url", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!filePath.includes("/_plans/")) return res.status(400).json({ error: "not a plan path" });
    const [url] = await imageBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `inline; filename="${filePath.split("/").pop()}"`,
    });
    res.json({ viewUrl: url });
  } catch (err) {
    console.error("Plan view-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Proxy a plan thumbnail — PUBLIC
app.get("/api/plan/thumbnail", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).send("file is required");
    if (!filePath.includes("/_plans/") || !filePath.endsWith(".thumb.jpg")) {
      return res.status(400).send("invalid thumbnail path");
    }
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).send("Not found");
    const [metadata] = await file.getMetadata();
    res.set("Content-Type", metadata.contentType || "image/jpeg");
    res.set("Cache-Control", "public, max-age=300");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("Plan thumbnail error:", err.message);
    res.status(500).send(err.message);
  }
});

// Delete a plan — admins + project editors. Also deletes the sibling thumbnail.
app.post("/api/plan/delete", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!filePath.includes("/_plans/")) return res.status(400).json({ error: "not a plan path" });
  if (filePath.endsWith(".thumb.jpg")) {
    return res.status(400).json({ error: "Thumbnails are deleted alongside their plan" });
  }
  try {
    const f = imageBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await f.delete();
    console.log(`Deleted plan: gs://${IMAGE_BUCKET_NAME}/${filePath}`);
    try {
      const thumb = imageBucket.file(filePath + ".thumb.jpg");
      const [thumbExists] = await thumb.exists();
      if (thumbExists) {
        await thumb.delete();
        console.log(`Deleted plan thumbnail: gs://${IMAGE_BUCKET_NAME}/${filePath}.thumb.jpg`);
      }
    } catch (e) { /* non-fatal */ }
    res.json({ success: true });
  } catch (err) {
    console.error("Plan delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---- Document endpoints (image bucket, {project}/_documents/ prefix) ----
// Documents are admin-only across the board: list, download, rename, change
// visibility, and delete all require admin. The `visibility` flag is stored
// per-document on a sidecar (<file>.meta.json) but isn't enforced by any
// public endpoint yet — no viewer-facing surface exposes documents at all.

const DOCUMENTS_PREFIX_SEG = "/_documents/";
function isDocumentPath(p) { return typeof p === "string" && p.includes(DOCUMENTS_PREFIX_SEG); }
function metaPathFor(filePath) { return filePath + ".meta.json"; }
async function readDocumentMeta(filePath) {
  try {
    const metaFile = imageBucket.file(metaPathFor(filePath));
    const [exists] = await metaFile.exists();
    if (!exists) return {};
    const [content] = await metaFile.download();
    return JSON.parse(content.toString());
  } catch (err) {
    console.warn(`[document] meta read failed for ${filePath}:`, err.message);
    return {};
  }
}
async function writeDocumentMeta(filePath, meta) {
  await imageBucket.file(metaPathFor(filePath)).save(
    JSON.stringify(meta, null, 2),
    { contentType: "application/json" }
  );
}

// Admin: full document listing including private docs and uploader info.
// Mirror of /api/document/files but kept OFF the public URL-map matcher
// so it routes through the IAP backend — the public listing endpoint
// can't see admin identity (it sits behind the no-IAP backend serving
// /documents/<project>) and would silently filter private docs out.
app.get("/api/admin/document/files", requireAdmin, async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_documents/`;
    const [files] = await imageBucket.getFiles({ prefix });
    const docs = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"));
    const list = await Promise.all(docs.map(async (f) => {
      const meta = await readDocumentMeta(f.name);
      return {
        name: f.name,
        displayName: f.name.replace(prefix, ""),
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        uploadedBy: meta.uploadedBy || null,
        uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
        visibility: meta.visibility === "public" ? "public" : "private",
      };
    }));
    list.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    res.json(list);
  } catch (err) {
    console.error("Admin list documents error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: signed download URL that ignores the visibility flag — same
// reason as the listing mirror above. Public download-url is reachable
// to non-admins via the no-IAP backend and only signs URLs for public
// docs; admins use this version for everything else.
app.get("/api/admin/document/download-url", requireAdmin, async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!isDocumentPath(filePath)) return res.status(400).json({ error: "not a document path" });
    const [url] = await imageBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `attachment; filename="${filePath.split("/").pop()}"`,
    });
    res.json({ downloadUrl: url });
  } catch (err) {
    console.error("Admin document download-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// List documents in a project. Public route, but content is filtered by
// caller role: admins see every document with full metadata; everyone
// else (anonymous viewers and authenticated non-admins) sees only those
// flagged visibility=public. The /documents/<project> page consumes this.
app.get("/api/document/files", async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_documents/`;
    const [files] = await imageBucket.getFiles({ prefix });
    const docs = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"));
    const isAdminCaller = !!req.user?.isAdmin;
    const list = (await Promise.all(docs.map(async (f) => {
      const meta = await readDocumentMeta(f.name);
      const visibility = meta.visibility === "public" ? "public" : "private";
      // Skip private documents for non-admin callers — they shouldn't even
      // know they exist.
      if (!isAdminCaller && visibility !== "public") return null;
      const entry = {
        name: f.name,
        displayName: f.name.replace(prefix, ""),
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
      };
      if (isAdminCaller) {
        entry.uploadedBy = meta.uploadedBy || null;
        entry.visibility = visibility;
      }
      return entry;
    }))).filter(Boolean);
    list.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    res.json(list);
  } catch (err) {
    console.error("List documents error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Acknowledge a completed direct-to-GCS upload — admin only. The signed
// upload URL itself doesn't carry the requesting admin's identity past the
// PUT, so the client calls this immediately after a successful PUT and the
// server records the uploader on a sidecar metadata file.
app.post("/api/document/uploaded", requireAdmin, async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!isDocumentPath(filePath)) return res.status(400).json({ error: "not a document path" });
  try {
    const f = imageBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    // Preserve uploadedBy if a sidecar already exists (re-upload over an
    // existing path keeps the original uploader). Visibility defaults to
    // private on first creation.
    const existing = await readDocumentMeta(filePath);
    const meta = {
      uploadedBy: existing.uploadedBy || req.user?.email || null,
      uploadedAt: existing.uploadedAt || new Date().toISOString(),
      visibility: existing.visibility === "public" ? "public" : "private",
    };
    await writeDocumentMeta(filePath, meta);
    res.json({ success: true, meta });
  } catch (err) {
    console.error("Document uploaded ack error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Signed download URL for a document — public route, but private docs
// require admin. Non-admins (including anonymous viewers) can only
// download a document whose visibility metadata is "public".
app.get("/api/document/download-url", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!isDocumentPath(filePath)) return res.status(400).json({ error: "not a document path" });
    if (!req.user?.isAdmin) {
      const meta = await readDocumentMeta(filePath);
      if (meta.visibility !== "public") {
        return res.status(403).json({ error: "This document is private" });
      }
    }
    const [url] = await imageBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `attachment; filename="${filePath.split("/").pop()}"`,
    });
    res.json({ downloadUrl: url });
  } catch (err) {
    console.error("Document download-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Rename a document — admin only. GCS has no rename, so copy + delete on
// both the file and its sidecar metadata. The new name must stay inside
// the same project's _documents/ folder.
app.post("/api/document/rename", requireAdmin, async (req, res) => {
  const { file: filePath, newName } = req.body || {};
  if (!filePath || !newName) return res.status(400).json({ error: "file and newName are required" });
  if (!isDocumentPath(filePath)) return res.status(400).json({ error: "not a document path" });
  const cleanNew = String(newName).trim().replace(/^\/+|\/+$/g, "");
  if (!cleanNew || cleanNew.includes("/")) {
    return res.status(400).json({ error: "newName must be a single filename (no slashes)" });
  }
  if (cleanNew.endsWith(".meta.json")) {
    return res.status(400).json({ error: "filename can't end in .meta.json" });
  }
  try {
    const dirIdx = filePath.lastIndexOf("/");
    const newPath = filePath.slice(0, dirIdx + 1) + cleanNew;
    if (newPath === filePath) return res.json({ success: true, newPath });
    const src = imageBucket.file(filePath);
    const [exists] = await src.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    const [collides] = await imageBucket.file(newPath).exists();
    if (collides) return res.status(409).json({ error: "A document with that name already exists" });
    await src.copy(imageBucket.file(newPath));
    await src.delete();
    // Move the meta sidecar alongside, if present.
    try {
      const oldMeta = imageBucket.file(metaPathFor(filePath));
      const [metaExists] = await oldMeta.exists();
      if (metaExists) {
        await oldMeta.copy(imageBucket.file(metaPathFor(newPath)));
        await oldMeta.delete();
      }
    } catch (metaErr) {
      console.warn(`[document] meta rename failed for ${filePath}:`, metaErr.message);
    }
    res.json({ success: true, newPath });
  } catch (err) {
    console.error("Document rename error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Set the public/private visibility flag on a document — admin only.
// The flag is stored on the sidecar metadata; nothing in the codebase
// enforces it yet (no viewer endpoint exists), so it's purely metadata
// captured for future use.
app.post("/api/document/visibility", requireAdmin, async (req, res) => {
  const { file: filePath, visibility } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!isDocumentPath(filePath)) return res.status(400).json({ error: "not a document path" });
  if (visibility !== "public" && visibility !== "private") {
    return res.status(400).json({ error: "visibility must be 'public' or 'private'" });
  }
  try {
    const f = imageBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    const meta = await readDocumentMeta(filePath);
    meta.visibility = visibility;
    if (!meta.uploadedAt) meta.uploadedAt = new Date().toISOString();
    await writeDocumentMeta(filePath, meta);
    res.json({ success: true, visibility });
  } catch (err) {
    console.error("Document visibility error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Delete a document — admin only. Also removes the sidecar metadata.
app.post("/api/document/delete", requireAdmin, async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!isDocumentPath(filePath)) return res.status(400).json({ error: "not a document path" });
  if (filePath.endsWith(".meta.json")) {
    return res.status(400).json({ error: "metadata sidecars are deleted alongside their document" });
  }
  try {
    const f = imageBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await f.delete();
    console.log(`Deleted document: gs://${IMAGE_BUCKET_NAME}/${filePath}`);
    try {
      const meta = imageBucket.file(metaPathFor(filePath));
      const [metaExists] = await meta.exists();
      if (metaExists) await meta.delete();
    } catch (e) { /* non-fatal */ }
    res.json({ success: true });
  } catch (err) {
    console.error("Document delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---- Video endpoints (image bucket, {project}/_videos/ prefix) ----
// Same admin-only-end-to-end pattern as documents: list, download, rename,
// delete. No public surface exists; videos never appear in any viewer-
// facing list. Per-file metadata (uploadedBy, uploadedAt) lives on a
// sibling .meta.json sidecar.

const VIDEOS_PREFIX_SEG = "/_videos/";
function isVideoPath(p) { return typeof p === "string" && p.includes(VIDEOS_PREFIX_SEG); }
function videoMetaPathFor(filePath) { return filePath + ".meta.json"; }
async function readVideoMeta(filePath) {
  try {
    const metaFile = imageBucket.file(videoMetaPathFor(filePath));
    const [exists] = await metaFile.exists();
    if (!exists) return {};
    const [content] = await metaFile.download();
    return JSON.parse(content.toString());
  } catch (err) {
    console.warn(`[video] meta read failed for ${filePath}:`, err.message);
    return {};
  }
}
async function writeVideoMeta(filePath, meta) {
  await imageBucket.file(videoMetaPathFor(filePath)).save(
    JSON.stringify(meta, null, 2),
    { contentType: "application/json" }
  );
}

app.get("/api/video/files", requireAdmin, async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_videos/`;
    const [files] = await imageBucket.getFiles({ prefix });
    const videos = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"));
    const list = await Promise.all(videos.map(async (f) => {
      const meta = await readVideoMeta(f.name);
      return {
        name: f.name,
        displayName: f.name.replace(prefix, ""),
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        uploadedBy: meta.uploadedBy || null,
        uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
      };
    }));
    list.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    res.json(list);
  } catch (err) {
    console.error("List videos error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Acknowledge a completed direct-to-GCS upload — admin only. Stamps the
// metadata sidecar with the uploader's email and a timestamp so the list
// view can show who uploaded what.
app.post("/api/video/uploaded", requireAdmin, async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
  try {
    const f = imageBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    const existing = await readVideoMeta(filePath);
    const meta = {
      uploadedBy: existing.uploadedBy || req.user?.email || null,
      uploadedAt: existing.uploadedAt || new Date().toISOString(),
    };
    await writeVideoMeta(filePath, meta);
    res.json({ success: true, meta });
  } catch (err) {
    console.error("Video uploaded ack error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Signed download URL for a video — admin only (15-min expiry).
app.get("/api/video/download-url", requireAdmin, async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
    const [url] = await imageBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `attachment; filename="${filePath.split("/").pop()}"`,
    });
    res.json({ downloadUrl: url });
  } catch (err) {
    console.error("Video download-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Rename a video — admin only. GCS has no rename, so copy + delete on
// both the file and its sidecar metadata. The new name must stay inside
// the same project's _videos/ folder.
app.post("/api/video/rename", requireAdmin, async (req, res) => {
  const { file: filePath, newName } = req.body || {};
  if (!filePath || !newName) return res.status(400).json({ error: "file and newName are required" });
  if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
  const cleanNew = String(newName).trim().replace(/^\/+|\/+$/g, "");
  if (!cleanNew || cleanNew.includes("/")) {
    return res.status(400).json({ error: "newName must be a single filename (no slashes)" });
  }
  if (cleanNew.endsWith(".meta.json")) {
    return res.status(400).json({ error: "filename can't end in .meta.json" });
  }
  try {
    const dirIdx = filePath.lastIndexOf("/");
    const newPath = filePath.slice(0, dirIdx + 1) + cleanNew;
    if (newPath === filePath) return res.json({ success: true, newPath });
    const src = imageBucket.file(filePath);
    const [exists] = await src.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    const [collides] = await imageBucket.file(newPath).exists();
    if (collides) return res.status(409).json({ error: "A video with that name already exists" });
    await src.copy(imageBucket.file(newPath));
    await src.delete();
    try {
      const oldMeta = imageBucket.file(videoMetaPathFor(filePath));
      const [metaExists] = await oldMeta.exists();
      if (metaExists) {
        await oldMeta.copy(imageBucket.file(videoMetaPathFor(newPath)));
        await oldMeta.delete();
      }
    } catch (metaErr) {
      console.warn(`[video] meta rename failed for ${filePath}:`, metaErr.message);
    }
    res.json({ success: true, newPath });
  } catch (err) {
    console.error("Video rename error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Delete a video — admin only. Also removes the sidecar metadata.
app.post("/api/video/delete", requireAdmin, async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
  if (filePath.endsWith(".meta.json")) {
    return res.status(400).json({ error: "metadata sidecars are deleted alongside their video" });
  }
  try {
    const f = imageBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await f.delete();
    console.log(`Deleted video: gs://${IMAGE_BUCKET_NAME}/${filePath}`);
    try {
      const meta = imageBucket.file(videoMetaPathFor(filePath));
      const [metaExists] = await meta.exists();
      if (metaExists) await meta.delete();
    } catch (e) { /* non-fatal */ }
    res.json({ success: true });
  } catch (err) {
    console.error("Video delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---- Model file endpoints (gt_platform_model_storage) ----

// List models in a project — PUBLIC (anyone with the project name can see).
// Convention: a model at project/foo.glb may have a sibling thumbnail at
// project/foo.glb.thumb.jpg (any image type, but we always store with the
// .thumb.jpg suffix on the model's filename). Thumbnails are hidden from the
// main list and surfaced as the `thumbnail` field on the associated model.
//
// OBJ↔MTL pairing: an OBJ at project/foo.obj is paired with project/foo.mtl
// when both exist. The MTL is hidden from the listing and surfaced as
// `companions.mtl` on the OBJ entry — the 3D viewer uses this to load
// materials/textures alongside the geometry.
app.get("/api/model/files", async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = project + "/";
    const [files] = await modelBucket.getFiles({ prefix });
    const allNames = new Set(files.map((f) => f.name));

    // Identify MTL files paired to a same-basename OBJ — they get hidden from
    // the visible list and attached as a companion to the OBJ entry instead.
    const pairedMtl = new Set();
    for (const name of allNames) {
      if (name.endsWith(".obj")) {
        const mtl = name.slice(0, -4) + ".mtl";
        if (allNames.has(mtl)) pairedMtl.add(mtl);
      }
    }

    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !pairedMtl.has(f.name))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        const entry = {
          name: f.name,
          displayName: f.name.replace(prefix, ""),
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: allNames.has(thumbName) ? thumbName : null,
        };
        if (f.name.endsWith(".obj")) {
          const mtl = f.name.slice(0, -4) + ".mtl";
          if (allNames.has(mtl)) entry.companions = { mtl };
        }
        return entry;
      });
    res.json(list);
  } catch (err) {
    console.error("List models error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Proxy a model thumbnail — PUBLIC
app.get("/api/model/thumbnail", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).send("file is required");
    if (!filePath.endsWith(".thumb.jpg")) return res.status(400).send("invalid thumbnail path");
    const file = modelBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).send("Not found");
    const [metadata] = await file.getMetadata();
    res.set("Content-Type", metadata.contentType || "image/jpeg");
    res.set("Cache-Control", "public, max-age=300");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("Model thumbnail error:", err.message);
    res.status(500).send(err.message);
  }
});

// Generate a signed download URL for a model — PUBLIC (15-min expiry)
app.get("/api/model/download-url", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    const [url] = await modelBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `attachment; filename="${filePath.split("/").pop()}"`,
    });
    res.json({ downloadUrl: url });
  } catch (err) {
    console.error("Model download-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Delete a model
// Delete a model — admins + project editors. Also deletes the sibling
// thumbnail (<file>.thumb.jpg) if present.
app.post("/api/model/delete", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (filePath.endsWith(".thumb.jpg")) {
    return res.status(400).json({ error: "Thumbnails are deleted alongside their model" });
  }
  try {
    const f = modelBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await f.delete();
    console.log(`Deleted model: gs://${MODEL_BUCKET_NAME}/${filePath}`);

    // Best-effort: remove the thumbnail if one exists
    try {
      const thumb = modelBucket.file(filePath + ".thumb.jpg");
      const [thumbExists] = await thumb.exists();
      if (thumbExists) {
        await thumb.delete();
        console.log(`Deleted thumbnail: gs://${MODEL_BUCKET_NAME}/${filePath}.thumb.jpg`);
      }
    } catch (e) { /* non-fatal */ }

    // Best-effort: remove companion MTL when deleting an OBJ. The MTL is
    // hidden from the listing and only useful alongside its OBJ, so the
    // pairing implies shared lifecycle.
    if (filePath.endsWith(".obj")) {
      try {
        const mtlPath = filePath.slice(0, -4) + ".mtl";
        const mtl = modelBucket.file(mtlPath);
        const [mtlExists] = await mtl.exists();
        if (mtlExists) {
          await mtl.delete();
          console.log(`Deleted companion MTL: gs://${MODEL_BUCKET_NAME}/${mtlPath}`);
        }
      } catch (e) { /* non-fatal */ }
    }

    res.json({ success: true });
  } catch (err) {
    console.error("Model delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---- Point cloud endpoints (gt_platform_pointcloud_storage) ----
// Same access pattern as models: list/thumbnail/download are PUBLIC (anyone
// with the project name can see), uploads are admin-only via /api/upload-url
// with bucket=pointcloud, deletes are editor-gated.

app.get("/api/pointcloud/files", async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = project + "/";
    const [files] = await pointcloudBucket.getFiles({ prefix });
    const allNames = new Set(files.map((f) => f.name));
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        return {
          name: f.name,
          displayName: f.name.replace(prefix, ""),
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: allNames.has(thumbName) ? thumbName : null,
        };
      });
    res.json(list);
  } catch (err) {
    console.error("List point clouds error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Proxy a point cloud thumbnail — PUBLIC
app.get("/api/pointcloud/thumbnail", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).send("file is required");
    if (!filePath.endsWith(".thumb.jpg")) return res.status(400).send("invalid thumbnail path");
    const file = pointcloudBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).send("Not found");
    const [metadata] = await file.getMetadata();
    res.set("Content-Type", metadata.contentType || "image/jpeg");
    res.set("Cache-Control", "public, max-age=300");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("Point cloud thumbnail error:", err.message);
    res.status(500).send(err.message);
  }
});

// Generate a signed download URL for a point cloud — PUBLIC (15-min expiry).
// Long-lived enough for big-file downloads but short enough that a leaked URL
// stops working quickly.
app.get("/api/pointcloud/download-url", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    const [url] = await pointcloudBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
      responseDisposition: `attachment; filename="${filePath.split("/").pop()}"`,
    });
    res.json({ downloadUrl: url });
  } catch (err) {
    console.error("Point cloud download-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Delete a point cloud — admins + project editors. Also deletes the sibling
// thumbnail (<file>.thumb.jpg) if present.
app.post("/api/pointcloud/delete", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (filePath.endsWith(".thumb.jpg")) {
    return res.status(400).json({ error: "Thumbnails are deleted alongside their point cloud" });
  }
  try {
    const f = pointcloudBucket.file(filePath);
    const [exists] = await f.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await f.delete();
    console.log(`Deleted point cloud: gs://${POINTCLOUD_BUCKET_NAME}/${filePath}`);

    try {
      const thumb = pointcloudBucket.file(filePath + ".thumb.jpg");
      const [thumbExists] = await thumb.exists();
      if (thumbExists) {
        await thumb.delete();
        console.log(`Deleted thumbnail: gs://${POINTCLOUD_BUCKET_NAME}/${filePath}.thumb.jpg`);
      }
    } catch (e) { /* non-fatal */ }

    res.json({ success: true });
  } catch (err) {
    console.error("Point cloud delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---- User account management ----

// Current user's info + effective permissions. Used by the frontend to render
// appropriate UI. Un-rostered authenticated users get {authorized: false}.
app.get("/api/me", async (req, res) => {
  const email = req.userEmail;
  const profile = req.user;
  if (!profile) {
    return res.json({ email, authorized: false, isAdmin: false, projects: {} });
  }
  res.json({
    email: profile.email,
    authorized: true,
    isAdmin: !!profile.isAdmin,
    name: profile.name,
    address: profile.address,
    phone: profile.phone,
    createdAt: profile.createdAt,
    projects: profile.projects || {},
  });
});

// List all users (admin only)
app.get("/api/users", requireAdmin, async (req, res) => {
  try {
    const users = await userService.listUsers();
    res.json(users);
  } catch (err) {
    console.error("List users error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Upsert a user (admin only)
app.post("/api/users", requireAdmin, async (req, res) => {
  const { email, name, address, phone, isAdmin, projects } = req.body || {};
  if (!email) return res.status(400).json({ error: "email required" });
  try {
    const user = await userService.upsertUser(email, { name, address, phone, isAdmin, projects });
    res.json(user);
  } catch (err) {
    console.error("Upsert user error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Delete a user (admin only; can't delete the last admin)
app.delete("/api/users/:email", requireAdmin, async (req, res) => {
  const target = (req.params.email || "").toLowerCase();
  if (target === req.user.email) {
    return res.status(400).json({ error: "You can't delete your own account while logged in." });
  }
  try {
    const existed = await userService.deleteUser(target);
    if (!existed) return res.status(404).json({ error: "User not found" });
    res.json({ success: true });
  } catch (err) {
    console.error("Delete user error:", err.message);
    res.status(400).json({ error: err.message });
  }
});

// Share a project with another user as a viewer. Editors (and admins) can
// invite anyone by name + email. If the recipient already has editor role we
// leave it intact rather than downgrading.
// Resolve a thumbnail file path for a project — same logic the home page uses:
// the editor-set coverPhoto wins, otherwise fall back to the first 2D image.
async function resolveProjectCover(project) {
  try {
    const infoFile = bucket.file(`${project}/project.json`);
    const [exists] = await infoFile.exists();
    if (exists) {
      const [content] = await infoFile.download();
      const info = JSON.parse(content.toString());
      if (info.coverPhoto) return info.coverPhoto;
    }
  } catch (err) {
    console.warn(`[share] coverPhoto lookup failed for ${project}:`, err.message);
  }
  try {
    const [files] = await imageBucket.getFiles({ prefix: `${project}/` });
    const first = files.find((f) =>
      !f.name.endsWith("/") &&
      !f.name.includes("/_plans/") &&
      !f.name.includes("/_documents/") &&
      !f.name.includes("/_videos/") &&
      (f.metadata.contentType || "").startsWith("image/") &&
      !(f.metadata.metadata && f.metadata.metadata.hidden === "true")
    );
    if (first) return first.name;
  } catch (err) {
    console.warn(`[share] 2D fallback lookup failed for ${project}:`, err.message);
  }
  return null;
}

// Throttle helper for /api/share-project-public. Each anonymous client IP is
// allowed one successful share per project per 24 hours — repeats inside that
// window get a friendly "contact us" response. The marker is a GCS object so
// the limit holds across Cloud Run instances and across restarts (in-memory
// wouldn't survive either). The marker's last-modified time is what we
// compare against, so simply re-saving the marker resets the window.
const SHARE_THROTTLE_PREFIX = "_share-throttle/";
const SHARE_THROTTLE_WINDOW_MS = 24 * 60 * 60 * 1000;
function clientIpHash(req) {
  // Behind Google Cloud LB the original client IP is the leftmost entry in
  // X-Forwarded-For; req.socket.remoteAddress is the LB's edge IP and not
  // useful for distinguishing callers.
  const xff = req.get("x-forwarded-for") || "";
  const ip = xff.split(",")[0].trim() || req.socket?.remoteAddress || "unknown";
  return crypto.createHash("sha256").update(ip).digest("hex").slice(0, 32);
}
function shareThrottleFile(req, project) {
  return bucket.file(
    `${SHARE_THROTTLE_PREFIX}${encodeURIComponent(project)}/${clientIpHash(req)}`
  );
}

// Anonymous-friendly share endpoint used by the /public/<project> mirror.
// Same upsert + invite flow as /api/share-project, but no auth: anyone with a
// public share link can pass the access along. The project name in the body
// is the only thing tying the share to a real project — same trust model as
// the public link itself. Honeypot field defends against the most basic bots,
// and the per-IP throttle below caps anonymous shares at one per source IP.
app.post("/api/share-project-public", async (req, res) => {
  if (req.body && req.body.website) return res.json({ success: true });

  // After ensureProjectResolved: req.body.project is the physical GCS
  // prefix (use for path lookups), req.projectCanonical is the slug used
  // as a key in users.json and in URLs, req.projectDisplay is the
  // human-readable name for emails/UI.
  const physical = String((req.body && req.body.project) || "").trim();
  const canonical = String(req.projectCanonical || physical).trim();
  const display = String(req.projectDisplay || canonical).trim();
  if (!physical) return res.status(400).json({ error: "project required" });
  const email = String((req.body && req.body.email) || "").trim().toLowerCase();
  const name = String((req.body && req.body.name) || "").trim();
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: "Valid email required" });
  }
  if (!name) return res.status(400).json({ error: "Name required" });

  try {
    // Verify the project actually exists before logging a stranger as a
    // viewer of it — without this, a bot could pollute the user roster with
    // arbitrary project names.
    const [exists] = await bucket.file(`${physical}/`).exists();
    if (!exists) return res.status(404).json({ error: "Project not found" });

    // Per-IP per-project cap: one anonymous share per (source IP, project)
    // every 24h. Marker file's last-modified time is the window anchor —
    // a stale marker (>24h old) is treated as expired and the share goes
    // through, refreshing the marker.
    const throttleFile = shareThrottleFile(req, canonical);
    const [throttled] = await throttleFile.exists();
    if (throttled) {
      let ageMs = Infinity;
      try {
        const [meta] = await throttleFile.getMetadata();
        const updatedMs = Date.parse(meta.updated || meta.timeCreated || "");
        if (!Number.isNaN(updatedMs)) ageMs = Date.now() - updatedMs;
      } catch (metaErr) {
        // Couldn't read metadata — fall through to a conservative throttle
        // (treating ageMs as Infinity) so a flaky read doesn't accidentally
        // open the floodgates.
        console.warn("[share-public] throttle marker metadata read failed:", metaErr.message);
        ageMs = 0;
      }
      if (ageMs < SHARE_THROTTLE_WINDOW_MS) {
        return res.status(429).json({
          error: "share_limit_reached",
          message: "Please contact Ground Truth 3D to share this project with additional people.",
        });
      }
    }

    const existing = await userService.getUser(email);
    const patch = { name };
    if (!existing) patch.projects = { [canonical]: "viewer" };
    await userService.upsertUser(email, patch);
    const currentRole = existing?.projects?.[canonical];
    if (currentRole !== "editor") {
      await userService.setProjectRole(email, canonical, "viewer");
    }

    const proto = req.get("x-forwarded-proto") || req.protocol || "https";
    const host = req.get("x-forwarded-host") || req.get("host");
    const projectUrl = `${proto}://${host}/public/${encodeURIComponent(canonical)}`;
    const coverPath = await resolveProjectCover(physical);
    let thumbnailUrl = null;
    if (coverPath) {
      try {
        const [signed] = await imageBucket.file(coverPath).getSignedUrl({
          version: "v4",
          action: "read",
          expires: Date.now() + 7 * 24 * 60 * 60 * 1000,
        });
        thumbnailUrl = signed;
      } catch (err) {
        console.warn(`[share-public] signed thumbnail URL failed for ${coverPath}:`, err.message);
      }
    }

    const invite = await emailService.sendShareInvite({
      toEmail: email,
      toName: name,
      fromName: "",
      fromEmail: "",
      project: display,
      projectUrl,
      thumbnailUrl,
    });

    // Record the per-IP throttle marker only after the share succeeded — if
    // the upsert or invite blew up we don't want to lock the visitor out of
    // retrying. Best-effort write; a marker failure shouldn't fail the
    // request the user already perceives as successful.
    try {
      await throttleFile.save(JSON.stringify({
        project: canonical,
        recipientEmail: email,
        recipientName: name,
        timestamp: new Date().toISOString(),
      }), { contentType: "application/json" });
    } catch (markerErr) {
      console.warn("[share-public] throttle marker write failed:", markerErr.message);
    }

    res.json({
      success: true,
      email,
      name,
      project: canonical,
      role: currentRole === "editor" ? "editor" : "viewer",
      emailSent: invite.sent,
      emailReason: invite.sent ? undefined : invite.reason,
      projectUrl,
    });
  } catch (err) {
    console.error("share-project-public error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/share-project", requireProjectRole("editor"), async (req, res) => {
  // After ensureProjectResolved + requireProjectRole: req.body.project is
  // physical prefix (path), req.projectCanonical is the URL/users.json
  // slug, req.projectDisplay is the human-readable name for the email.
  const physical = String((req.body && req.body.project) || req.projectName || "").trim();
  const canonical = String(req.projectCanonical || physical).trim();
  const display = String(req.projectDisplay || canonical).trim();
  if (!physical) return res.status(400).json({ error: "project required" });
  const rawEmail = (req.body && req.body.email) || "";
  const rawName = (req.body && req.body.name) || "";
  const email = String(rawEmail).trim().toLowerCase();
  const name = String(rawName).trim();
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: "Valid email required" });
  }
  if (!name) return res.status(400).json({ error: "Name required" });
  try {
    const existing = await userService.getUser(email);
    const patch = { name };
    if (!existing) patch.projects = { [canonical]: "viewer" };
    await userService.upsertUser(email, patch);
    const currentRole = existing?.projects?.[canonical];
    if (currentRole !== "editor") {
      await userService.setProjectRole(email, canonical, "viewer");
    }

    const proto = req.get("x-forwarded-proto") || req.protocol || "https";
    const host = req.get("x-forwarded-host") || req.get("host");
    // Send invitees to the /public/<project> mirror — that path is routed to
    // the no-IAP backend in the URL map so the recipient can land on it
    // without being on the IAP allow-list.
    const projectUrl = `${proto}://${host}/public/${encodeURIComponent(canonical)}`;
    const coverPath = await resolveProjectCover(physical);
    // Cloud Run is fronted by IAP, so the /api/2d/image proxy isn't reachable
    // from an email client without a Google login. Sign a short-lived URL
    // straight to the GCS object instead — same pattern used for plan/model
    // download links.
    let thumbnailUrl = null;
    if (coverPath) {
      try {
        const [signed] = await imageBucket.file(coverPath).getSignedUrl({
          version: "v4",
          action: "read",
          expires: Date.now() + 7 * 24 * 60 * 60 * 1000,
        });
        thumbnailUrl = signed;
      } catch (err) {
        console.warn(`[share] signed thumbnail URL failed for ${coverPath}:`, err.message);
      }
    }

    const invite = await emailService.sendShareInvite({
      toEmail: email,
      toName: name,
      fromName: req.user?.name || req.user?.email || "A teammate",
      fromEmail: req.user?.email || "",
      project: display,
      projectUrl,
      thumbnailUrl,
    });

    res.json({
      success: true,
      email,
      name,
      project: canonical,
      role: currentRole === "editor" ? "editor" : "viewer",
      emailSent: invite.sent,
      emailReason: invite.sent ? undefined : invite.reason,
      projectUrl,
    });
  } catch (err) {
    console.error("Share project error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Anonymous "request access" form on the public landing page. Sends an email
// to the platform owner (j@gt3d.com). No auth required — the form is the
// front door for prospective users.
app.post("/api/contact-request", async (req, res) => {
  // Honeypot — bots fill all fields, humans don't see this one. Reply OK so
  // we don't tip off the bot that it was rejected.
  if (req.body && req.body.website) return res.json({ success: true });

  const name = String((req.body && req.body.name) || "").trim();
  const company = String((req.body && req.body.company) || "").trim();
  const email = String((req.body && req.body.email) || "").trim().toLowerCase();
  if (!name) return res.status(400).json({ error: "Name is required" });
  if (!company) return res.status(400).json({ error: "Company is required" });
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: "Valid email is required" });
  }

  try {
    const result = await emailService.sendContactRequest({ name, company, email });
    res.json({ success: true, emailSent: result.sent, emailReason: result.sent ? undefined : result.reason });
  } catch (err) {
    console.error("Contact request error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Public per-project home page at /<project-name>. Placed LAST so all static
// files, pretty URLs (/map-viewer, /models, /plans), and /api/* routes take
// priority. Anything with a "." in the path is treated as a filename and
// falls through to 404 (so the static handler already tried it).
app.get("/:project", async (req, res, next) => {
  const project = req.params.project;
  if (!project || project.includes(".") || project.startsWith("_")) return next();
  // Reserved top-level words that are not projects
  const reserved = new Set([
    "api", "map-viewer", "models", "pointclouds", "plans", "images", "documents", "projects", "public",
    "robots.txt", "tokens.css", "app.css", "me.js",
  ]);
  if (reserved.has(project)) return next();
  if (await maybeRedirectCanonical(req, res, "", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-home.html"));
});

app.listen(PORT, () => {
  console.log(`GCS Uploader running on port ${PORT}`);
});

// Phase-1 indexing: register every existing project as layout:"old" in
// _platform/slug-index.json. Idempotent. Fire-and-forget so startup isn't
// blocked on a GCS round trip; the resolver tolerates a missing/empty
// index file. Retries with exponential backoff because a failure leaves
// migration-guard checks fail-open until the next deploy.
//
// Opt out with DISABLE_BOOT_INDEXING=1 (useful for inspecting state
// before any indexing has happened).
(async () => {
  if (process.env.DISABLE_BOOT_INDEXING === "1") {
    console.log('[index] skipped (DISABLE_BOOT_INDEXING=1)');
    return;
  }
  const delays = [0, 5_000, 20_000, 60_000];
  for (let attempt = 0; attempt < delays.length; attempt++) {
    if (delays[attempt]) await new Promise((r) => setTimeout(r, delays[attempt]));
    try {
      const map = await getProjectNameMap();
      const names = [...map.values()];
      if (names.length === 0) {
        console.log('[index] no project folders found; nothing to seed');
        return;
      }
      await projectResolver.buildLegacyIndex(names);
      console.log(`[index] seeded slug-index with ${names.length} legacy project(s) (attempt ${attempt + 1})`);
      return;
    } catch (err) {
      const final = attempt === delays.length - 1;
      const level = final ? 'error' : 'warn';
      console[level](`[index] seed attempt ${attempt + 1} failed: ${err.message}${final ? ' — giving up; migration guard will fail open until next deploy' : ''}`);
    }
  }
})();
