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

// ---- Address normalization ----
// Common-case fixer for the address field on the new-project form.
// Returns { normalized, autoCorrected, needsReview, warnings: [...] }.
// Autocorrect rules: fix CamelCase boundary after a street type (e.g.
// "Treat AveSan Francisco" -> "Treat Ave, San Francisco"), uppercase the
// state abbreviation, collapse multiple spaces. Flags ambiguous cases
// (no street number, missing state/zip, fewer than 2 commas) for user
// review rather than guessing.
function normalizeAddress(addr) {
  if (typeof addr !== "string") {
    return { normalized: "", autoCorrected: false, needsReview: true, warnings: ["empty"] };
  }
  let s = addr.trim();
  if (!s) return { normalized: "", autoCorrected: false, needsReview: true, warnings: ["empty"] };

  let auto = false;
  const warnings = [];

  // Collapse internal whitespace
  const collapsed = s.replace(/\s+/g, " ");
  if (collapsed !== s) auto = true;
  s = collapsed;

  // Insert a comma at a CamelCase boundary that follows a recognised
  // street-type token (typical typo: typing the address with no comma
  // between "Ave" and the city name). Conservative: only fires when the
  // street type token is followed directly by a capital letter and a
  // lowercase letter (i.e. "AveSan", "RoadBel"), not just any case shift.
  const cm = s.match(
    /^(.*?\b(?:St|Street|Ave|Avenue|Blvd|Boulevard|Rd|Road|Dr|Drive|Ln|Lane|Way|Ct|Court|Pl|Place|Hwy|Highway|Pkwy|Parkway|Cir|Circle|Ter|Terrace))([A-Z][a-z])(.*)$/
  );
  if (cm) {
    s = cm[1] + ", " + cm[2] + cm[3];
    auto = true;
  }

  // Tidy comma spacing: ensure exactly one space after each comma.
  const tidied = s.replace(/\s*,\s*/g, ", ");
  if (tidied !== s) auto = true;
  s = tidied;

  // Split into parts; common shape is [street, city, "STATE ZIP"].
  const parts = s.split(",").map((p) => p.trim()).filter(Boolean);

  if (parts.length < 2) warnings.push("missing-commas");

  // Last part should be "STATE ZIP" — uppercase the state if present.
  if (parts.length >= 2) {
    const last = parts[parts.length - 1];
    const m = last.match(/^([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$/);
    if (m) {
      const upper = m[1].toUpperCase() + " " + m[2];
      if (upper !== last) {
        parts[parts.length - 1] = upper;
        auto = true;
      }
    } else {
      warnings.push("malformed-state-zip");
    }
  } else {
    warnings.push("missing-state-zip");
  }

  // First part should contain a digit (street number).
  if (parts.length >= 1 && !/\d/.test(parts[0])) {
    warnings.push("missing-street-number");
  }

  s = parts.join(", ");
  return { normalized: s, autoCorrected: auto, needsReview: warnings.length > 0, warnings };
}

// ---- Property store (read-modify-write of _platform/properties.json) ----
const PROPERTIES_PATH = "_platform/properties.json";
let propertiesWriteQueue = Promise.resolve();

async function loadProperties() {
  const f = bucket.file(PROPERTIES_PATH);
  const [exists] = await f.exists();
  if (!exists) return { version: 0, properties: {} };
  const [content] = await f.download();
  return JSON.parse(content.toString());
}

function mutateProperties(fn) {
  // Same in-process serialization pattern as the slug-index writer.
  const next = propertiesWriteQueue.then(async () => {
    const data = await loadProperties();
    if (!data.properties) data.properties = {};
    const result = await fn(data);
    data.version = (data.version || 0) + 1;
    await bucket
      .file(PROPERTIES_PATH)
      .save(JSON.stringify(data, null, 2), { contentType: "application/json" });
    return result;
  });
  propertiesWriteQueue = next.catch(() => {});
  return next;
}

async function getProperty(propertyId) {
  const data = await loadProperties();
  return data.properties[propertyId] || null;
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

// Property landing page — /property/<propertyId>. Audience is anyone who
// has access to at least one project on the property; admins see all
// properties. The page itself fetches /api/property/<id>/overview which
// enforces the access check.
app.get("/property/:propertyId", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "property.html"));
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

// Per-project videos listing: /videos/<project-name>. Page renders for
// anyone — the video list is filtered by role inside /api/video/files
// (non-admins see only non-hidden videos with no uploader info) and
// admins fetch the full set via /api/admin/video/files.
app.get("/videos/:project", async (req, res) => {
  if (await maybeRedirectCanonical(req, res, "/videos", req.params.project)) return;
  res.sendFile(path.join(__dirname, "public", "project-videos.html"));
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
  // Truthy short-circuit only — a previous null result means the first
  // call ran before the body was parsed (multer hasn't fired yet on
  // multipart routes), so a retry from requireProjectRole is allowed to
  // resolve once req.body.project becomes available. A real resolved
  // object still short-circuits subsequent calls.
  if (req.projectResolved) return;
  const raw = resolveProjectFromRequest(req);
  if (!raw) {
    if (req.projectResolved === undefined) req.projectResolved = null;
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

// Permission roster (users.json) is keyed by whatever name was in use
// when the entry was last written: the old project name pre-switchover,
// the compound canonical slug post-switchover, or any historical alias.
// Try the canonical first, then any alias pointing to it, so a user
// keeps access through both states without an atomic flip.
async function hasAliasAwareAccess(email, canonical, minRole) {
  if (await userService.hasProjectAccess(email, canonical, minRole)) return true;
  try {
    const idx = await projectResolver.getIndex();
    if (idx && idx.alias) {
      for (const [alias, target] of Object.entries(idx.alias)) {
        if (target !== canonical) continue;
        if (alias === canonical) continue;
        if (await userService.hasProjectAccess(email, alias, minRole)) return true;
      }
    }
  } catch {}
  return false;
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
    const ok = await hasAliasAwareAccess(req.user.email, canonical, minRole);
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

// ---- Property endpoints ----
// List properties for the new-project form's "Select Property" dropdown.
// Admin-only for now; the form is admin-only too. Future work: open to
// non-admin editors so they can add projects to properties they have
// access to.
app.get("/api/properties", requireAdmin, async (req, res) => {
  try {
    const data = await loadProperties();
    const list = Object.values(data.properties || {})
      .map((p) => ({
        propertyId: p.propertyId,
        name: p.name,
        slug: p.slug,
        address: p.address,
        coverPhoto: p.coverPhoto || null,
        needsAddress: !!p.needsAddress,
        projectCount: (p.projectIds || []).length,
      }))
      .sort((a, b) => (a.name || "").localeCompare(b.name || ""));
    res.json(list);
  } catch (err) {
    console.error("List properties error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Single property by id — returns full record (admin only). Used by the
// new-project form to populate address + cover photo when an existing
// property is selected.
app.get("/api/property/:propertyId", requireAdmin, async (req, res) => {
  try {
    const property = await getProperty(req.params.propertyId);
    if (!property) return res.status(404).json({ error: "Property not found" });
    res.json(property);
  } catch (err) {
    console.error("Get property error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Property overview for the /property/<id> landing page. Returns the
// property record + the projects on it that the caller can view.
// Auth: signed-in user with access to ≥1 project on this property,
// or admin (admins see all). Anonymous → 401.
app.get("/api/property/:propertyId/overview", async (req, res) => {
  if (!req.user || !req.user.email) return res.status(401).json({ error: "Authentication required" });
  try {
    const propertyId = req.params.propertyId;
    const property = await getProperty(propertyId);
    if (!property) return res.status(404).json({ error: "Property not found" });

    const idx = await projectResolver.getIndex();
    const matches = [];
    if (idx && idx.canonical) {
      for (const [slug, ref] of Object.entries(idx.canonical)) {
        if (ref && ref.layout === "new" && ref.propertyId === propertyId) {
          matches.push({ canonicalSlug: slug, projectId: ref.projectId });
        }
      }
    }

    const isAdmin = !!req.user.isAdmin;
    let canView = isAdmin;
    if (!canView) {
      for (const m of matches) {
        if (await hasAliasAwareAccess(req.user.email, m.canonicalSlug, "viewer")) {
          canView = true;
          break;
        }
      }
    }
    if (!canView) return res.status(403).json({ error: "Access denied" });

    // Pull project name + cover from each project.json. Skip projects
    // the caller can't view (admins keep them all).
    const enriched = await Promise.all(matches.map(async (m) => {
      try {
        const f = bucket.file(`${propertyId}/${m.projectId}/project.json`);
        const [exists] = await f.exists();
        const data = exists ? JSON.parse((await f.download())[0].toString()) : {};
        return {
          canonicalSlug: m.canonicalSlug,
          projectId: m.projectId,
          name: data.name || m.canonicalSlug,
          coverPhoto: data.coverPhoto || null,
        };
      } catch {
        return { canonicalSlug: m.canonicalSlug, projectId: m.projectId, name: m.canonicalSlug, coverPhoto: null };
      }
    }));

    const visibleProjects = isAdmin
      ? enriched
      : (await Promise.all(enriched.map(async (p) => {
          const ok = await hasAliasAwareAccess(req.user.email, p.canonicalSlug, "viewer");
          return ok ? p : null;
        }))).filter(Boolean);

    visibleProjects.sort((a, b) => (a.name || "").localeCompare(b.name || ""));

    res.json({
      propertyId: property.propertyId,
      name: property.name,
      slug: property.slug,
      address: property.address,
      coverPhoto: property.coverPhoto || null,
      isAdmin,
      canEditProperty: isAdmin,
      projects: visibleProjects,
    });
  } catch (err) {
    console.error("Property overview error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// List the properties the current user can land on. Used by the
// property selector on /property/<id>: campus managers with multiple
// properties get a dropdown to switch contexts. Admins see all
// properties. Returns lightweight summaries (no nested projects).
app.get("/api/my-properties", async (req, res) => {
  if (!req.user || !req.user.email) return res.status(401).json({ error: "Authentication required" });
  try {
    const data = await loadProperties();
    const allProperties = Object.values(data.properties || {});
    const isAdmin = !!req.user.isAdmin;

    let visible;
    if (isAdmin) {
      visible = allProperties;
    } else {
      const projectKeys = Object.keys(req.user.projects || {});
      if (projectKeys.length === 0) return res.json([]);
      const idx = await projectResolver.getIndex();
      const propertyIds = new Set();
      if (idx && idx.canonical) {
        for (const slug of projectKeys) {
          const canonical = (idx.alias && idx.alias[slug]) || slug;
          const ref = idx.canonical[canonical];
          if (ref && ref.layout === "new" && ref.propertyId) {
            propertyIds.add(ref.propertyId);
          }
        }
      }
      visible = allProperties.filter((p) => propertyIds.has(p.propertyId));
    }

    const list = visible.map((p) => ({
      propertyId: p.propertyId,
      name: p.name,
      address: p.address,
      slug: p.slug,
      projectCount: (p.projectIds || []).length,
    }));
    list.sort((a, b) => (a.name || "").localeCompare(b.name || ""));
    res.json(list);
  } catch (err) {
    console.error("My properties error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Address punctuation/casing normalizer. Frontend can call this for
// live feedback before submit. Server also calls it inline on
// /api/create-project; this endpoint is purely for UX.
app.post("/api/validate-address", requireAdmin, async (req, res) => {
  const addr = (req.body && req.body.address) || "";
  res.json(normalizeAddress(addr));
});

// Update an existing property — name, address, and/or cover photo.
// All fields optional; only changed fields are written. Address is
// validated/normalized the same way the create flow does.
app.post(
  "/api/property/:propertyId",
  requireAdmin,
  upload.single("propertyCoverPhoto"),
  async (req, res) => {
    const propertyId = req.params.propertyId;
    const cleanup = () => req.file && fs.unlink(req.file.path, () => {});

    try {
      const existing = await getProperty(propertyId);
      if (!existing) {
        cleanup();
        return res.status(404).json({ error: "Property not found" });
      }

      const updates = {};

      const newName = (req.body.name || "").trim();
      if (newName && newName !== existing.name) updates.name = newName;

      const rawAddress = (req.body.address || "").trim();
      if (rawAddress && rawAddress !== existing.address) {
        const norm = normalizeAddress(rawAddress);
        if (norm.needsReview && !norm.autoCorrected) {
          cleanup();
          return res.status(400).json({
            error: "address_needs_review",
            message: "The address looks incomplete or malformed.",
            suggestion: norm.normalized,
            warnings: norm.warnings,
          });
        }
        updates.address = norm.normalized;
        // Clearing the needsAddress flag — user has just confirmed an
        // address that passed validation.
        if (existing.needsAddress) updates.needsAddress = false;
      }

      if (req.file) {
        const ext = path.extname(req.file.originalname) || ".jpg";
        const coverPath = `${propertyId}/_property-cover${ext}`;
        const gcs = imageBucket.file(coverPath);
        await new Promise((resolve, reject) => {
          fs.createReadStream(req.file.path)
            .pipe(
              gcs.createWriteStream({
                resumable: false,
                metadata: { contentType: req.file.mimetype || "image/jpeg" },
              })
            )
            .on("error", reject)
            .on("finish", resolve);
        });
        fs.unlink(req.file.path, () => {});
        updates.coverPhoto = coverPath;
      }

      if (Object.keys(updates).length === 0) {
        return res.json({ success: true, property: existing, message: "no changes" });
      }

      const updated = await mutateProperties((data) => {
        const p = data.properties[propertyId];
        if (!p) throw new Error("Property disappeared mid-update");
        Object.assign(p, updates, { updatedAt: new Date().toISOString() });
        return p;
      });

      console.log(`Updated property ${propertyId}: ${Object.keys(updates).join(", ")}`);
      res.json({ success: true, property: updated });
    } catch (err) {
      console.error("Update property error:", err.message);
      cleanup();
      res.status(500).json({ error: err.message });
    }
  }
);

app.get("/api/projects", async (req, res) => {
  try {
    if (!req.user) return res.json([]);

    // Drive from the slug index — its canonical entries are the
    // authoritative project list. Listing GCS prefixes was leaking
    // both stale pre-migration folders AND the post-migration
    // property-id roots, and would silently break post-switchover
    // (when users.json keys flip but GCS folders don't).
    const idx = await projectResolver.getIndex();
    const canonicals = Object.keys(idx.canonical || {});

    // Reverse-alias map: canonical -> [aliases pointing at it]. Used
    // both for permission matching (users.json may key by any alias)
    // and for resolving project-order entries through aliases.
    const reverseAlias = new Map();
    for (const [alias, target] of Object.entries(idx.alias || {})) {
      if (!reverseAlias.has(target)) reverseAlias.set(target, []);
      reverseAlias.get(target).push(alias);
    }

    let accessible;
    if (req.user.isAdmin) {
      accessible = canonicals.slice();
    } else {
      const userKeys = new Set(Object.keys(req.user.projects || {}));
      accessible = canonicals.filter((canonical) => {
        if (userKeys.has(canonical)) return true;
        const aliases = reverseAlias.get(canonical) || [];
        return aliases.some((a) => userKeys.has(a));
      });
    }

    // Admin-managed order. Pre-switchover this file holds old names;
    // post-switchover the switchover script will replace it with
    // property-order.json (different shape — handled separately when
    // it lands). For now resolve old names through aliases to
    // canonicals so saved order survives migration without manual
    // re-ordering.
    const savedOrder = await loadProjectOrder();
    const accessibleSet = new Set(accessible);
    const ordered = [];
    const placed = new Set();
    for (const name of savedOrder) {
      const target = (idx.alias && idx.alias[name]) || name;
      if (accessibleSet.has(target) && !placed.has(target)) {
        ordered.push(target);
        placed.add(target);
      }
    }
    for (const c of accessible) {
      if (!placed.has(c)) ordered.push(c);
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

// Create a project — always in the new (property/project) layout. Two
// modes by `propertyId`:
//   absent  → create a new property using the address field, then attach
//             the first project to it.
//   present → attach a new project to that existing property.
//
// Storage uses {propertyId}/{projectId}/... in all four buckets so it
// matches the post-migration layout the resolver already understands.
// Two photo fields: `coverPhoto` (project) and `propertyCoverPhoto`
// (property — only set when creating a new property or replacing the
// existing property's cover).
function shortPropProjId() {
  return crypto.randomUUID().replace(/-/g, "").slice(0, 16);
}

app.post(
  "/api/create-project",
  requireAdmin,
  upload.fields([
    { name: "coverPhoto", maxCount: 1 },
    { name: "propertyCoverPhoto", maxCount: 1 },
  ]),
  async (req, res) => {
    const cleanupTmp = () => {
      const f = req.files || {};
      if (f.coverPhoto?.[0]?.path) fs.unlink(f.coverPhoto[0].path, () => {});
      if (f.propertyCoverPhoto?.[0]?.path) fs.unlink(f.propertyCoverPhoto[0].path, () => {});
    };

    const projectName = (req.body.name || "").trim();
    const propertyIdInput = (req.body.propertyId || "").trim();
    const rawAddress = (req.body.address || "").trim();
    if (!projectName) {
      cleanupTmp();
      return res.status(400).json({ error: "Project name is required" });
    }

    // Parse levels (same defaulting as before).
    let levels = DEFAULT_LEVELS;
    if (typeof req.body.levels === "string" && req.body.levels.trim()) {
      try {
        const parsed = JSON.parse(req.body.levels);
        if (Array.isArray(parsed)) {
          levels = parsed
            .map((l) => String(l || "").trim().toLowerCase())
            .filter(Boolean)
            .filter((l) => DEFAULT_LEVELS.includes(l));
        }
      } catch { /* use defaults */ }
    }

    try {
      let propertyId, propertySlug, propertyName, propertyAddress;
      let isNewProperty;
      let updatedAddress = false;

      if (propertyIdInput) {
        // ---- Add to existing property ----
        const existing = await getProperty(propertyIdInput);
        if (!existing) {
          cleanupTmp();
          return res.status(404).json({ error: "Selected property not found" });
        }
        propertyId = existing.propertyId;
        propertySlug = existing.slug;
        propertyName = existing.name;
        propertyAddress = existing.address || "";
        isNewProperty = false;
        // Editor can also amend the address from this form. Validate
        // only if they actually changed it.
        if (rawAddress && rawAddress !== existing.address) {
          const norm = normalizeAddress(rawAddress);
          if (norm.needsReview && !norm.autoCorrected) {
            cleanupTmp();
            return res.status(400).json({
              error: "address_needs_review",
              message: "The address looks incomplete or malformed.",
              suggestion: norm.normalized,
              warnings: norm.warnings,
            });
          }
          propertyAddress = norm.normalized;
          updatedAddress = true;
        }
      } else {
        // ---- New property ----
        if (!rawAddress) {
          cleanupTmp();
          return res.status(400).json({ error: "Address is required for a new property" });
        }
        const norm = normalizeAddress(rawAddress);
        if (norm.needsReview && !norm.autoCorrected) {
          cleanupTmp();
          return res.status(400).json({
            error: "address_needs_review",
            message: "The address looks incomplete or malformed.",
            suggestion: norm.normalized,
            warnings: norm.warnings,
          });
        }
        propertyAddress = norm.normalized;
        propertyId = `prop_${shortPropProjId()}`;
        propertyName = propertyAddress;
        propertySlug =
          projectResolver.slugify(propertyAddress) ||
          projectResolver.slugify(projectName) ||
          "property";
        isNewProperty = true;
      }

      const projectId = `proj_${shortPropProjId()}`;
      const projectSlug = projectResolver.slugify(projectName) || `project-${Date.now()}`;
      const compoundSlug = `${propertySlug}--${projectSlug}`;
      const newPrefix = `${propertyId}/${projectId}`;

      // Folder markers — same shape as the migration script produces.
      await bucket.file(`${newPrefix}/`).save("", { contentType: "application/x-directory" });
      for (const lvl of levels) {
        await bucket.file(`${newPrefix}/${lvl}/`).save("", { contentType: "application/x-directory" });
      }
      await imageBucket.file(`${newPrefix}/`).save("", { contentType: "application/x-directory" });
      try {
        await modelBucket.file(`${newPrefix}/`).save("", { contentType: "application/x-directory" });
      } catch { /* non-fatal */ }

      // Project cover photo (right uploader on the form).
      let projectCoverPath = null;
      const projectCoverFile = req.files?.coverPhoto?.[0];
      if (projectCoverFile) {
        const ext = path.extname(projectCoverFile.originalname) || ".jpg";
        projectCoverPath = `${newPrefix}/cover${ext}`;
        const gcs = imageBucket.file(projectCoverPath);
        await new Promise((resolve, reject) => {
          fs.createReadStream(projectCoverFile.path)
            .pipe(
              gcs.createWriteStream({
                resumable: false,
                metadata: { contentType: projectCoverFile.mimetype || "image/jpeg" },
              })
            )
            .on("error", reject)
            .on("finish", resolve);
        });
        fs.unlink(projectCoverFile.path, () => {});
      }

      // Property cover photo (left uploader on the form). Stored at
      // imageBucket/{propertyId}/_property-cover.{ext} so it sits next
      // to the project subfolders without colliding with any.
      let propertyCoverPath = null;
      const propertyCoverFile = req.files?.propertyCoverPhoto?.[0];
      if (propertyCoverFile) {
        const ext = path.extname(propertyCoverFile.originalname) || ".jpg";
        propertyCoverPath = `${propertyId}/_property-cover${ext}`;
        const gcs = imageBucket.file(propertyCoverPath);
        await new Promise((resolve, reject) => {
          fs.createReadStream(propertyCoverFile.path)
            .pipe(
              gcs.createWriteStream({
                resumable: false,
                metadata: { contentType: propertyCoverFile.mimetype || "image/jpeg" },
              })
            )
            .on("error", reject)
            .on("finish", resolve);
        });
        fs.unlink(propertyCoverFile.path, () => {});
      }

      // Project metadata at the new layout's project.json.
      const projectMeta = {
        name: projectName,
        propertyId,
        projectId,
        slug: compoundSlug,
        coverPhoto: projectCoverPath,
        createdAt: new Date().toISOString(),
      };
      await bucket.file(`${newPrefix}/project.json`).save(
        JSON.stringify(projectMeta, null, 2),
        { contentType: "application/json" }
      );

      // Property record — create or update under a single mutation so
      // concurrent /api/create-project calls don't clobber each other.
      await mutateProperties((data) => {
        let prop = data.properties[propertyId];
        if (!prop) {
          prop = {
            propertyId,
            name: propertyName,
            slug: propertySlug,
            address: propertyAddress,
            needsAddress: false,
            createdAt: new Date().toISOString(),
            createdBy: req.user?.email || null,
            projectIds: [],
          };
          data.properties[propertyId] = prop;
        } else if (updatedAddress) {
          prop.address = propertyAddress;
          prop.needsAddress = false;
        }
        if (propertyCoverPath) prop.coverPhoto = propertyCoverPath;
        if (!prop.projectIds.includes(projectId)) prop.projectIds.push(projectId);
      });

      // Slug-index registration — layout:"new" so the resolver picks it
      // up immediately. registerNewProject does not write any aliases;
      // a fresh project has no historical names to redirect from.
      try {
        await projectResolver.registerNewProject({ propertyId, projectId, compoundSlug });
      } catch (err) {
        console.warn(`[index] failed to register new project "${compoundSlug}":`, err.message);
      }

      console.log(
        `Created project: ${projectName} (${compoundSlug}) on ${isNewProperty ? "new" : "existing"} property ${propertyId}`
      );
      invalidateProjectNameCache();

      res.json({
        success: true,
        propertyId,
        projectId,
        slug: compoundSlug,
        url: `/${encodeURIComponent(compoundSlug)}`,
        propertyCoverPhoto: propertyCoverPath,
        projectCoverPhoto: projectCoverPath,
        isNewProperty,
      });
    } catch (err) {
      console.error("Create project error:", err.message);
      cleanupTmp();
      res.status(500).json({ error: err.message });
    }
  }
);

// Update project metadata (address + optional cover photo). Name is immutable.
// For migrated projects, address lives on the property record — writes route
// there instead of into project.json so the address stays in one place.
app.post("/api/update-project", upload.single("coverPhoto"), requireProjectRole("editor"), async (req, res) => {
  const name = (req.body.name || "").trim();
  const address = (req.body.address || "").trim();
  if (!name) return res.status(400).json({ error: "Project name is required" });

  // Storage prefix: use the resolved physical path when available so
  // migrated projects get written to {propertyId}/{projectId}/, not the
  // stale {projectName}/ folder.
  const prefix = req.projectName || name;
  const isMigrated =
    !!(req.projectResolved && req.projectResolved.layout === "new" && req.projectResolved.propertyId);

  try {
    // Validate any address change first so we don't write a cover photo
    // and then bail out on the address.
    let addressForProperty = null;
    if (isMigrated && address) {
      const currentProperty = await getProperty(req.projectResolved.propertyId);
      const currentAddress = currentProperty ? currentProperty.address : "";
      if (address !== currentAddress) {
        const norm = normalizeAddress(address);
        if (norm.needsReview && !norm.autoCorrected) {
          if (req.file) fs.unlink(req.file.path, () => {});
          return res.status(400).json({
            error: "address_needs_review",
            message: "The address looks incomplete or malformed.",
            suggestion: norm.normalized,
            warnings: norm.warnings,
          });
        }
        addressForProperty = norm.normalized;
      }
    }

    // Verify project metadata exists at the resolved path.
    const metaFile = bucket.file(`${prefix}/project.json`);
    const [metaExists] = await metaFile.exists();
    let existing = { name, address: null, coverPhoto: null };
    if (metaExists) {
      const [content] = await metaFile.download();
      try { existing = JSON.parse(content.toString()); } catch {}
    }

    // Upload new cover photo if provided.
    let coverPhotoPath = existing.coverPhoto || null;
    if (req.file) {
      const ext = path.extname(req.file.originalname) || ".jpg";
      const coverName = `cover${ext}`;
      coverPhotoPath = `${prefix}/${coverName}`;
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

    // Persist project.json. For migrated projects, address is omitted
    // and goes onto the property record below.
    const metadata = {
      ...existing,
      name,
      coverPhoto: coverPhotoPath,
      createdAt: existing.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    if (isMigrated) {
      delete metadata.address;
    } else {
      metadata.address = address;
    }
    await metaFile.save(JSON.stringify(metadata, null, 2), { contentType: "application/json" });

    // Address goes onto the property record for migrated projects.
    if (isMigrated && addressForProperty !== null) {
      try {
        await mutateProperties((data) => {
          const p = data.properties[req.projectResolved.propertyId];
          if (p) {
            p.address = addressForProperty;
            if (p.needsAddress) p.needsAddress = false;
            p.updatedAt = new Date().toISOString();
          }
        });
      } catch (err) {
        console.warn("[update-project] property address write failed:", err.message);
      }
    }

    // The metadata we send back includes the address that was actually
    // applied, regardless of whether it landed on the property or the
    // project — the form just shows it in one field.
    const responseMeta = {
      ...metadata,
      address: isMigrated ? (addressForProperty !== null ? addressForProperty : address) : metadata.address,
    };
    console.log(`Updated project: ${name} (${prefix})`);
    res.json({ success: true, project: name, metadata: responseMeta });
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

  // Refuse for migrated projects — the move-folder + rewrite-mappings
  // logic below assumes a single layout:"old" prefix. Renaming a
  // layout:"new" project should change project.json.name and the
  // canonical slug while leaving the propertyId/projectId storage
  // roots untouched, which has design choices (does the URL change?
  // does the property name change?) that need the property/project
  // edit UI to land first.
  try {
    const existing = await projectResolver.resolveProject(oldName);
    if (existing && existing.layout === "new") {
      return res.status(400).json({
        error: "This project has been migrated to the property model. Renaming via this endpoint is not yet supported — use the property/project edit UI when it ships.",
      });
    }
  } catch (err) {
    console.warn("[rename-project] resolver lookup failed:", err.message);
  }

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

  // Refuse for migrated projects — proper deletion needs to remove the
  // {propertyId}/{projectId}/ tree, _thumbs/{propertyId}/{projectId}/,
  // the slug-index canonical + aliases, and the projectId from
  // properties.json (deleting the property entry if it was the last
  // project). Out of scope until the property/project edit UI lands.
  try {
    const existing = await projectResolver.resolveProject(name);
    if (existing && existing.layout === "new") {
      return res.status(400).json({
        error: "This project has been migrated to the property model. Deletion via this endpoint is not yet supported — use the property/project edit UI when it ships.",
      });
    }
  } catch (err) {
    console.warn("[delete-project] resolver lookup failed:", err.message);
  }

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
  const validCards = new Set(["main", "images", "plans", "models", "pointclouds", "documents"]);
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
    const data = JSON.parse(content.toString());
    // For migrated projects (layout:"new"), the address lives on the
    // property record rather than in project.json. Pull it through so
    // the response shape matches what edit-project.html expects, and
    // surface propertyProjectCount so the projects index can link the
    // address to /property/<id> when more than one project shares it.
    if (data.propertyId) {
      try {
        const property = await getProperty(data.propertyId);
        if (property) {
          if (!data.address) data.address = property.address || null;
          data.propertyName = property.name || null;
          data.propertyCoverPhoto = property.coverPhoto || null;
          data.propertyProjectCount = Array.isArray(property.projectIds) ? property.projectIds.length : 0;
        }
      } catch { /* leave fields null on error */ }
    }
    res.json(data);
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
  const { bucket: bucketKind, project, fileName, contentType, level, scope: rawScope } = req.body || {};
  if (!bucketKind || !fileName || !contentType) {
    return res.status(400).json({ error: "bucket, fileName, and contentType are required" });
  }
  if (!project) return res.status(400).json({ error: "project is required" });
  const scope = rawScope === "shared" ? "shared" : "project";

  // Authorization. ensureProjectResolved has already mutated req.body.project
  // to the physical GCS prefix; the alias-aware permission check needs the
  // canonical slug stashed by the resolver instead — using `project` here
  // (post-mutation physical path) silently fails for migrated projects
  // because users.json is keyed by canonical slug. req.projectCanonical is
  // the right key.
  const canonicalForAuth = req.projectCanonical || project;
  const isAssetThumbnail =
    (bucketKind === "model" || bucketKind === "pointcloud" || bucketKind === "plan") &&
    fileName.endsWith(".thumb.jpg");
  // Plans and models stay admin-only for primary uploads — the showcase
  // pages assume admin-vetted content. Per-file thumbnails on those
  // categories are editor-gated since they pair with files editors
  // already manage. Documents and videos are now editor-accessible across
  // the board (upload/edit/delete) so editors can manage them on projects
  // they own; admin-only operations remain under /api/admin/.
  const planModelPcAdminOnly = bucketKind === "model" || bucketKind === "plan" || bucketKind === "pointcloud";
  if (planModelPcAdminOnly && !isAssetThumbnail) {
    if (!req.user?.isAdmin) return res.status(403).json({ error: `Admin required for ${bucketKind} uploads` });
  } else if (!req.user?.isAdmin) {
    const ok = await hasAliasAwareAccess(req.user?.email, canonicalForAuth, "editor");
    if (!ok) return res.status(403).json({ error: `editor access to ${req.projectDisplay || canonicalForAuth} required` });
  }

  let target, bucketName;
  if (bucketKind === "2d") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else if (bucketKind === "model") { target = modelBucket; bucketName = MODEL_BUCKET_NAME; }
  else if (bucketKind === "plan") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else if (bucketKind === "pointcloud") { target = pointcloudBucket; bucketName = POINTCLOUD_BUCKET_NAME; }
  else if (bucketKind === "document") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else if (bucketKind === "video") { target = imageBucket; bucketName = IMAGE_BUCKET_NAME; }
  else { target = bucket; bucketName = BUCKET_NAME; }

  // Sub-prefix per category — matches the per-project layout. Used for
  // both project-scoped and shared destinations.
  let sub = "";
  if (bucketKind === "plan") sub = "_plans/";
  else if (bucketKind === "document") sub = "_documents/";
  else if (bucketKind === "video") sub = "_videos/";

  let dest;
  if (scope === "shared") {
    const base = req.projectPaths && req.projectPaths.base;
    if (!base || !base.includes("/")) {
      return res.status(400).json({ error: "shared scope requires a property-layout project" });
    }
    const parts = base.split("/");
    const propertyId = parts[0];
    dest = `${propertyId}/_shared/${sub}${fileName}`;
    // ownerProjectId is stamped on the GCS object via /api/shared/ack
    // after the PUT completes. We deliberately don't sign an extension
    // header here — that would force the browser's CORS preflight to
    // ask GCS to allow x-goog-meta-* on the request, which fails
    // unless every bucket's CORS config explicitly whitelists it. The
    // ack pattern keeps the signed URL CORS-equivalent to a non-shared
    // upload (just Content-Type) and pushes the metadata write back to
    // a normal authenticated server endpoint.
  } else if (sub) {
    dest = `${project}/${sub}${fileName}`;
  } else if (project && level) {
    dest = `${project}/${level}/${fileName}`;
  } else if (project) {
    dest = `${project}/${fileName}`;
  } else {
    dest = fileName;
  }

  try {
    const [url] = await target.file(dest).getSignedUrl({
      version: "v4",
      action: "write",
      expires: Date.now() + 60 * 60 * 1000, // 60 minutes (large models/plans take time)
      contentType,
    });
    res.json({
      uploadUrl: url,
      gcsPath: dest,
      bucket: bucketName,
      // Always null now that we've moved to the ack pattern. Kept in
      // the response shape so existing clients that look for it don't
      // need a second deploy to ignore it.
      requiredHeaders: null,
      // True when the caller asked for scope=shared and the dest is
      // under <propertyId>/_shared/. Clients use this to decide whether
      // to call /api/shared/ack after a successful PUT.
      isShared: scope === "shared",
    });
  } catch (err) {
    console.error("Signed URL error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Post-upload ack for shared assets. The client calls this after a
// successful PUT to a shared signed URL; we stamp ownerProjectId on the
// GCS object's custom metadata so the listing layer can attribute it
// back and the shared-delete authorization can resolve the owner.
//
// Body: { file: "<propertyId>/_shared/...", bucket: "2d"|"plan"|"model"|"pointcloud"|"document"|"video" }
// (project comes from the resolver as usual.) Editor-gated; the file
// path must live under this requesting project's property's _shared/
// prefix.
app.post("/api/shared/ack", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, bucket: bucketKind } = req.body || {};
  if (!filePath || typeof filePath !== "string") return res.status(400).json({ error: "file is required" });
  const base = req.projectPaths && req.projectPaths.base;
  if (!base || !base.includes("/")) return res.status(400).json({ error: "project is not in property layout" });
  const propertyId = base.split("/")[0];
  const ownerProjectId = base.split("/")[1];
  if (!filePath.startsWith(`${propertyId}/_shared/`)) {
    return res.status(400).json({ error: "file is not a shared asset on this property" });
  }
  // Bucket lookup mirrors /api/upload-url. Default to imageBucket
  // (covers 2d/plan/document/video which all share that bucket).
  let bucketRef;
  if (bucketKind === "model") bucketRef = modelBucket;
  else if (bucketKind === "pointcloud") bucketRef = pointcloudBucket;
  else bucketRef = imageBucket;
  try {
    const file = bucketRef.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    const [meta] = await file.getMetadata();
    const existing = (meta.metadata && typeof meta.metadata === "object")
      ? { ...meta.metadata }
      : {};
    existing.ownerProjectId = ownerProjectId;
    await file.setMetadata({ metadata: existing });
    res.json({ success: true, ownerProjectId });
  } catch (err) {
    console.error("Shared ack error:", err.message);
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
// Property-level shared assets — Phase A of the cross-project sharing
// design. Files at <propertyId>/_shared/[<sub>] are merged into each
// per-project listing for projects on the same property, tagged with
// shared:true so the frontend can render a "Shared" badge. A project's
// project.json may carry a hiddenSharedAssets[] of full GCS paths to
// hide specific shared items on this project only — viewers and the
// non-with-hidden endpoints filter those out; the editor "with-hidden"
// endpoints surface them with hiddenInProject:true so the editor UI
// (Phase B) can offer a per-project unhide toggle.
//
// Returns the property's shared prefix, optionally with a sub-path
// (e.g. "_plans/", "_documents/"). Null when the project is on the
// legacy <oldName> layout — there's no property to share from.
function sharedPrefixFor(req, sub = "") {
  const base = req.projectPaths && req.projectPaths.base;
  if (!base || !base.includes("/")) return null;
  const propertyId = base.split("/")[0];
  if (!propertyId || propertyId === "_thumbs" || propertyId === "_platform") return null;
  return `${propertyId}/_shared/${sub}`;
}

// Per-project hide list for shared assets — already cached on
// req.projectResolved by the resolver middleware (project.json is read
// once per request and spread onto the resolved object), so this is
// synchronous and free.
function getProjectHiddenSharedSet(req) {
  const arr = req.projectResolved && req.projectResolved.hiddenSharedAssets;
  return new Set(Array.isArray(arr) ? arr.filter((s) => typeof s === "string") : []);
}

// Fetch the property's shared GCS files for a given bucket + sub-prefix.
// Mirrors getFiles' [files] shape; returns { files: [], prefix: null }
// for legacy-layout projects. Errors are logged and swallowed so a
// shared-listing failure can't take down a project listing.
async function fetchSharedFiles(req, bucketRef, sub = "") {
  const prefix = sharedPrefixFor(req, sub);
  if (!prefix) return { files: [], prefix: null };
  try {
    const [files] = await bucketRef.getFiles({ prefix });
    return { files, prefix };
  } catch (err) {
    console.warn(`[shared] list failed for ${prefix}:`, err.message);
    return { files: [], prefix };
  }
}

// Find the canonical slug for a project given its propertyId/projectId
// pair, by scanning the slug-index. Used by shared-asset delete
// authorization to resolve the owner project from the GCS metadata stamp
// (ownerProjectId) without doing a full reverse search through users.json.
async function canonicalSlugFor(propertyId, projectId) {
  if (!propertyId || !projectId) return null;
  try {
    const idx = await projectResolver.getIndex();
    if (!idx || !idx.canonical) return null;
    for (const [slug, ref] of Object.entries(idx.canonical)) {
      if (ref && ref.layout === "new" && ref.propertyId === propertyId && ref.projectId === projectId) {
        return slug;
      }
    }
    return null;
  } catch (err) {
    console.warn("[canonicalSlugFor] lookup failed:", err.message);
    return null;
  }
}

// Delete authorization for shared paths. requireProjectRole has already
// confirmed the requester is editor on the project they're acting from.
// For destructive delete on a shared asset, we additionally require
// editor on the *owner* project — otherwise an editor on project A
// could nuke shared content for everyone on the property. Admins bypass.
// Returns { allowed, error, isShared }; callers use isShared for
// helpful error messaging only.
async function authorizeSharedDelete(req, file) {
  const filePath = file.name;
  const m = filePath.match(/^([^/]+)\/_shared\//);
  if (!m) return { allowed: true, error: null, isShared: false };
  if (req.user && req.user.isAdmin) return { allowed: true, error: null, isShared: true };
  let metadata;
  try {
    [metadata] = await file.getMetadata();
  } catch (err) {
    return { allowed: false, error: "could not read shared asset metadata", isShared: true };
  }
  const ownerProjectId = metadata && metadata.metadata && metadata.metadata.ownerProjectId;
  if (!ownerProjectId) {
    return { allowed: false, error: "shared asset has no owner project; only admins can delete", isShared: true };
  }
  const propertyId = m[1];
  const ownerCanonical = await canonicalSlugFor(propertyId, ownerProjectId);
  if (!ownerCanonical) {
    return { allowed: false, error: "owner project not found", isShared: true };
  }
  const ok = await hasAliasAwareAccess(req.user && req.user.email, ownerCanonical, "editor");
  if (!ok) return { allowed: false, error: "only editors of the owner project can delete this shared asset", isShared: true };
  return { allowed: true, error: null, isShared: true };
}

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
    const [[files], shared, floorPlans] = await Promise.all([
      imageBucket.getFiles(options),
      fetchSharedFiles(req, imageBucket),
      getProjectFloorPlanPaths(project),
    ]);
    const hiddenShared = getProjectHiddenSharedSet(req);
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
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/"))
      .filter((f) => !f.name.includes("/_documents/"))
      .filter((f) => !f.name.includes("/_videos/"))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
      .filter((f) => !hiddenShared.has(f.name))
      .map((f) => ({
        name: f.name,
        displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
        shared: true,
      }));
    res.json(list.concat(sharedList));
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
    const [[files], shared, floorPlans] = await Promise.all([
      imageBucket.getFiles(options),
      fetchSharedFiles(req, imageBucket),
      getProjectFloorPlanPaths(project),
    ]);
    const hiddenShared = getProjectHiddenSharedSet(req);
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
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/"))
      .filter((f) => !f.name.includes("/_documents/"))
      .filter((f) => !f.name.includes("/_videos/"))
      .map((f) => ({
        name: f.name,
        displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
        hiddenInProject: hiddenShared.has(f.name),
        ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
        shared: true,
      }));
    res.json(list.concat(sharedList));
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
    const [[files], shared, floorPlans] = await Promise.all([
      imageBucket.getFiles({ prefix }),
      fetchSharedFiles(req, imageBucket),
      getProjectFloorPlanPaths(project),
    ]);
    const hiddenShared = getProjectHiddenSharedSet(req);
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
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/"))
      .filter((f) => !f.name.includes("/_documents/"))
      .filter((f) => !f.name.includes("/_videos/"))
      .map((f) => ({
        name: f.name,
        displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
        hiddenInProject: hiddenShared.has(f.name),
        ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
        shared: true,
      }));
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List 2D editor files error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: toggle whether a property-shared asset shows on this
// project. Per-project hide is stored in project.json under
// hiddenSharedAssets[]; the listing endpoints filter shared items
// against that set for the public view and surface hiddenInProject
// for the editor view. This endpoint just maintains the array — it
// never touches the shared asset's GCS metadata, so other projects on
// the property are unaffected.
app.post("/api/project/shared-visibility", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, hidden } = req.body || {};
  if (!filePath || typeof filePath !== "string") {
    return res.status(400).json({ error: "file is required" });
  }
  // Sanity-check: the path must be under this property's _shared/. The
  // resolved project's paths.base gives us the propertyId.
  const base = req.projectPaths && req.projectPaths.base;
  if (!base || !base.includes("/")) {
    return res.status(400).json({ error: "project is not in property layout" });
  }
  const propertyId = base.split("/")[0];
  if (!filePath.startsWith(`${propertyId}/_shared/`)) {
    return res.status(400).json({ error: "file is not a shared asset on this property" });
  }
  try {
    const projectFile = bucket.file(`${base}/project.json`);
    const [exists] = await projectFile.exists();
    if (!exists) return res.status(404).json({ error: "project.json not found" });
    const [content] = await projectFile.download();
    const meta = JSON.parse(content.toString());
    const current = Array.isArray(meta.hiddenSharedAssets)
      ? meta.hiddenSharedAssets.filter((s) => typeof s === "string")
      : [];
    const set = new Set(current);
    if (hidden) set.add(filePath);
    else set.delete(filePath);
    const next = Array.from(set);
    if (next.length === 0) delete meta.hiddenSharedAssets;
    else meta.hiddenSharedAssets = next;
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, hidden: !!hidden, hiddenSharedAssets: next });
  } catch (err) {
    console.error("Shared visibility toggle error:", err.message);
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

// Upload 2D image. scope=shared writes to <propertyId>/_shared/<file>
// instead of <project>/<file> and stamps the uploading project's id on
// custom metadata as ownerProjectId — the listing layer surfaces that
// so delete authorization can require editor role on the owner project.
app.post("/api/2d/upload", upload.single("file"), requireProjectRole("editor"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file provided" });
  }

  const tmpPath = req.file.path;
  const baseName = req.body.destName || req.file.originalname;
  const project = req.body.project;
  const scope = req.body.scope === "shared" ? "shared" : "project";

  let destName;
  let customMeta = null;
  if (scope === "shared") {
    const base = req.projectPaths && req.projectPaths.base;
    if (!base || !base.includes("/")) {
      fs.unlink(tmpPath, () => {});
      return res.status(400).json({ error: "shared scope requires a property-layout project" });
    }
    const parts = base.split("/");
    const propertyId = parts[0];
    const ownerProjectId = parts[1];
    destName = `${propertyId}/_shared/${baseName}`;
    customMeta = { ownerProjectId };
  } else {
    destName = project ? `${project}/${baseName}` : baseName;
  }
  const fileSize = req.file.size;

  try {
    const gcsFile = imageBucket.file(destName);
    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: true,
      metadata: {
        contentType: req.file.mimetype || "application/octet-stream",
        ...(customMeta ? { metadata: customMeta } : {}),
      },
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

    console.log(`Uploaded 2D: gs://${IMAGE_BUCKET_NAME}/${destName} (${(fileSize / 1024 / 1024).toFixed(2)} MB)${scope === "shared" ? " [shared]" : ""}`);
    res.json({ success: true, fileName: baseName, destName, fileSize, gcsPath: `gs://${IMAGE_BUCKET_NAME}/${destName}`, project: project || null, scope });
  } catch (err) {
    console.error("2D upload failed:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    fs.unlink(tmpPath, () => {});
  }
});

// Replace a 2D image in place — editor only. Overwrites the GCS object
// at the same path with the new bytes, preserves any custom metadata
// (e.g. the hidden flag), and refreshes the cached _thumbs/ thumbnail
// so the gallery picks up the change. The file path/name is unchanged
// so existing references (cardThumbnails, mappings, etc.) keep working.
app.post("/api/2d/replace", upload.single("file"), requireProjectRole("editor"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file provided" });
  const tmpPath = req.file.path;
  const filePath = req.body && req.body.destName;
  try {
    if (!filePath) return res.status(400).json({ error: "destName (existing image path) required" });
    if (filePath.startsWith("_thumbs/") || filePath.includes("/_thumbs/")) {
      return res.status(400).json({ error: "Cannot replace a thumbnail directly" });
    }
    const gcsFile = imageBucket.file(filePath);
    const [exists] = await gcsFile.exists();
    if (!exists) return res.status(404).json({ error: "Image not found" });

    // Read existing custom metadata (e.g. hidden flag) so we can re-apply
    // it after the overwrite — createWriteStream replaces the whole
    // metadata block, not just the bytes.
    const [oldMetadata] = await gcsFile.getMetadata();
    const preservedCustom = (oldMetadata.metadata && typeof oldMetadata.metadata === "object")
      ? { ...oldMetadata.metadata }
      : null;

    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: true,
      metadata: {
        contentType: req.file.mimetype || oldMetadata.contentType || "application/octet-stream",
        ...(preservedCustom ? { metadata: preservedCustom } : {}),
      },
    });
    await new Promise((resolve, reject) => {
      readStream.pipe(writeStream).on("error", reject).on("finish", resolve);
    });

    console.log(`Replaced 2D: gs://${IMAGE_BUCKET_NAME}/${filePath} (${(req.file.size / 1024 / 1024).toFixed(2)} MB)`);

    // Refresh the cached thumbnail. Best-effort: if anything fails the
    // proxy will fall back to the original on next read.
    try { await deleteThumbnail(filePath); } catch (err) {
      console.warn(`[2d/replace] thumb delete failed for ${filePath}:`, err.message);
    }
    try { await generateThumbnailFromGCS(filePath); } catch (err) {
      console.warn(`[2d/replace] thumb regen failed for ${filePath}:`, err.message);
    }

    res.json({ success: true, file: filePath, fileSize: req.file.size });
  } catch (err) {
    console.error("2D replace error:", err.message);
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
    const auth = await authorizeSharedDelete(req, file);
    if (!auth.allowed) return res.status(403).json({ error: auth.error });
    await file.delete();
    console.log(`Deleted 2D: gs://${IMAGE_BUCKET_NAME}/${filePath}${auth.isShared ? " [shared]" : ""}`);
    res.json({ success: true });
  } catch (err) {
    console.error("2D delete error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// List sibling projects on the same property as the current one. Used by
// the "Make shared" modal so the editor can pick which projects on the
// property the asset should be visible on. Editor-or-admin gated since
// it leaks the property's project roster.
app.get("/api/property-projects", requireProjectRole("editor"), async (req, res) => {
  const base = req.projectPaths && req.projectPaths.base;
  if (!base || !base.includes("/")) return res.json([]);
  const propertyId = base.split("/")[0];
  const currentProjectId = base.split("/")[1];
  try {
    const idx = await projectResolver.getIndex();
    if (!idx || !idx.canonical) return res.json([]);
    const matches = [];
    for (const [slug, ref] of Object.entries(idx.canonical)) {
      if (ref && ref.layout === "new" && ref.propertyId === propertyId) {
        matches.push({ canonicalSlug: slug, projectId: ref.projectId });
      }
    }
    // Enrich with the human-readable project name from each project.json.
    const enriched = await Promise.all(matches.map(async (m) => {
      try {
        const f = bucket.file(`${propertyId}/${m.projectId}/project.json`);
        const [exists] = await f.exists();
        if (!exists) return { ...m, name: m.canonicalSlug, isCurrent: m.projectId === currentProjectId };
        const [content] = await f.download();
        const data = JSON.parse(content.toString());
        return { ...m, name: data.name || m.canonicalSlug, isCurrent: m.projectId === currentProjectId };
      } catch {
        return { ...m, name: m.canonicalSlug, isCurrent: m.projectId === currentProjectId };
      }
    }));
    enriched.sort((a, b) => {
      // Current project first so the modal can highlight context, then alpha.
      if (a.isCurrent !== b.isCurrent) return a.isCurrent ? -1 : 1;
      return (a.name || "").localeCompare(b.name || "");
    });
    res.json(enriched);
  } catch (err) {
    console.error("List property projects error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Convert an owned 2D image to a property-shared asset. Moves the bytes
// from <propertyId>/<projectId>/<file> to <propertyId>/_shared/<file>,
// stamps ownerProjectId on the object's custom metadata so the listing
// layer can attribute it back, drops the old path from this project's
// imageOrder, and adds the new shared path to hiddenSharedAssets[] on
// any sibling project the editor explicitly excluded. Body:
//   { file: "<propertyId>/<projectId>/<basename>", projects: ["__all__"] | [<canonicalSlug>, ...] }
// "__all__" (or an empty allow-list) makes the asset visible on every
// project on the property.
app.post("/api/2d/share", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, projects } = req.body || {};
  if (!filePath || typeof filePath !== "string") return res.status(400).json({ error: "file is required" });
  const base = req.projectPaths && req.projectPaths.base;
  if (!base || !base.includes("/")) return res.status(400).json({ error: "project is not in property layout" });
  if (!filePath.startsWith(base + "/")) return res.status(400).json({ error: "file is not in this project" });
  if (filePath.includes("/_plans/") || filePath.includes("/_documents/") || filePath.includes("/_videos/")) {
    return res.status(400).json({ error: "this file type cannot be shared from /api/2d/share" });
  }
  if (filePath.startsWith(base.split("/")[0] + "/_shared/")) {
    return res.status(400).json({ error: "file is already shared" });
  }
  const propertyId = base.split("/")[0];
  const currentProjectId = base.split("/")[1];
  const baseName = filePath.substring(base.length + 1);
  const newPath = `${propertyId}/_shared/${baseName}`;

  try {
    const src = imageBucket.file(filePath);
    const [srcExists] = await src.exists();
    if (!srcExists) return res.status(404).json({ error: "Source file not found" });
    // Bail if a shared file with the same name already exists — silently
    // overwriting would clobber another project's shared asset.
    const dest = imageBucket.file(newPath);
    const [destExists] = await dest.exists();
    if (destExists) return res.status(409).json({ error: "A shared asset with this name already exists on the property" });

    // Read existing custom metadata before the move so we can re-apply
    // it (with ownerProjectId added) at the destination — move() preserves
    // metadata, but explicitly setting it here protects against any
    // future change in copy semantics.
    const [oldMeta] = await src.getMetadata();
    const customMeta = (oldMeta.metadata && typeof oldMeta.metadata === "object")
      ? { ...oldMeta.metadata }
      : {};
    customMeta.ownerProjectId = currentProjectId;

    await src.move(newPath);
    await imageBucket.file(newPath).setMetadata({ metadata: customMeta });

    // Drop the old path from this project's imageOrder. The new shared
    // path will be appended to the listing automatically (Phase A merge).
    try {
      const projectFile = bucket.file(`${base}/project.json`);
      const [exists] = await projectFile.exists();
      if (exists) {
        const [content] = await projectFile.download();
        const meta = JSON.parse(content.toString());
        if (Array.isArray(meta.imageOrder) && meta.imageOrder.includes(filePath)) {
          meta.imageOrder = meta.imageOrder.filter((p) => p !== filePath);
          await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
        }
      }
    } catch (err) {
      console.warn("[2d/share] imageOrder cleanup failed:", err.message);
    }

    // For excluded sibling projects, append the new shared path to
    // hiddenSharedAssets[] so the asset doesn't surface on their gallery.
    const allowList = Array.isArray(projects) ? projects : ["__all__"];
    const shareWithAll = allowList.length === 0 || allowList.includes("__all__");

    let excludedCount = 0;
    if (!shareWithAll) {
      const idx = await projectResolver.getIndex();
      if (idx && idx.canonical) {
        for (const [slug, ref] of Object.entries(idx.canonical)) {
          if (!ref || ref.layout !== "new" || ref.propertyId !== propertyId) continue;
          if (ref.projectId === currentProjectId) continue;
          if (allowList.includes(slug)) continue;
          try {
            const sibFile = bucket.file(`${propertyId}/${ref.projectId}/project.json`);
            const [sibExists] = await sibFile.exists();
            if (!sibExists) continue;
            const [c] = await sibFile.download();
            const sibMeta = JSON.parse(c.toString());
            const arr = Array.isArray(sibMeta.hiddenSharedAssets)
              ? sibMeta.hiddenSharedAssets.filter((s) => typeof s === "string")
              : [];
            if (!arr.includes(newPath)) {
              arr.push(newPath);
              sibMeta.hiddenSharedAssets = arr;
              await sibFile.save(JSON.stringify(sibMeta, null, 2), { contentType: "application/json" });
              excludedCount++;
            }
          } catch (err) {
            console.warn(`[2d/share] sibling hide failed for ${slug}:`, err.message);
          }
        }
      }
    }

    console.log(`Shared 2D: ${filePath} -> ${newPath} (owner=${currentProjectId}, excluded=${excludedCount})`);
    res.json({ success: true, newPath, ownerProjectId: currentProjectId, sharedWithAll: shareWithAll, excludedCount });
  } catch (err) {
    console.error("2D share error:", err.message);
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

// List plans in a project — PUBLIC. Hidden plans are filtered out so
// anonymous viewers never see them; editors get the full list (including
// hidden ones, with the flag) via /api/plan/files-with-hidden below.
app.get("/api/plan/files", async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_plans/`;
    const [[files], shared] = await Promise.all([
      imageBucket.getFiles({ prefix }),
      fetchSharedFiles(req, imageBucket, "_plans/"),
    ]);
    const allNames = new Set(files.map((f) => f.name));
    const sharedAllNames = new Set(shared.files.map((f) => f.name));
    const hiddenShared = getProjectHiddenSharedSet(req);
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
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
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
      .filter((f) => !hiddenShared.has(f.name))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        return {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: sharedAllNames.has(thumbName) ? thumbName : null,
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
      });
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List plans error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: list plans including hidden ones plus the flag — used by
// project-plans.html so editors see what they've hidden and can unhide
// without bouncing somewhere else. Mirrors /api/2d/files-with-hidden.
app.get("/api/plan/files-with-hidden", requireProjectRole("editor"), async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_plans/`;
    const [[files], shared] = await Promise.all([
      imageBucket.getFiles({ prefix }),
      fetchSharedFiles(req, imageBucket, "_plans/"),
    ]);
    const allNames = new Set(files.map((f) => f.name));
    const sharedAllNames = new Set(shared.files.map((f) => f.name));
    const hiddenShared = getProjectHiddenSharedSet(req);
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
          hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
        };
      });
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        return {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: sharedAllNames.has(thumbName) ? thumbName : null,
          hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
          hiddenInProject: hiddenShared.has(f.name),
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
      });
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List plans (with hidden) error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: toggle the hidden flag on a plan. Same shape as /api/2d/visibility.
app.post("/api/plan/visibility", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, hidden } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!filePath.includes("/_plans/")) return res.status(400).json({ error: "not a plan path" });
  if (filePath.endsWith(".thumb.jpg")) return res.status(400).json({ error: "cannot hide a thumbnail directly" });
  try {
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await file.setMetadata({ metadata: { hidden: hidden ? "true" : null } });
    res.json({ success: true, hidden: !!hidden });
  } catch (err) {
    console.error("Plan visibility toggle error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: replace a plan in place. Overwrites the GCS object with new
// bytes, preserves the hidden flag, and deletes the cached <file>.thumb.jpg
// so the next thumbnail upload (or fallback to the file itself) reflects
// the new content. Same end-to-end pattern as /api/2d/replace.
app.post("/api/plan/replace", upload.single("file"), requireProjectRole("editor"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file provided" });
  const tmpPath = req.file.path;
  const filePath = req.body && req.body.destName;
  try {
    if (!filePath) return res.status(400).json({ error: "destName (existing plan path) required" });
    if (!filePath.includes("/_plans/")) return res.status(400).json({ error: "not a plan path" });
    if (filePath.endsWith(".thumb.jpg")) return res.status(400).json({ error: "cannot replace a thumbnail directly" });

    const gcsFile = imageBucket.file(filePath);
    const [exists] = await gcsFile.exists();
    if (!exists) return res.status(404).json({ error: "Plan not found" });

    const [oldMetadata] = await gcsFile.getMetadata();
    const preservedCustom = (oldMetadata.metadata && typeof oldMetadata.metadata === "object")
      ? { ...oldMetadata.metadata }
      : null;

    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: true,
      metadata: {
        contentType: req.file.mimetype || oldMetadata.contentType || "application/octet-stream",
        ...(preservedCustom ? { metadata: preservedCustom } : {}),
      },
    });
    await new Promise((resolve, reject) => {
      readStream.pipe(writeStream).on("error", reject).on("finish", resolve);
    });

    // Drop any stale cached thumbnail — the editor re-uploads via
    // /api/upload-url (bucket=plan, fileName=<base>.thumb.jpg) when they
    // want a fresh preview. Best-effort: a missing thumb just falls back
    // to whatever the page renders for "no thumbnail".
    try {
      const thumb = imageBucket.file(filePath + ".thumb.jpg");
      const [thumbExists] = await thumb.exists();
      if (thumbExists) await thumb.delete();
    } catch (err) {
      console.warn(`[plan/replace] thumb cleanup failed for ${filePath}:`, err.message);
    }

    res.json({ success: true, file: filePath, fileSize: req.file.size });
  } catch (err) {
    console.error("Plan replace error:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    fs.unlink(tmpPath, () => {});
  }
});

// Editor: persist plan ordering on project.json (planOrder). Mirrors
// /api/2d/order — empty array clears the override and the page falls
// back to chronological (newest first) on render.
app.post("/api/plan/order", requireProjectRole("editor"), async (req, res) => {
  const { project, order } = req.body || {};
  if (!project) return res.status(400).json({ error: "project is required" });
  if (order != null && !Array.isArray(order)) {
    return res.status(400).json({ error: "order must be an array of plan paths or null" });
  }
  const planPrefix = `${project}/_plans/`;
  const cleaned = Array.isArray(order)
    ? order
        .filter((s) => typeof s === "string" && s.startsWith(planPrefix) && !s.endsWith(".thumb.jpg"))
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
    if (cleaned.length === 0) delete meta.planOrder;
    else meta.planOrder = cleaned;
    meta.updatedAt = new Date().toISOString();
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, planOrder: meta.planOrder || [] });
  } catch (err) {
    console.error("plan order error:", err.message);
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
    const auth = await authorizeSharedDelete(req, f);
    if (!auth.allowed) return res.status(403).json({ error: auth.error });
    await f.delete();
    console.log(`Deleted plan: gs://${IMAGE_BUCKET_NAME}/${filePath}${auth.isShared ? " [shared]" : ""}`);
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

// Convert an owned plan to a property-shared asset. Mirrors /api/2d/share
// but for the imageBucket _plans/ subtree. Moves the bytes, the sibling
// thumbnail (<file>.thumb.jpg) if present, stamps ownerProjectId, drops
// the old path from this project's planOrder, and adds the new shared
// path to hiddenSharedAssets[] on excluded sibling projects.
app.post("/api/plan/share", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, projects } = req.body || {};
  if (!filePath || typeof filePath !== "string") return res.status(400).json({ error: "file is required" });
  const base = req.projectPaths && req.projectPaths.base;
  if (!base || !base.includes("/")) return res.status(400).json({ error: "project is not in property layout" });
  if (!filePath.startsWith(`${base}/_plans/`)) return res.status(400).json({ error: "file is not a plan in this project" });
  if (filePath.endsWith(".thumb.jpg")) return res.status(400).json({ error: "thumbnails move alongside their plan" });

  const propertyId = base.split("/")[0];
  const currentProjectId = base.split("/")[1];
  const baseName = filePath.substring((base + "/_plans/").length);
  const newPath = `${propertyId}/_shared/_plans/${baseName}`;
  const oldThumb = filePath + ".thumb.jpg";
  const newThumb = newPath + ".thumb.jpg";

  try {
    const src = imageBucket.file(filePath);
    const [srcExists] = await src.exists();
    if (!srcExists) return res.status(404).json({ error: "Source plan not found" });
    const dest = imageBucket.file(newPath);
    const [destExists] = await dest.exists();
    if (destExists) return res.status(409).json({ error: "A shared plan with this name already exists on the property" });

    const [oldMeta] = await src.getMetadata();
    const customMeta = (oldMeta.metadata && typeof oldMeta.metadata === "object")
      ? { ...oldMeta.metadata }
      : {};
    customMeta.ownerProjectId = currentProjectId;

    await src.move(newPath);
    await imageBucket.file(newPath).setMetadata({ metadata: customMeta });

    // Move the sidecar thumbnail too if present. Best-effort — listing
    // falls back to no-thumbnail if it goes missing.
    try {
      const thumbSrc = imageBucket.file(oldThumb);
      const [thumbExists] = await thumbSrc.exists();
      if (thumbExists) {
        await thumbSrc.move(newThumb);
      }
    } catch (err) {
      console.warn("[plan/share] thumbnail move failed:", err.message);
    }

    // Drop the old path from this project's planOrder. Phase A's listing
    // append will surface the new shared path automatically.
    try {
      const projectFile = bucket.file(`${base}/project.json`);
      const [exists] = await projectFile.exists();
      if (exists) {
        const [content] = await projectFile.download();
        const meta = JSON.parse(content.toString());
        if (Array.isArray(meta.planOrder) && meta.planOrder.includes(filePath)) {
          meta.planOrder = meta.planOrder.filter((p) => p !== filePath);
          await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
        }
      }
    } catch (err) {
      console.warn("[plan/share] planOrder cleanup failed:", err.message);
    }

    const allowList = Array.isArray(projects) ? projects : ["__all__"];
    const shareWithAll = allowList.length === 0 || allowList.includes("__all__");

    let excludedCount = 0;
    if (!shareWithAll) {
      const idx = await projectResolver.getIndex();
      if (idx && idx.canonical) {
        for (const [slug, ref] of Object.entries(idx.canonical)) {
          if (!ref || ref.layout !== "new" || ref.propertyId !== propertyId) continue;
          if (ref.projectId === currentProjectId) continue;
          if (allowList.includes(slug)) continue;
          try {
            const sibFile = bucket.file(`${propertyId}/${ref.projectId}/project.json`);
            const [sibExists] = await sibFile.exists();
            if (!sibExists) continue;
            const [c] = await sibFile.download();
            const sibMeta = JSON.parse(c.toString());
            const arr = Array.isArray(sibMeta.hiddenSharedAssets)
              ? sibMeta.hiddenSharedAssets.filter((s) => typeof s === "string")
              : [];
            if (!arr.includes(newPath)) {
              arr.push(newPath);
              sibMeta.hiddenSharedAssets = arr;
              await sibFile.save(JSON.stringify(sibMeta, null, 2), { contentType: "application/json" });
              excludedCount++;
            }
          } catch (err) {
            console.warn(`[plan/share] sibling hide failed for ${slug}:`, err.message);
          }
        }
      }
    }

    console.log(`Shared plan: ${filePath} -> ${newPath} (owner=${currentProjectId}, excluded=${excludedCount})`);
    res.json({ success: true, newPath, ownerProjectId: currentProjectId, sharedWithAll: shareWithAll, excludedCount });
  } catch (err) {
    console.error("Plan share error:", err.message);
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
    const [[files], shared] = await Promise.all([
      imageBucket.getFiles({ prefix }),
      fetchSharedFiles(req, imageBucket, "_documents/"),
    ]);
    const hiddenShared = getProjectHiddenSharedSet(req);
    const docs = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"));
    const sharedDocs = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"));
    const [list, sharedList] = await Promise.all([
      Promise.all(docs.map(async (f) => {
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
      })),
      Promise.all(sharedDocs.map(async (f) => {
        const meta = await readDocumentMeta(f.name);
        return {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          uploadedBy: meta.uploadedBy || null,
          uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
          visibility: meta.visibility === "public" ? "public" : "private",
          hiddenInProject: hiddenShared.has(f.name),
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
      })),
    ]);
    list.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    sharedList.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    res.json(list.concat(sharedList));
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
    const [[files], shared] = await Promise.all([
      imageBucket.getFiles({ prefix }),
      fetchSharedFiles(req, imageBucket, "_documents/"),
    ]);
    const hiddenShared = getProjectHiddenSharedSet(req);
    const docs = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"));
    const sharedDocs = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"))
      .filter((f) => !hiddenShared.has(f.name));
    // canSeeAll covers admin and project editors — both need to see
    // private docs (so editors can manage what they uploaded) and the
    // uploadedBy / visibility metadata (project-documents.html shows
    // the uploader on hover and lets editors flip visibility).
    const isAdminCaller = !!req.user?.isAdmin;
    const canSeeAll = isAdminCaller || (
      req.projectCanonical
        ? await hasAliasAwareAccess(req.user?.email, req.projectCanonical, "editor")
        : false
    );
    const [list, sharedList] = await Promise.all([
      Promise.all(docs.map(async (f) => {
        const meta = await readDocumentMeta(f.name);
        const visibility = meta.visibility === "public" ? "public" : "private";
        // Private docs hidden from anonymous and viewer-role callers —
        // they shouldn't even know they exist.
        if (!canSeeAll && visibility !== "public") return null;
        const entry = {
          name: f.name,
          displayName: f.name.replace(prefix, ""),
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
        };
        if (canSeeAll) {
          entry.uploadedBy = meta.uploadedBy || null;
          entry.visibility = visibility;
        }
        return entry;
      })).then((arr) => arr.filter(Boolean)),
      Promise.all(sharedDocs.map(async (f) => {
        const meta = await readDocumentMeta(f.name);
        const visibility = meta.visibility === "public" ? "public" : "private";
        if (!canSeeAll && visibility !== "public") return null;
        const entry = {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
        if (canSeeAll) {
          entry.uploadedBy = meta.uploadedBy || null;
          entry.visibility = visibility;
        }
        return entry;
      })).then((arr) => arr.filter(Boolean)),
    ]);
    list.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    sharedList.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List documents error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Acknowledge a completed direct-to-GCS upload — admin only. The signed
// upload URL itself doesn't carry the requesting admin's identity past the
// PUT, so the client calls this immediately after a successful PUT and the
// server records the uploader on a sidecar metadata file.
app.post("/api/document/uploaded", requireProjectRole("editor"), async (req, res) => {
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

// Signed inline-view URL for a document — for non-anonymous viewers
// (any signed-in role) so the browser can render PDFs in an <iframe>
// without forcing a download. Kept OFF the public URL-map matcher so
// IAP enforces sign-in before the request reaches us; if somehow an
// anonymous request slips through (req.user null), we 401 explicitly.
// Visibility is still respected: non-admin viewers can only view
// documents flagged visibility=public.
app.get("/api/document/view-url", async (req, res) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Sign in to view documents" });
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!isDocumentPath(filePath)) return res.status(400).json({ error: "not a document path" });
    if (!req.user.isAdmin) {
      const meta = await readDocumentMeta(filePath);
      if (meta.visibility !== "public") {
        return res.status(403).json({ error: "This document is private" });
      }
    }
    const [url] = await imageBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
    });
    res.json({ viewUrl: url });
  } catch (err) {
    console.error("Document view-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Rename a document — admin only. GCS has no rename, so copy + delete on
// both the file and its sidecar metadata. The new name must stay inside
// the same project's _documents/ folder.
app.post("/api/document/rename", requireProjectRole("editor"), async (req, res) => {
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
app.post("/api/document/visibility", requireProjectRole("editor"), async (req, res) => {
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

// Delete a document — editors and admins. Shared documents additionally
// require editor on the owner project (or admin); see authorizeSharedDelete.
// Also removes the sidecar metadata.
app.post("/api/document/delete", requireProjectRole("editor"), async (req, res) => {
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
    const auth = await authorizeSharedDelete(req, f);
    if (!auth.allowed) return res.status(403).json({ error: auth.error });
    await f.delete();
    console.log(`Deleted document: gs://${IMAGE_BUCKET_NAME}/${filePath}${auth.isShared ? " [shared]" : ""}`);
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
// Public viewers can browse + play + download non-hidden videos via the
// /api/video/* routes (filtered listing, hidden-aware stream/download).
// Admins use the /api/admin/video/* mirrors to see hidden videos and
// uploader info — those stay off the public URL-map matcher so they
// route through IAP and preserve admin identity. Mutations (visibility,
// order, rename, delete, thumbnail upload/clear) are admin-only.
// Per-file metadata (uploadedBy, uploadedAt, thumbnail path) lives on a
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

// Admin: full video listing including hidden videos and uploader info.
// Mirror of /api/video/files but kept OFF the public URL-map matcher
// so it routes through the IAP backend — the public listing endpoint
// can't see admin identity (it sits behind the no-IAP backend serving
// /videos/<project>) and would silently filter hidden videos out.
app.get("/api/admin/video/files", requireAdmin, async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_videos/`;
    const [[files], shared] = await Promise.all([
      imageBucket.getFiles({ prefix }),
      fetchSharedFiles(req, imageBucket, "_videos/"),
    ]);
    const hiddenShared = getProjectHiddenSharedSet(req);
    const videos = files
      // Skip the _thumbs/ subtree — those are sidecar images for the
      // videos themselves, not videos we want to list.
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"))
      .filter((f) => !f.name.startsWith(`${prefix}_thumbs/`));
    const sharedVideos = shared.prefix ? shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"))
      .filter((f) => !f.name.startsWith(`${shared.prefix}_thumbs/`))
      : [];
    const buildEntry = async (f, displayPrefix, opts = {}) => {
      const meta = await readVideoMeta(f.name);
      const entry = {
        name: f.name,
        displayName: displayPrefix ? f.name.replace(displayPrefix, "") : f.name,
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        uploadedBy: meta.uploadedBy || null,
        uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
        hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
      };
      if (opts.shared) {
        entry.shared = true;
        entry.hiddenInProject = hiddenShared.has(f.name);
        entry.ownerProjectId = (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null;
      }
      // Thumbnail is stored as a sibling image under _videos/_thumbs/ and
      // tracked by GCS path on the meta sidecar (so renames don't break it).
      // Sign a 15-min URL inline — admins consume the listing immediately.
      if (meta.thumbnail) {
        try {
          const [signed] = await imageBucket.file(meta.thumbnail).getSignedUrl({
            version: "v4",
            action: "read",
            expires: Date.now() + 15 * 60 * 1000,
          });
          entry.thumbnail = meta.thumbnail;
          entry.thumbnailUrl = signed;
        } catch (err) {
          console.warn(`[video] thumbnail signed-url failed for ${meta.thumbnail}:`, err.message);
        }
      }
      return entry;
    };
    const [list, sharedList] = await Promise.all([
      Promise.all(videos.map((f) => buildEntry(f, prefix))),
      Promise.all(sharedVideos.map((f) => buildEntry(f, shared.prefix, { shared: true }))),
    ]);
    list.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    sharedList.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("Admin list videos error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// List videos in a project. Public route — non-admin viewers see only
// non-hidden videos with no uploader info. Hidden videos are skipped
// entirely so viewers don't even know they exist. Admins use
// /api/admin/video/files (above) to see the full set.
app.get("/api/video/files", async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = `${project}/_videos/`;
    const [[files], shared] = await Promise.all([
      imageBucket.getFiles({ prefix }),
      fetchSharedFiles(req, imageBucket, "_videos/"),
    ]);
    const hiddenShared = getProjectHiddenSharedSet(req);
    const videos = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"))
      .filter((f) => !f.name.startsWith(`${prefix}_thumbs/`))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"));
    const sharedVideos = shared.prefix ? shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".meta.json"))
      .filter((f) => !f.name.startsWith(`${shared.prefix}_thumbs/`))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
      .filter((f) => !hiddenShared.has(f.name))
      : [];
    const buildEntry = async (f, displayPrefix, opts = {}) => {
      const meta = await readVideoMeta(f.name);
      const entry = {
        name: f.name,
        displayName: displayPrefix ? f.name.replace(displayPrefix, "") : f.name,
        size: Number(f.metadata.size),
        updated: f.metadata.updated,
        contentType: f.metadata.contentType,
        uploadedAt: meta.uploadedAt || f.metadata.timeCreated || f.metadata.updated,
      };
      if (opts.shared) {
        entry.shared = true;
        entry.ownerProjectId = (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null;
      }
      if (meta.thumbnail) {
        try {
          const [signed] = await imageBucket.file(meta.thumbnail).getSignedUrl({
            version: "v4",
            action: "read",
            expires: Date.now() + 15 * 60 * 1000,
          });
          entry.thumbnailUrl = signed;
        } catch (err) {
          console.warn(`[video] thumbnail signed-url failed for ${meta.thumbnail}:`, err.message);
        }
      }
      return entry;
    };
    const [list, sharedList] = await Promise.all([
      Promise.all(videos.map((f) => buildEntry(f, prefix))),
      Promise.all(sharedVideos.map((f) => buildEntry(f, shared.prefix, { shared: true }))),
    ]);
    list.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    sharedList.sort((a, b) => (b.uploadedAt || "").localeCompare(a.uploadedAt || ""));
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List videos error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: toggle the hidden flag on a video. Stored on GCS custom
// metadata so the listing's `hidden` field above reflects it directly.
app.post("/api/video/visibility", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, hidden } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
  if (filePath.endsWith(".meta.json")) return res.status(400).json({ error: "cannot hide a meta sidecar directly" });
  try {
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await file.setMetadata({ metadata: { hidden: hidden ? "true" : null } });
    res.json({ success: true, hidden: !!hidden });
  } catch (err) {
    console.error("Video visibility toggle error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: persist video ordering on project.json (videoOrder).
app.post("/api/video/order", requireProjectRole("editor"), async (req, res) => {
  const { project, order } = req.body || {};
  if (!project) return res.status(400).json({ error: "project is required" });
  if (order != null && !Array.isArray(order)) {
    return res.status(400).json({ error: "order must be an array of video paths or null" });
  }
  const videoPrefix = `${project}/_videos/`;
  const cleaned = Array.isArray(order)
    ? order
        .filter((s) => typeof s === "string" && s.startsWith(videoPrefix) && !s.endsWith(".meta.json") && !s.startsWith(`${videoPrefix}_thumbs/`))
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
    if (cleaned.length === 0) delete meta.videoOrder;
    else meta.videoOrder = cleaned;
    meta.updatedAt = new Date().toISOString();
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, videoOrder: meta.videoOrder || [] });
  } catch (err) {
    console.error("video order error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: signed inline streaming URL for any video (including hidden).
// Kept OFF the public URL-map matcher so it routes through IAP and
// preserves admin identity — same reason as the listing mirror above.
app.get("/api/admin/video/stream-url", requireAdmin, async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
    const [url] = await imageBucket.file(filePath).getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
    });
    res.json({ streamUrl: url });
  } catch (err) {
    console.error("Admin video stream-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Inline streaming URL — same signed-URL pattern as download-url, but
// without the attachment Content-Disposition so a <video src> tag can
// play it directly in the browser. Public route, but hidden videos 403.
app.get("/api/video/stream-url", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    const [metadata] = await file.getMetadata();
    if (metadata.metadata && metadata.metadata.hidden === "true") {
      return res.status(403).json({ error: "Video not available" });
    }
    const [url] = await file.getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
    });
    res.json({ streamUrl: url });
  } catch (err) {
    console.error("Video stream-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Set / replace the thumbnail for a video — admin only. Multipart form
// with `file` (the video's GCS path) and `thumbnail` (the image upload).
// Stores under _videos/_thumbs/ with a unique-ish basename so an old URL
// embedded in someone's open tab doesn't accidentally surface a new
// image, and writes the GCS path onto the video's meta sidecar.
app.post("/api/admin/video/thumbnail", upload.single("thumbnail"), requireAdmin, async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "thumbnail file required" });
  const filePath = req.body && req.body.file;
  const tmpPath = req.file.path;
  try {
    if (!filePath) return res.status(400).json({ error: "file (video path) required" });
    if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
    const [exists] = await imageBucket.file(filePath).exists();
    if (!exists) return res.status(404).json({ error: "Video not found" });

    // Project prefix is everything up to /_videos/. Thumb lands under
    // {project}/_videos/_thumbs/<videoBase>-<timestamp>.<ext>.
    const sepIdx = filePath.indexOf(VIDEOS_PREFIX_SEG);
    const project = filePath.slice(0, sepIdx);
    const videoBase = filePath.slice(sepIdx + VIDEOS_PREFIX_SEG.length).replace(/\.[^.]+$/, "");
    const origName = req.file.originalname || "thumb";
    const extMatch = origName.match(/\.([a-z0-9]+)$/i);
    const ext = (extMatch && extMatch[1].toLowerCase()) || "jpg";
    const thumbPath = `${project}/_videos/_thumbs/${videoBase}-${Date.now()}.${ext}`;

    // Best-effort delete of any previous thumbnail before we write the new
    // sidecar pointer — keeps the bucket from accumulating orphans on
    // every re-upload. Failure is non-fatal: the meta still gets repointed.
    const existingMeta = await readVideoMeta(filePath);
    if (existingMeta.thumbnail && existingMeta.thumbnail !== thumbPath) {
      try {
        const old = imageBucket.file(existingMeta.thumbnail);
        const [oldExists] = await old.exists();
        if (oldExists) await old.delete();
      } catch (err) {
        console.warn(`[video] old thumbnail cleanup failed for ${existingMeta.thumbnail}:`, err.message);
      }
    }

    const gcsFile = imageBucket.file(thumbPath);
    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: false,
      metadata: { contentType: req.file.mimetype || "image/jpeg" },
    });
    await new Promise((resolve, reject) => {
      readStream.pipe(writeStream).on("error", reject).on("finish", resolve);
    });

    const newMeta = { ...existingMeta, thumbnail: thumbPath };
    await writeVideoMeta(filePath, newMeta);

    const [signed] = await gcsFile.getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000,
    });
    res.json({ success: true, thumbnail: thumbPath, thumbnailUrl: signed });
  } catch (err) {
    console.error("Video thumbnail upload error:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    fs.unlink(tmpPath, () => {});
  }
});

// Clear the thumbnail for a video — admin only. Removes the sidecar
// pointer and best-effort deletes the GCS object.
app.post("/api/admin/video/thumbnail-clear", requireAdmin, async (req, res) => {
  const { file: filePath } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
  try {
    const meta = await readVideoMeta(filePath);
    if (meta.thumbnail) {
      try {
        const old = imageBucket.file(meta.thumbnail);
        const [oldExists] = await old.exists();
        if (oldExists) await old.delete();
      } catch (err) {
        console.warn(`[video] thumbnail-clear cleanup failed:`, err.message);
      }
      const newMeta = { ...meta };
      delete newMeta.thumbnail;
      await writeVideoMeta(filePath, newMeta);
    }
    res.json({ success: true });
  } catch (err) {
    console.error("Video thumbnail-clear error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Acknowledge a completed direct-to-GCS upload — admin only. Stamps the
// metadata sidecar with the uploader's email and a timestamp so the list
// view can show who uploaded what.
app.post("/api/video/uploaded", requireProjectRole("editor"), async (req, res) => {
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

// Admin: signed download URL for any video (including hidden). Kept
// OFF the public URL-map matcher so it routes through IAP — same
// rationale as the admin listing/stream mirrors above.
app.get("/api/admin/video/download-url", requireAdmin, async (req, res) => {
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
    console.error("Admin video download-url error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Signed download URL for a video — public route (15-min expiry).
// Hidden videos 403 so they don't leak to non-admin viewers.
app.get("/api/video/download-url", async (req, res) => {
  try {
    const filePath = req.query.file;
    if (!filePath) return res.status(400).json({ error: "file is required" });
    if (!isVideoPath(filePath)) return res.status(400).json({ error: "not a video path" });
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    const [metadata] = await file.getMetadata();
    if (metadata.metadata && metadata.metadata.hidden === "true") {
      return res.status(403).json({ error: "Video not available" });
    }
    const [url] = await file.getSignedUrl({
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
app.post("/api/video/rename", requireProjectRole("editor"), async (req, res) => {
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

// Delete a video — editors and admins. Shared videos additionally
// require editor on the owner project (or admin); see authorizeSharedDelete.
// Also removes the sidecar metadata and the thumbnail GCS object (if any)
// so deletes don't leak orphan files into _thumbs/.
app.post("/api/video/delete", requireProjectRole("editor"), async (req, res) => {
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
    const auth = await authorizeSharedDelete(req, f);
    if (!auth.allowed) return res.status(403).json({ error: auth.error });
    // Read meta before deleting it so we know which thumbnail to clean up.
    const meta = await readVideoMeta(filePath);
    await f.delete();
    console.log(`Deleted video: gs://${IMAGE_BUCKET_NAME}/${filePath}${auth.isShared ? " [shared]" : ""}`);
    try {
      const metaFile = imageBucket.file(videoMetaPathFor(filePath));
      const [metaExists] = await metaFile.exists();
      if (metaExists) await metaFile.delete();
    } catch (e) { /* non-fatal */ }
    if (meta.thumbnail) {
      try {
        const thumb = imageBucket.file(meta.thumbnail);
        const [thumbExists] = await thumb.exists();
        if (thumbExists) await thumb.delete();
      } catch (e) { /* non-fatal */ }
    }
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
    const [[files], shared] = await Promise.all([
      modelBucket.getFiles({ prefix }),
      fetchSharedFiles(req, modelBucket),
    ]);
    const allNames = new Set(files.map((f) => f.name));
    const sharedAllNames = new Set(shared.files.map((f) => f.name));
    const hiddenShared = getProjectHiddenSharedSet(req);

    // Identify MTL files paired to a same-basename OBJ — they get hidden from
    // the visible list and attached as a companion to the OBJ entry instead.
    const pairedMtl = new Set();
    for (const name of allNames) {
      if (name.endsWith(".obj")) {
        const mtl = name.slice(0, -4) + ".mtl";
        if (allNames.has(mtl)) pairedMtl.add(mtl);
      }
    }
    const sharedPairedMtl = new Set();
    for (const name of sharedAllNames) {
      if (name.endsWith(".obj")) {
        const mtl = name.slice(0, -4) + ".mtl";
        if (sharedAllNames.has(mtl)) sharedPairedMtl.add(mtl);
      }
    }

    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !pairedMtl.has(f.name))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
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
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !sharedPairedMtl.has(f.name))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
      .filter((f) => !hiddenShared.has(f.name))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        const entry = {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: sharedAllNames.has(thumbName) ? thumbName : null,
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
        if (f.name.endsWith(".obj")) {
          const mtl = f.name.slice(0, -4) + ".mtl";
          if (sharedAllNames.has(mtl)) entry.companions = { mtl };
        }
        return entry;
      });
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List models error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: list models including hidden ones plus the flag — used by
// project-models.html so editors can see + unhide what they've hidden.
app.get("/api/model/files-with-hidden", requireProjectRole("editor"), async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = project + "/";
    const [[files], shared] = await Promise.all([
      modelBucket.getFiles({ prefix }),
      fetchSharedFiles(req, modelBucket),
    ]);
    const allNames = new Set(files.map((f) => f.name));
    const sharedAllNames = new Set(shared.files.map((f) => f.name));
    const hiddenShared = getProjectHiddenSharedSet(req);
    const pairedMtl = new Set();
    for (const name of allNames) {
      if (name.endsWith(".obj")) {
        const mtl = name.slice(0, -4) + ".mtl";
        if (allNames.has(mtl)) pairedMtl.add(mtl);
      }
    }
    const sharedPairedMtl = new Set();
    for (const name of sharedAllNames) {
      if (name.endsWith(".obj")) {
        const mtl = name.slice(0, -4) + ".mtl";
        if (sharedAllNames.has(mtl)) sharedPairedMtl.add(mtl);
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
          hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
        };
        if (f.name.endsWith(".obj")) {
          const mtl = f.name.slice(0, -4) + ".mtl";
          if (allNames.has(mtl)) entry.companions = { mtl };
        }
        return entry;
      });
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !sharedPairedMtl.has(f.name))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        const entry = {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: sharedAllNames.has(thumbName) ? thumbName : null,
          hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
          hiddenInProject: hiddenShared.has(f.name),
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
        if (f.name.endsWith(".obj")) {
          const mtl = f.name.slice(0, -4) + ".mtl";
          if (sharedAllNames.has(mtl)) entry.companions = { mtl };
        }
        return entry;
      });
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List models (with hidden) error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: toggle the hidden flag on a model.
app.post("/api/model/visibility", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, hidden } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (filePath.endsWith(".thumb.jpg")) return res.status(400).json({ error: "cannot hide a thumbnail directly" });
  try {
    const file = modelBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await file.setMetadata({ metadata: { hidden: hidden ? "true" : null } });
    res.json({ success: true, hidden: !!hidden });
  } catch (err) {
    console.error("Model visibility toggle error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: replace a model in place. Same pattern as /api/2d/replace —
// preserves custom metadata (hidden flag) and drops the cached thumbnail
// so the next thumbnail upload reflects the new content.
app.post("/api/model/replace", upload.single("file"), requireProjectRole("editor"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file provided" });
  const tmpPath = req.file.path;
  const filePath = req.body && req.body.destName;
  try {
    if (!filePath) return res.status(400).json({ error: "destName (existing model path) required" });
    if (filePath.endsWith(".thumb.jpg")) return res.status(400).json({ error: "cannot replace a thumbnail directly" });

    const gcsFile = modelBucket.file(filePath);
    const [exists] = await gcsFile.exists();
    if (!exists) return res.status(404).json({ error: "Model not found" });

    const [oldMetadata] = await gcsFile.getMetadata();
    const preservedCustom = (oldMetadata.metadata && typeof oldMetadata.metadata === "object")
      ? { ...oldMetadata.metadata }
      : null;

    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: true,
      metadata: {
        contentType: req.file.mimetype || oldMetadata.contentType || "application/octet-stream",
        ...(preservedCustom ? { metadata: preservedCustom } : {}),
      },
    });
    await new Promise((resolve, reject) => {
      readStream.pipe(writeStream).on("error", reject).on("finish", resolve);
    });

    try {
      const thumb = modelBucket.file(filePath + ".thumb.jpg");
      const [thumbExists] = await thumb.exists();
      if (thumbExists) await thumb.delete();
    } catch (err) {
      console.warn(`[model/replace] thumb cleanup failed for ${filePath}:`, err.message);
    }

    res.json({ success: true, file: filePath, fileSize: req.file.size });
  } catch (err) {
    console.error("Model replace error:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    fs.unlink(tmpPath, () => {});
  }
});

// Editor: persist model ordering on project.json (modelOrder).
app.post("/api/model/order", requireProjectRole("editor"), async (req, res) => {
  const { project, order } = req.body || {};
  if (!project) return res.status(400).json({ error: "project is required" });
  if (order != null && !Array.isArray(order)) {
    return res.status(400).json({ error: "order must be an array of model paths or null" });
  }
  const projectPrefix = `${project}/`;
  const cleaned = Array.isArray(order)
    ? order
        .filter((s) => typeof s === "string" && s.startsWith(projectPrefix) && !s.endsWith(".thumb.jpg"))
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
    if (cleaned.length === 0) delete meta.modelOrder;
    else meta.modelOrder = cleaned;
    meta.updatedAt = new Date().toISOString();
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, modelOrder: meta.modelOrder || [] });
  } catch (err) {
    console.error("model order error:", err.message);
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
    const auth = await authorizeSharedDelete(req, f);
    if (!auth.allowed) return res.status(403).json({ error: auth.error });
    await f.delete();
    console.log(`Deleted model: gs://${MODEL_BUCKET_NAME}/${filePath}${auth.isShared ? " [shared]" : ""}`);

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
    const [[files], shared] = await Promise.all([
      pointcloudBucket.getFiles({ prefix }),
      fetchSharedFiles(req, pointcloudBucket),
    ]);
    const allNames = new Set(files.map((f) => f.name));
    const sharedAllNames = new Set(shared.files.map((f) => f.name));
    const hiddenShared = getProjectHiddenSharedSet(req);
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
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
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .filter((f) => !(f.metadata.metadata && f.metadata.metadata.hidden === "true"))
      .filter((f) => !hiddenShared.has(f.name))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        return {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: sharedAllNames.has(thumbName) ? thumbName : null,
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
      });
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List point clouds error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: list point clouds including hidden ones plus the flag.
app.get("/api/pointcloud/files-with-hidden", requireProjectRole("editor"), async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = project + "/";
    const [[files], shared] = await Promise.all([
      pointcloudBucket.getFiles({ prefix }),
      fetchSharedFiles(req, pointcloudBucket),
    ]);
    const allNames = new Set(files.map((f) => f.name));
    const sharedAllNames = new Set(shared.files.map((f) => f.name));
    const hiddenShared = getProjectHiddenSharedSet(req);
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
          hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
        };
      });
    const sharedList = shared.files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.endsWith(".thumb.jpg"))
      .map((f) => {
        const thumbName = f.name + ".thumb.jpg";
        return {
          name: f.name,
          displayName: shared.prefix ? f.name.replace(shared.prefix, "") : f.name,
          size: Number(f.metadata.size),
          updated: f.metadata.updated,
          contentType: f.metadata.contentType,
          thumbnail: sharedAllNames.has(thumbName) ? thumbName : null,
          hidden: !!(f.metadata.metadata && f.metadata.metadata.hidden === "true"),
          hiddenInProject: hiddenShared.has(f.name),
          ownerProjectId: (f.metadata.metadata && f.metadata.metadata.ownerProjectId) || null,
          shared: true,
        };
      });
    res.json(list.concat(sharedList));
  } catch (err) {
    console.error("List point clouds (with hidden) error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: toggle the hidden flag on a point cloud.
app.post("/api/pointcloud/visibility", requireProjectRole("editor"), async (req, res) => {
  const { file: filePath, hidden } = req.body || {};
  if (!filePath) return res.status(400).json({ error: "file is required" });
  if (filePath.endsWith(".thumb.jpg")) return res.status(400).json({ error: "cannot hide a thumbnail directly" });
  try {
    const file = pointcloudBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) return res.status(404).json({ error: "File not found" });
    await file.setMetadata({ metadata: { hidden: hidden ? "true" : null } });
    res.json({ success: true, hidden: !!hidden });
  } catch (err) {
    console.error("Point cloud visibility toggle error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Editor: replace a point cloud in place — preserves custom metadata
// (hidden flag) and drops the cached thumbnail.
app.post("/api/pointcloud/replace", upload.single("file"), requireProjectRole("editor"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file provided" });
  const tmpPath = req.file.path;
  const filePath = req.body && req.body.destName;
  try {
    if (!filePath) return res.status(400).json({ error: "destName (existing point cloud path) required" });
    if (filePath.endsWith(".thumb.jpg")) return res.status(400).json({ error: "cannot replace a thumbnail directly" });

    const gcsFile = pointcloudBucket.file(filePath);
    const [exists] = await gcsFile.exists();
    if (!exists) return res.status(404).json({ error: "Point cloud not found" });

    const [oldMetadata] = await gcsFile.getMetadata();
    const preservedCustom = (oldMetadata.metadata && typeof oldMetadata.metadata === "object")
      ? { ...oldMetadata.metadata }
      : null;

    const readStream = fs.createReadStream(tmpPath);
    const writeStream = gcsFile.createWriteStream({
      resumable: true,
      metadata: {
        contentType: req.file.mimetype || oldMetadata.contentType || "application/octet-stream",
        ...(preservedCustom ? { metadata: preservedCustom } : {}),
      },
    });
    await new Promise((resolve, reject) => {
      readStream.pipe(writeStream).on("error", reject).on("finish", resolve);
    });

    try {
      const thumb = pointcloudBucket.file(filePath + ".thumb.jpg");
      const [thumbExists] = await thumb.exists();
      if (thumbExists) await thumb.delete();
    } catch (err) {
      console.warn(`[pointcloud/replace] thumb cleanup failed for ${filePath}:`, err.message);
    }

    res.json({ success: true, file: filePath, fileSize: req.file.size });
  } catch (err) {
    console.error("Point cloud replace error:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    fs.unlink(tmpPath, () => {});
  }
});

// Editor: persist point cloud ordering on project.json (pointcloudOrder).
app.post("/api/pointcloud/order", requireProjectRole("editor"), async (req, res) => {
  const { project, order } = req.body || {};
  if (!project) return res.status(400).json({ error: "project is required" });
  if (order != null && !Array.isArray(order)) {
    return res.status(400).json({ error: "order must be an array of point cloud paths or null" });
  }
  const projectPrefix = `${project}/`;
  const cleaned = Array.isArray(order)
    ? order
        .filter((s) => typeof s === "string" && s.startsWith(projectPrefix) && !s.endsWith(".thumb.jpg"))
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
    if (cleaned.length === 0) delete meta.pointcloudOrder;
    else meta.pointcloudOrder = cleaned;
    meta.updatedAt = new Date().toISOString();
    await projectFile.save(JSON.stringify(meta, null, 2), { contentType: "application/json" });
    res.json({ success: true, pointcloudOrder: meta.pointcloudOrder || [] });
  } catch (err) {
    console.error("pointcloud order error:", err.message);
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
    const auth = await authorizeSharedDelete(req, f);
    if (!auth.allowed) return res.status(403).json({ error: auth.error });
    await f.delete();
    console.log(`Deleted point cloud: gs://${POINTCLOUD_BUCKET_NAME}/${filePath}${auth.isShared ? " [shared]" : ""}`);

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
  // Expand the projects map so canEdit/canView work whether the URL
  // slug is the original project name, the canonical compound slug, or
  // any historical alias. For each stored permission, also add the
  // canonical form AND every reverse-alias that points to it. Cheap —
  // single getIndex() call, then in-memory iteration.
  const stored = profile.projects || {};
  const expanded = { ...stored };
  try {
    const idx = await projectResolver.getIndex();
    const reverseAlias = new Map();
    for (const [alias, target] of Object.entries(idx.alias || {})) {
      if (!reverseAlias.has(target)) reverseAlias.set(target, []);
      reverseAlias.get(target).push(alias);
    }
    for (const [k, role] of Object.entries(stored)) {
      const canonical = (idx.alias && idx.alias[k]) || k;
      if (canonical !== k && !expanded[canonical]) expanded[canonical] = role;
      for (const a of (reverseAlias.get(canonical) || [])) {
        if (a !== k && !expanded[a]) expanded[a] = role;
      }
    }
  } catch (err) {
    console.warn("[me] alias expansion failed:", err.message);
  }
  res.json({
    email: profile.email,
    authorized: true,
    isAdmin: !!profile.isAdmin,
    name: profile.name,
    address: profile.address,
    phone: profile.phone,
    createdAt: profile.createdAt,
    projects: expanded,
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

    // Heads-up to the platform owner — same as the authed share endpoint,
    // marked isPublicLink so the email reflects there's no identified
    // sharer to set Reply-To against.
    emailService.sendShareNotification({
      project: display,
      projectUrl,
      recipientEmail: email,
      recipientName: name,
      sharerName: "",
      sharerEmail: "",
      isPublicLink: true,
    }).catch((err) => console.warn("[share-public] notification failed:", err.message));

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

    // Heads-up to the platform owner. Best-effort — log and move on if it
    // fails so the share itself still reports success.
    emailService.sendShareNotification({
      project: display,
      projectUrl,
      recipientEmail: email,
      recipientName: name,
      sharerName: req.user?.name || "",
      sharerEmail: req.user?.email || "",
      isPublicLink: false,
    }).catch((err) => console.warn("[share] notification failed:", err.message));

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
