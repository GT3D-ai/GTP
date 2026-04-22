// User roster stored as a single JSON object in GCS.
// Path: _platform/users.json in the 360 photos bucket.
// Provides profile CRUD + per-project role checks.

const ROSTER_PATH = "_platform/users.json";
const VALID_ROLES = new Set(["viewer", "editor"]);
const ROLE_RANK = { viewer: 1, editor: 2 };

function nowIso() { return new Date().toISOString(); }
function normalizeEmail(e) { return (e || "").trim().toLowerCase(); }

module.exports = function createUserService({ bucket }) {
  const file = bucket.file(ROSTER_PATH);
  let cache = null;
  let cacheAt = 0;

  async function loadRoster() {
    try {
      const [content] = await file.download();
      const data = JSON.parse(content.toString());
      if (!data.users || typeof data.users !== "object") data.users = {};
      return data;
    } catch (err) {
      if (err.code === 404) return { users: {} };
      throw err;
    }
  }

  async function saveRoster(data, { ifGenerationMatch } = {}) {
    const opts = { contentType: "application/json" };
    if (ifGenerationMatch !== undefined) opts.preconditionOpts = { ifGenerationMatch };
    await file.save(JSON.stringify(data, null, 2), opts);
    cache = data;
    cacheAt = Date.now();
  }

  async function getRosterCached({ maxAgeMs = 10_000 } = {}) {
    if (cache && Date.now() - cacheAt < maxAgeMs) return cache;
    cache = await loadRoster();
    cacheAt = Date.now();
    return cache;
  }

  function invalidate() { cache = null; cacheAt = 0; }

  async function getUser(email) {
    const e = normalizeEmail(email);
    if (!e) return null;
    const r = await getRosterCached();
    return r.users[e] || null;
  }

  async function listUsers() {
    const r = await getRosterCached();
    return Object.values(r.users);
  }

  async function upsertUser(email, patch) {
    const e = normalizeEmail(email);
    if (!e) throw new Error("email required");
    const roster = await loadRoster();
    const existing = roster.users[e];
    const merged = {
      email: e,
      name: patch.name ?? existing?.name ?? null,
      address: patch.address ?? existing?.address ?? null,
      phone: patch.phone ?? existing?.phone ?? null,
      isAdmin: typeof patch.isAdmin === "boolean" ? patch.isAdmin : !!existing?.isAdmin,
      projects: patch.projects && typeof patch.projects === "object"
        ? sanitizeProjects(patch.projects)
        : existing?.projects || {},
      createdAt: existing?.createdAt || nowIso(),
      updatedAt: nowIso(),
    };
    roster.users[e] = merged;
    await saveRoster(roster);
    return merged;
  }

  function sanitizeProjects(projMap) {
    const out = {};
    for (const [k, v] of Object.entries(projMap)) {
      if (!k) continue;
      if (!VALID_ROLES.has(v)) continue;
      out[k] = v;
    }
    return out;
  }

  async function deleteUser(email) {
    const e = normalizeEmail(email);
    const roster = await loadRoster();
    if (!roster.users[e]) return false;
    // Don't allow deleting the last admin
    const admins = Object.values(roster.users).filter((u) => u.isAdmin);
    if (roster.users[e].isAdmin && admins.length <= 1) {
      throw new Error("Cannot delete the last admin");
    }
    delete roster.users[e];
    await saveRoster(roster);
    return true;
  }

  async function setProjectRole(email, project, role) {
    const e = normalizeEmail(email);
    const roster = await loadRoster();
    const user = roster.users[e];
    if (!user) throw new Error("User not found");
    if (!user.projects) user.projects = {};
    if (role === null || role === undefined || role === "") {
      delete user.projects[project];
    } else {
      if (!VALID_ROLES.has(role)) throw new Error("Invalid role");
      user.projects[project] = role;
    }
    user.updatedAt = nowIso();
    await saveRoster(roster);
    return user;
  }

  async function hasProjectAccess(email, project, minRole) {
    const user = await getUser(email);
    if (!user) return false;
    if (user.isAdmin) return true;
    if (!project) return false;
    const role = user.projects?.[project];
    if (!role) return false;
    return ROLE_RANK[role] >= ROLE_RANK[minRole];
  }

  async function isAdmin(email) {
    const user = await getUser(email);
    return !!user?.isAdmin;
  }

  async function accessibleProjects(email, allProjects) {
    const user = await getUser(email);
    if (!user) return [];
    if (user.isAdmin) return allProjects.slice();
    const assigned = new Set(Object.keys(user.projects || {}));
    return allProjects.filter((p) => assigned.has(p));
  }

  // Create the first admin if the roster is empty. Guarded by a GCS generation
  // precondition so two simultaneous first-requests can't both win.
  async function bootstrapIfEmpty(email, profile = {}) {
    const e = normalizeEmail(email);
    if (!e) throw new Error("email required");
    const current = await loadRoster();
    if (Object.keys(current.users).length > 0) return { bootstrapped: false };
    const user = {
      email: e,
      name: profile.name ?? null,
      address: profile.address ?? null,
      phone: profile.phone ?? null,
      isAdmin: true,
      projects: {},
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    try {
      await saveRoster({ users: { [e]: user } }, { ifGenerationMatch: 0 });
      console.log(`[user-service] bootstrap: ${e} is now admin`);
      return { bootstrapped: true, user };
    } catch (err) {
      // Race: another instance wrote first. Fall through so caller will do a
      // normal lookup next time.
      if (err.code === 412) {
        invalidate();
        return { bootstrapped: false, lostRace: true };
      }
      throw err;
    }
  }

  return {
    loadRoster, saveRoster, getRosterCached, invalidate,
    getUser, listUsers, upsertUser, deleteUser, setProjectRole,
    hasProjectAccess, isAdmin, accessibleProjects, bootstrapIfEmpty,
  };
};
