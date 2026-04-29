#!/usr/bin/env node
// Phase-3 switchover: after every project is backfilled, rewrite the
// permission roster (users.json) and the admin-controlled order file
// (project-order.json -> property-order.json) so they refer to projects
// by their canonical compound slug rather than their pre-migration name.
// Sets _platform/migration-state.json = "switched-over" as the public
// signal that Phase 3 has happened.
//
// Usage:
//   node scripts/switchover.js --dry-run
//   node scripts/switchover.js               # actually write
//   node scripts/switchover.js --rollback    # restore from snapshot
//
// Reads/writes only on the main bucket; image/model/pointcloud buckets
// are untouched by this phase.

const { Storage } = require("@google-cloud/storage");

const MAIN_BUCKET = process.env.MAIN_BUCKET || "gt-platform-360-photos-bucket";

const USERS_PATH = "_platform/users.json";
const PROJECT_ORDER_PATH = "_platform/project-order.json";
const PROPERTY_ORDER_PATH = "_platform/property-order.json";
const PROPERTIES_PATH = "_platform/properties.json";
const SLUG_INDEX_PATH = "_platform/slug-index.json";
const MANIFEST_PATH = "_platform/migration-manifest.jsonl";
const STATE_PATH = "_platform/migration-state.json";

function parseArgs(argv) {
  const args = {};
  for (const a of argv.slice(2)) {
    const m = a.match(/^--([^=]+)(?:=(.*))?$/);
    if (!m) continue;
    args[m[1]] = m[2] === undefined ? true : m[2];
  }
  return args;
}

function backupPath(name) {
  return `_platform/_backup/T0-pre-switchover/${name}`;
}

async function readJson(bucket, p) {
  const file = bucket.file(p);
  const [exists] = await file.exists();
  if (!exists) return null;
  const [content] = await file.download();
  return JSON.parse(content.toString());
}

async function writeJson(bucket, p, data) {
  await bucket
    .file(p)
    .save(JSON.stringify(data, null, 2), { contentType: "application/json" });
}

async function copyToBackup(bucket, src) {
  const [exists] = await bucket.file(src).exists();
  if (!exists) return false;
  const dest = backupPath(src.replace(/^_platform\//, ""));
  await bucket.file(src).copy(bucket.file(dest));
  return dest;
}

async function readManifest(bucket) {
  const file = bucket.file(MANIFEST_PATH);
  const [exists] = await file.exists();
  if (!exists) return [];
  const [content] = await file.download();
  return content
    .toString()
    .split("\n")
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

// ---------------------------------------------------------------------------
// users.json rewrite
// ---------------------------------------------------------------------------
function rewriteUsersJson(users, aliasMap) {
  // users shape: { users: { <email>: { projects: { <name>: "viewer"|"editor" }, ... } } }
  if (!users || !users.users) return { changed: 0, mapped: {} };
  const mapped = {};
  let changed = 0;
  for (const email of Object.keys(users.users)) {
    const u = users.users[email];
    if (!u || !u.projects || typeof u.projects !== "object") continue;
    const next = {};
    for (const [oldKey, role] of Object.entries(u.projects)) {
      const canonical = aliasMap[oldKey] || oldKey;
      if (canonical !== oldKey) {
        changed++;
        mapped[oldKey] = (mapped[oldKey] || 0) + 1;
      }
      // Last write wins on the rare case the user already has both keys.
      next[canonical] = role;
    }
    u.projects = next;
  }
  return { changed, mapped };
}

// ---------------------------------------------------------------------------
// property-order.json build
// ---------------------------------------------------------------------------
function buildPropertyOrder(projectOrder, manifest, properties) {
  // projectOrder: ["My Project A", "My Project B", ...] — old names
  // manifest:    [{ oldProjectName, propertyId, projectId, compoundSlug, ... }]
  // properties:  { version, properties: { propId: { projectIds: [...] } } }
  const byOldName = new Map(manifest.map((e) => [e.oldProjectName, e]));
  const seenProperties = new Set();
  const order = [];
  const orphaned = [];
  for (const oldName of projectOrder || []) {
    const e = byOldName.get(oldName);
    if (!e) {
      orphaned.push(oldName);
      continue;
    }
    if (seenProperties.has(e.propertyId)) continue;
    seenProperties.add(e.propertyId);
    const propEntry = properties && properties.properties && properties.properties[e.propertyId];
    order.push({
      propertyId: e.propertyId,
      projectIds: propEntry ? propEntry.projectIds : [e.projectId],
    });
  }
  // Append any properties that weren't referenced from project-order
  // (e.g. projects added after project-order was last touched).
  if (properties && properties.properties) {
    for (const [propId, p] of Object.entries(properties.properties)) {
      if (!seenProperties.has(propId)) {
        order.push({ propertyId: propId, projectIds: p.projectIds || [] });
      }
    }
  }
  return { order, orphaned, updatedAt: new Date().toISOString() };
}

// ---------------------------------------------------------------------------
// rollback
// ---------------------------------------------------------------------------
async function rollback(bucket, dryRun) {
  console.log("rollback: restoring pre-switchover snapshots");
  const restore = [
    [backupPath("users.json"), USERS_PATH],
    [backupPath("project-order.json"), PROJECT_ORDER_PATH],
    [backupPath("slug-index.json"), SLUG_INDEX_PATH],
  ];
  for (const [src, dest] of restore) {
    const [exists] = await bucket.file(src).exists();
    if (!exists) {
      console.log(`  skip ${dest} (no backup at ${src})`);
      continue;
    }
    console.log(`  restore ${src} -> ${dest}`);
    if (!dryRun) await bucket.file(src).copy(bucket.file(dest));
  }
  if (!dryRun) {
    const state = await readJson(bucket, STATE_PATH);
    if (state) {
      state.phase = "backfilled";
      state.rolledBackAt = new Date().toISOString();
      await writeJson(bucket, STATE_PATH, state);
    }
    // property-order.json is post-switchover artifact — remove it
    const [pOrderExists] = await bucket.file(PROPERTY_ORDER_PATH).exists();
    if (pOrderExists) await bucket.file(PROPERTY_ORDER_PATH).delete();
  }
  console.log("rollback complete");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
async function main() {
  const args = parseArgs(process.argv);
  const dryRun = !!args["dry-run"];

  const storage = new Storage();
  const bucket = storage.bucket(MAIN_BUCKET);

  console.log(`switchover.js  bucket=${MAIN_BUCKET}  dryRun=${dryRun}`);

  if (args.rollback) {
    await rollback(bucket, dryRun);
    return;
  }

  // 1. Load everything we need up front so we can fail loudly before touching anything.
  const [index, users, projectOrder, properties, manifest] = await Promise.all([
    readJson(bucket, SLUG_INDEX_PATH),
    readJson(bucket, USERS_PATH),
    readJson(bucket, PROJECT_ORDER_PATH),
    readJson(bucket, PROPERTIES_PATH),
    readManifest(bucket),
  ]);

  if (!index) throw new Error(`missing ${SLUG_INDEX_PATH} — run Phase-1 indexing first`);
  if (!properties) throw new Error(`missing ${PROPERTIES_PATH} — has Phase-2 migration run?`);
  if (manifest.length === 0) {
    console.warn("warning: manifest is empty; nothing has been migrated yet");
  }

  // 2. Pre-flight cross-checks. Halt on any inconsistency between the
  //    slug-index, manifest, and properties.json — these usually signal
  //    an interrupted migration and should be resolved by hand.
  const problems = [];
  const aliasMap = index.alias || {};
  const canonicalMap = index.canonical || {};
  const canonicalSet = new Set(Object.keys(canonicalMap));

  // (a) dangling aliases
  for (const [alias, target] of Object.entries(aliasMap)) {
    if (!canonicalSet.has(target)) {
      problems.push(`alias "${alias}" -> "${target}" but target not in canonical`);
    }
  }

  // (b) every layout:"new" canonical must have a matching manifest entry
  const manifestByCompound = new Map(manifest.map((e) => [e.compoundSlug, e]));
  for (const [slug, ref] of Object.entries(canonicalMap)) {
    if (ref.layout !== "new") continue;
    if (!manifestByCompound.has(slug)) {
      problems.push(`canonical "${slug}" is layout:"new" but has no manifest entry`);
    }
  }

  // (c) every manifest entry must be present as a layout:"new" canonical
  for (const e of manifest) {
    const ref = canonicalMap[e.compoundSlug];
    if (!ref) {
      problems.push(`manifest entry for "${e.oldProjectName}" -> "${e.compoundSlug}" missing from canonical`);
    } else if (ref.layout !== "new") {
      problems.push(`manifest entry "${e.compoundSlug}" exists but layout="${ref.layout}" (expected "new")`);
    } else if (ref.propertyId !== e.propertyId || ref.projectId !== e.projectId) {
      problems.push(`canonical "${e.compoundSlug}" IDs disagree with manifest (canonical=${ref.propertyId}/${ref.projectId}, manifest=${e.propertyId}/${e.projectId})`);
    }
  }

  // (d) every property in properties.json should have at least one
  //     migrated project (otherwise it's an orphan record)
  const propsRoot = properties.properties || {};
  const manifestPropertyIds = new Set(manifest.map((e) => e.propertyId));
  for (const propId of Object.keys(propsRoot)) {
    if (!manifestPropertyIds.has(propId)) {
      problems.push(`properties.json has property "${propId}" with no manifest reference (orphan)`);
    }
  }

  if (problems.length > 0) {
    console.error("pre-flight failures:");
    for (const p of problems) console.error("  -", p);
    throw new Error(`aborting: ${problems.length} pre-flight problem(s) detected`);
  }
  console.log(`pre-flight ok: ${manifest.length} migrated project(s), ${Object.keys(propsRoot).length} property/properties, ${Object.keys(aliasMap).length} alias(es)`);

  // 3. Snapshot to _backup/ so rollback works.
  console.log("snapshot:");
  for (const p of [USERS_PATH, PROJECT_ORDER_PATH, SLUG_INDEX_PATH]) {
    if (dryRun) {
      console.log(`  would back up ${p}`);
    } else {
      const dest = await copyToBackup(bucket, p);
      console.log(dest ? `  backed up ${p} -> ${dest}` : `  skipped ${p} (not present)`);
    }
  }

  // 4. Rewrite users.json
  const usersClone = users ? JSON.parse(JSON.stringify(users)) : { users: {} };
  const { changed, mapped } = rewriteUsersJson(usersClone, aliasMap);
  console.log(
    `users.json: ${changed} permission key(s) remapped across ${Object.keys(mapped).length} distinct old name(s)`
  );
  for (const [oldKey, count] of Object.entries(mapped)) {
    console.log(`  ${oldKey} (-> ${aliasMap[oldKey]})  x${count}`);
  }

  // 5. Build property-order.json
  const propertyOrder = buildPropertyOrder(
    projectOrder?.order || projectOrder || [],
    manifest,
    properties
  );
  console.log(
    `property-order.json: ${propertyOrder.order.length} properties; ${propertyOrder.orphaned.length} orphaned project-order entries`
  );
  if (propertyOrder.orphaned.length > 0) {
    console.log(
      `  orphaned (no manifest entry): ${propertyOrder.orphaned.join(", ")}`
    );
  }

  // 6. Migration state
  const state = {
    phase: "switched-over",
    switchedOverAt: new Date().toISOString(),
    manifestSize: manifest.length,
  };

  if (dryRun) {
    console.log("\nDRY RUN — no writes performed.");
    console.log("Would write:", USERS_PATH, PROPERTY_ORDER_PATH, STATE_PATH);
    return;
  }

  // 7. Apply.
  await writeJson(bucket, USERS_PATH, usersClone);
  console.log(`wrote ${USERS_PATH}`);
  await writeJson(bucket, PROPERTY_ORDER_PATH, propertyOrder);
  console.log(`wrote ${PROPERTY_ORDER_PATH}`);
  await writeJson(bucket, STATE_PATH, state);
  console.log(`wrote ${STATE_PATH}  phase=${state.phase}`);

  console.log("\nswitchover complete.");
  console.log(
    "Reminder: project-order.json is left in place but no longer authoritative; remove it after verification."
  );
}

if (require.main === module) {
  main().catch((err) => {
    console.error("fatal:", err);
    process.exit(1);
  });
}

module.exports = {
  parseArgs,
  rewriteUsersJson,
  buildPropertyOrder,
};
