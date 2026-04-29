#!/usr/bin/env node
// Phase-2 backfill: copy a project's data from {projectName}/... to
// {propertyId}/{projectId}/... across all four GCS buckets, register a
// Property record, flip the slug-index entry to layout:"new". Originals
// stay intact until Phase-5 cleanup runs.
//
// Usage:
//   node scripts/migrate-to-properties.js --dry-run
//   node scripts/migrate-to-properties.js --project="123 Main Street"
//   node scripts/migrate-to-properties.js --all --limit=10
//   node scripts/migrate-to-properties.js --rollback="123 Main Street"
//
// Bucket overrides via env: MAIN_BUCKET, IMAGE_BUCKET, MODEL_BUCKET,
// POINTCLOUD_BUCKET (defaults match production).

const path = require("path");
const crypto = require("crypto");
const { Storage } = require("@google-cloud/storage");
const createProjectResolver = require("../project-resolver");

const MAIN_BUCKET = process.env.MAIN_BUCKET || "gt-platform-360-photos-bucket";
const IMAGE_BUCKET = process.env.IMAGE_BUCKET || "gt_platform_image_storage";
const MODEL_BUCKET = process.env.MODEL_BUCKET || "gt_platform_model_storage";
const POINTCLOUD_BUCKET =
  process.env.POINTCLOUD_BUCKET || "gt_platform_pointcloud_storage";

const PROPERTIES_PATH = "_platform/properties.json";
const MANIFEST_PATH = "_platform/migration-manifest.jsonl";
const PLATFORM_PREFIX = "_platform/";

// ---------------------------------------------------------------------------
// arg parsing
// ---------------------------------------------------------------------------
function parseArgs(argv) {
  const args = {};
  for (const a of argv.slice(2)) {
    const m = a.match(/^--([^=]+)(?:=(.*))?$/);
    if (!m) continue;
    args[m[1]] = m[2] === undefined ? true : m[2];
  }
  return args;
}

// ---------------------------------------------------------------------------
// id + slug helpers
// ---------------------------------------------------------------------------
function shortId() {
  return crypto.randomUUID().replace(/-/g, "").slice(0, 16);
}

function isCompleteAddress(addr) {
  return typeof addr === "string" && addr.trim().length > 0;
}

// Slugifier inlined from project-resolver to avoid double-instantiating.
function slugify(s) {
  return String(s)
    .trim()
    .toLowerCase()
    .replace(/['"`]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function todayStamp() {
  return new Date().toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// GCS json helpers
// ---------------------------------------------------------------------------
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

async function appendLine(bucket, p, line) {
  // GCS has no append; read-modify-write. Single-process script, no
  // concurrency worry. The manifest is small.
  const file = bucket.file(p);
  const [exists] = await file.exists();
  let body = "";
  if (exists) {
    const [content] = await file.download();
    body = content.toString();
    if (body && !body.endsWith("\n")) body += "\n";
  }
  body += line + "\n";
  await file.save(body, { contentType: "application/x-ndjson" });
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
// project metadata path-rewriting
// ---------------------------------------------------------------------------
function rewriteProjectMeta(meta, oldName, newPrefix) {
  const oldP = `${oldName}/`;
  const newP = `${newPrefix}/`;
  const r = (s) =>
    typeof s === "string" && s.startsWith(oldP) ? newP + s.slice(oldP.length) : s;

  if (meta.coverPhoto) meta.coverPhoto = r(meta.coverPhoto);
  if (meta.cardThumbnails && typeof meta.cardThumbnails === "object") {
    for (const k of Object.keys(meta.cardThumbnails)) {
      meta.cardThumbnails[k] = r(meta.cardThumbnails[k]);
    }
  }
  if (Array.isArray(meta.imageOrder)) meta.imageOrder = meta.imageOrder.map(r);
  if (Array.isArray(meta.cardOrder)) meta.cardOrder = meta.cardOrder.map(r);
  return meta;
}

// ---------------------------------------------------------------------------
// project discovery
// ---------------------------------------------------------------------------
async function discoverProjects(mainBucket) {
  const [, , api] = await mainBucket.getFiles({
    delimiter: "/",
    autoPaginate: false,
  });
  const prefixes = (api && api.prefixes) || [];
  return prefixes
    .map((p) => p.replace(/\/$/, ""))
    .filter((p) => p && !p.startsWith("_platform"))
    .sort();
}

// ---------------------------------------------------------------------------
// per-bucket copy
// ---------------------------------------------------------------------------
async function listAllUnderPrefix(bucket, prefix) {
  const [files] = await bucket.getFiles({ prefix });
  return files;
}

async function copyOne(srcFile, destFile) {
  await srcFile.copy(destFile);
}

async function copyPrefix(bucket, oldPrefix, newPrefix, concurrency, log) {
  const files = await listAllUnderPrefix(bucket, oldPrefix);
  if (files.length === 0) return { count: 0, sampleHashes: [] };
  log(`  ${bucket.name}: ${files.length} objects`);

  // Run copies in bounded-concurrency waves.
  let i = 0;
  async function worker() {
    while (i < files.length) {
      const idx = i++;
      const src = files[idx];
      const destName = newPrefix + src.name.slice(oldPrefix.length);
      await copyOne(src, bucket.file(destName));
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(concurrency, files.length) }, worker)
  );

  // Sample-verify md5 on a few random objects.
  const sample = pickSample(files, 5);
  const sampleHashes = [];
  for (const src of sample) {
    const destName = newPrefix + src.name.slice(oldPrefix.length);
    const [srcMeta] = await src.getMetadata();
    const [destMeta] = await bucket.file(destName).getMetadata();
    const ok = srcMeta.md5Hash && srcMeta.md5Hash === destMeta.md5Hash;
    sampleHashes.push({ name: src.name, ok });
    if (!ok) {
      throw new Error(
        `md5 mismatch on ${src.name} → ${destName} in ${bucket.name}`
      );
    }
  }
  return { count: files.length, sampleHashes };
}

function pickSample(arr, n) {
  if (arr.length <= n) return arr.slice();
  const out = [];
  const seen = new Set();
  while (out.length < n) {
    const i = Math.floor(Math.random() * arr.length);
    if (!seen.has(i)) {
      seen.add(i);
      out.push(arr[i]);
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// properties.json mutation
// ---------------------------------------------------------------------------
async function mergeProperty(mainBucket, property) {
  const cur = (await readJson(mainBucket, PROPERTIES_PATH)) || {
    version: 0,
    properties: {},
  };
  cur.properties[property.propertyId] = property;
  cur.version = (cur.version || 0) + 1;
  await writeJson(mainBucket, PROPERTIES_PATH, cur);
}

async function unmergeProperty(mainBucket, propertyId) {
  const cur = await readJson(mainBucket, PROPERTIES_PATH);
  if (!cur || !cur.properties || !cur.properties[propertyId]) return;
  delete cur.properties[propertyId];
  cur.version = (cur.version || 0) + 1;
  await writeJson(mainBucket, PROPERTIES_PATH, cur);
}

// ---------------------------------------------------------------------------
// migrate one project
// ---------------------------------------------------------------------------
async function migrateProject(ctx, oldName) {
  const { buckets, resolver, dryRun, concurrency } = ctx;
  const main = buckets.main;
  const log = (...a) => console.log(`[${oldName}]`, ...a);

  // Idempotency via slug index — already done?
  const idx =
    (await readJson(main, "_platform/slug-index.json")) || {
      canonical: {},
      alias: {},
    };
  const existing = idx.canonical[oldName];
  if (existing && existing.layout === "new") {
    log("already migrated, skipping");
    return null;
  }

  // Load existing project.json from main bucket
  const oldMetaPath = `${oldName}/project.json`;
  const oldMeta = await readJson(main, oldMetaPath);
  if (!oldMeta) {
    log("no project.json found, skipping (orphaned folder?)");
    return null;
  }

  const propertyId = `prop_${shortId()}`;
  const projectId = `proj_${shortId()}`;
  const propertyName = (oldMeta.address && oldMeta.address.trim()) || oldName;
  const propertySlug = slugify(propertyName) || slugify(oldName) || "property";
  const projectSlug = slugify(oldMeta.name || oldName) || `project-${todayStamp()}`;
  const compoundSlug = `${propertySlug}--${projectSlug}`;
  const newPrefix = `${propertyId}/${projectId}`;

  log(`→ ${compoundSlug}`);
  log(`   propertyId=${propertyId} projectId=${projectId}`);
  log(`   newPrefix=${newPrefix}`);
  log(`   address=${JSON.stringify(oldMeta.address || "")}`);

  if (dryRun) {
    // Just count what would be copied.
    const counts = {};
    for (const [role, b] of Object.entries(buckets)) {
      const files = await listAllUnderPrefix(b, `${oldName}/`);
      counts[role] = files.length;
    }
    log(`   would copy:`, counts);
    return { dryRun: true };
  }

  // 1. Mark migrating on old project.json so the server (when resolver is
  //    wired in) returns 503 on writes during the copy window.
  await writeJson(main, oldMetaPath, { ...oldMeta, migrating: true });

  // 2. Copy all four buckets, sequentially per bucket to keep error
  //    handling simple (within a bucket we run parallel workers).
  const objectCounts = {};
  for (const [role, b] of Object.entries(buckets)) {
    const result = await copyPrefix(
      b,
      `${oldName}/`,
      `${newPrefix}/`,
      concurrency,
      log
    );
    objectCounts[role] = result.count;
  }

  // 3. Build the new project.json (no address, with IDs, paths rewritten).
  const newMeta = rewriteProjectMeta(
    { ...oldMeta },
    oldName,
    newPrefix
  );
  delete newMeta.address;
  delete newMeta.migrating;
  newMeta.propertyId = propertyId;
  newMeta.projectId = projectId;
  newMeta.slug = compoundSlug;
  newMeta.migratedAt = new Date().toISOString();
  await writeJson(main, `${newPrefix}/project.json`, newMeta);

  // 4. Build the Property record.
  const property = {
    propertyId,
    name: propertyName,
    slug: propertySlug,
    address: oldMeta.address || "",
    needsAddress: !isCompleteAddress(oldMeta.address),
    createdAt: oldMeta.createdAt || new Date().toISOString(),
    createdBy: oldMeta.createdBy || "migration",
    projectIds: [projectId],
  };
  await mergeProperty(main, property);

  // 5. Atomically flip the slug index — this is the "go-live" moment for
  //    this project. After this returns, the resolver serves the new path.
  await resolver.recordMigration({
    oldName,
    propertyId,
    projectId,
    compoundSlug,
  });

  // 6. Append manifest entry for audit + rollback.
  const entry = {
    oldProjectName: oldName,
    propertyId,
    projectId,
    compoundSlug,
    propertyName,
    needsAddress: property.needsAddress,
    objectCounts,
    at: new Date().toISOString(),
  };
  await appendLine(main, MANIFEST_PATH, JSON.stringify(entry));

  log("done");
  return entry;
}

// ---------------------------------------------------------------------------
// rollback one project
// ---------------------------------------------------------------------------
async function rollbackProject(ctx, oldName) {
  const { buckets, resolver, dryRun } = ctx;
  const main = buckets.main;
  const log = (...a) => console.log(`[rollback ${oldName}]`, ...a);

  const manifest = await readManifest(main);
  const entry = manifest.find((e) => e.oldProjectName === oldName);
  if (!entry) {
    log("no manifest entry; nothing to rollback");
    return;
  }
  const { propertyId, projectId, compoundSlug } = entry;
  const newPrefix = `${propertyId}/${projectId}`;
  log(`undoing migration → ${compoundSlug}`);

  if (dryRun) {
    log("would delete new tree, unmerge property, revert slug index");
    return;
  }

  // 1. Delete all objects under {newPrefix}/ in every bucket. Ignore
  //    "not found" since some buckets may have been empty for this project.
  for (const [role, b] of Object.entries(buckets)) {
    const files = await listAllUnderPrefix(b, `${newPrefix}/`);
    log(`  ${b.name}: deleting ${files.length} objects`);
    for (const f of files) {
      try {
        await f.delete();
      } catch (err) {
        if (err.code !== 404) throw err;
      }
    }
  }

  // 2. Remove from properties.json
  await unmergeProperty(main, propertyId);

  // 3. Revert slug index — drop the new canonical entry + alias and
  //    restore the old one as layout:"old". Do this directly via mutateIndex
  //    rather than adding a public revertMigration() to the resolver, since
  //    rollback is migration-script-only territory.
  await resolver.buildLegacyIndex([oldName]); // re-add as layout:"old"
  // Drop the new canonical + aliases by replaying through a transient call:
  // simplest path is to read+write the index directly here.
  const idx = await readJson(main, "_platform/slug-index.json");
  if (idx) {
    if (idx.canonical[compoundSlug] && idx.canonical[compoundSlug].propertyId === propertyId) {
      delete idx.canonical[compoundSlug];
    }
    for (const k of Object.keys(idx.alias)) {
      if (idx.alias[k] === compoundSlug) delete idx.alias[k];
    }
    idx.version = (idx.version || 0) + 1;
    await writeJson(main, "_platform/slug-index.json", idx);
    resolver.invalidateIndex();
  }

  // 4. Strip migrating + IDs from the OLD project.json so the project
  //    looks fully un-migrated again.
  const oldMetaPath = `${oldName}/project.json`;
  const oldMeta = await readJson(main, oldMetaPath);
  if (oldMeta) {
    delete oldMeta.migrating;
    delete oldMeta.propertyId;
    delete oldMeta.projectId;
    delete oldMeta.slug;
    delete oldMeta.migratedAt;
    await writeJson(main, oldMetaPath, oldMeta);
  }

  // 5. Remove the manifest entry by rewriting without it.
  const remaining = manifest.filter((e) => e.oldProjectName !== oldName);
  const body = remaining.map((e) => JSON.stringify(e)).join("\n");
  await main
    .file(MANIFEST_PATH)
    .save(body ? body + "\n" : "", { contentType: "application/x-ndjson" });

  log("rollback complete");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
async function main() {
  const args = parseArgs(process.argv);
  const dryRun = !!args["dry-run"];
  const concurrency = parseInt(args.concurrency, 10) || 8;
  const limit = args.limit ? parseInt(args.limit, 10) : Infinity;

  const storage = new Storage();
  const buckets = {
    main: storage.bucket(MAIN_BUCKET),
    image: storage.bucket(IMAGE_BUCKET),
    model: storage.bucket(MODEL_BUCKET),
    pointcloud: storage.bucket(POINTCLOUD_BUCKET),
  };
  const resolver = createProjectResolver({ bucket: buckets.main });

  const ctx = { buckets, resolver, dryRun, concurrency };

  console.log(
    `migrate-to-properties.js  dryRun=${dryRun}  concurrency=${concurrency}`
  );
  console.log(
    `  buckets: main=${MAIN_BUCKET} image=${IMAGE_BUCKET} model=${MODEL_BUCKET} pointcloud=${POINTCLOUD_BUCKET}`
  );

  // Rollback path (single project)
  if (typeof args.rollback === "string") {
    await rollbackProject(ctx, args.rollback);
    return;
  }

  // Single-project migrate
  if (typeof args.project === "string") {
    await migrateProject(ctx, args.project);
    return;
  }

  // --all (or default if no project specified)
  if (args.all || (!args.project && !args.rollback)) {
    const all = await discoverProjects(buckets.main);
    console.log(`discovered ${all.length} project folder(s) in main bucket`);

    // Phase-1 helper: index any not-yet-indexed projects as layout:"old".
    // This is safe and idempotent; running migration before an explicit
    // Phase-1 indexing pass would otherwise leave the index empty.
    if (!dryRun) await resolver.buildLegacyIndex(all);

    let processed = 0;
    for (const name of all) {
      if (processed >= limit) break;
      try {
        const result = await migrateProject(ctx, name);
        if (result) processed++;
      } catch (err) {
        console.error(`[${name}] FAILED:`, err.message);
        console.error(err.stack);
        console.error(
          "  Stopping. Re-run with --rollback to undo this project, or fix and re-run."
        );
        process.exit(1);
      }
    }
    console.log(`migrated ${processed} project(s)`);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error("fatal:", err);
    process.exit(1);
  });
}

module.exports = {
  parseArgs,
  rewriteProjectMeta,
  isCompleteAddress,
  shortId,
  slugify,
};
