#!/usr/bin/env node
// Phase-5 cleanup: delete old {projectName}/ paths after verifying every
// object has a counterpart at {propertyId}/{projectId}/. POINT OF NO RETURN.
//
// Recovery after this point requires GCS soft-delete restoration within
// the bucket's soft-delete window. Verify that retention before running.
//
// Usage:
//   node scripts/cleanup-old-paths.js --dry-run
//   node scripts/cleanup-old-paths.js --project="123 Main Street" --dry-run
//   node scripts/cleanup-old-paths.js --all --confirm-irreversible
//
// Refuses to run unless _platform/migration-state.json is "switched-over"
// AND has been switched over for at least --min-age-days days (default 14).

const { Storage } = require("@google-cloud/storage");

const MAIN_BUCKET = process.env.MAIN_BUCKET || "gt-platform-360-photos-bucket";
const IMAGE_BUCKET = process.env.IMAGE_BUCKET || "gt_platform_image_storage";
const MODEL_BUCKET = process.env.MODEL_BUCKET || "gt_platform_model_storage";
const POINTCLOUD_BUCKET =
  process.env.POINTCLOUD_BUCKET || "gt_platform_pointcloud_storage";

const MANIFEST_PATH = "_platform/migration-manifest.jsonl";
const STATE_PATH = "_platform/migration-state.json";
const SLUG_INDEX_PATH = "_platform/slug-index.json";
const DEL_LOG_PATH = "_platform/cleanup-deletion-log.jsonl";

const DEFAULT_MIN_AGE_DAYS = 14;

function parseArgs(argv) {
  const args = {};
  for (const a of argv.slice(2)) {
    const m = a.match(/^--([^=]+)(?:=(.*))?$/);
    if (!m) continue;
    args[m[1]] = m[2] === undefined ? true : m[2];
  }
  return args;
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

async function appendDelLog(bucket, line) {
  const file = bucket.file(DEL_LOG_PATH);
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

// ---------------------------------------------------------------------------
// safety gates
// ---------------------------------------------------------------------------
async function preflight(mainBucket, args) {
  const state = await readJson(mainBucket, STATE_PATH);
  if (!state) {
    throw new Error(
      `${STATE_PATH} not found — switchover hasn't run; cleanup is premature`
    );
  }
  if (state.phase !== "switched-over") {
    throw new Error(
      `migration-state.phase="${state.phase}", expected "switched-over"`
    );
  }
  if (!state.switchedOverAt) {
    throw new Error("migration-state.switchedOverAt missing");
  }
  const minAgeDays = parseInt(args["min-age-days"], 10);
  const minAge = Number.isFinite(minAgeDays) ? minAgeDays : DEFAULT_MIN_AGE_DAYS;
  const ageMs = Date.now() - new Date(state.switchedOverAt).getTime();
  const ageDays = ageMs / (1000 * 60 * 60 * 24);
  if (ageDays < minAge) {
    throw new Error(
      `switched over only ${ageDays.toFixed(1)} day(s) ago; minimum is ${minAge} (override with --min-age-days=N)`
    );
  }
  console.log(
    `preflight ok: switched-over ${ageDays.toFixed(1)} days ago (>= ${minAge})`
  );
  return state;
}

// ---------------------------------------------------------------------------
// per-project cleanup
// ---------------------------------------------------------------------------
async function listAll(bucket, prefix) {
  const [files] = await bucket.getFiles({ prefix });
  return files;
}

async function verifyAndDeleteProject(buckets, entry, opts) {
  const { dryRun } = opts;
  const log = (...a) => console.log(`[${entry.oldProjectName}]`, ...a);

  const oldPrefix = `${entry.oldProjectName}/`;
  const newPrefix = `${entry.propertyId}/${entry.projectId}/`;

  let totalDeleted = 0;
  for (const [role, bucket] of Object.entries(buckets)) {
    const oldFiles = await listAll(bucket, oldPrefix);
    if (oldFiles.length === 0) {
      log(`  ${bucket.name}: nothing under ${oldPrefix} (already cleaned?)`);
      continue;
    }
    log(`  ${bucket.name}: ${oldFiles.length} old object(s) to verify+delete`);

    // Build a Set of new-path object names for fast lookup.
    const newFiles = await listAll(bucket, newPrefix);
    const newNames = new Set(newFiles.map((f) => f.name));

    // Verify every old object has a counterpart at the new path with
    // matching md5. If even one doesn't, abort the project (safer to
    // leave old data in place than half-delete).
    const toDelete = [];
    for (const oldFile of oldFiles) {
      const expectedNew = newPrefix + oldFile.name.slice(oldPrefix.length);
      if (!newNames.has(expectedNew)) {
        throw new Error(
          `missing counterpart in ${bucket.name}: ${oldFile.name} -> expected ${expectedNew}`
        );
      }
      const [oldMeta] = await oldFile.getMetadata();
      const [newMeta] = await bucket.file(expectedNew).getMetadata();
      if (
        oldMeta.md5Hash &&
        newMeta.md5Hash &&
        oldMeta.md5Hash !== newMeta.md5Hash
      ) {
        throw new Error(
          `md5 mismatch in ${bucket.name}: ${oldFile.name} (${oldMeta.md5Hash}) vs ${expectedNew} (${newMeta.md5Hash})`
        );
      }
      toDelete.push(oldFile);
    }

    if (dryRun) {
      log(`  ${bucket.name}: verified ${toDelete.length}; would delete (dry run)`);
      continue;
    }

    for (const f of toDelete) {
      await f.delete();
      totalDeleted++;
    }
    log(`  ${bucket.name}: deleted ${toDelete.length} old object(s)`);
  }

  return totalDeleted;
}

// ---------------------------------------------------------------------------
// slug-index cleanup (drop legacy entries that are now defunct)
// ---------------------------------------------------------------------------
async function dropLegacyIndexEntries(bucket, projectNames, dryRun) {
  const idx = await readJson(bucket, SLUG_INDEX_PATH);
  if (!idx) return;
  let dropped = 0;
  for (const name of projectNames) {
    if (idx.canonical && idx.canonical[name] && idx.canonical[name].layout === "old") {
      delete idx.canonical[name];
      dropped++;
    }
  }
  if (dropped === 0) return;
  console.log(`slug-index: dropping ${dropped} legacy entry/entries`);
  if (dryRun) return;
  idx.version = (idx.version || 0) + 1;
  await writeJson(bucket, SLUG_INDEX_PATH, idx);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
async function main() {
  const args = parseArgs(process.argv);
  const dryRun = !!args["dry-run"];
  const all = !!args.all;
  const onlyProject = typeof args.project === "string" ? args.project : null;
  const confirm = !!args["confirm-irreversible"];

  const storage = new Storage();
  const buckets = {
    main: storage.bucket(MAIN_BUCKET),
    image: storage.bucket(IMAGE_BUCKET),
    model: storage.bucket(MODEL_BUCKET),
    pointcloud: storage.bucket(POINTCLOUD_BUCKET),
  };

  console.log(
    `cleanup-old-paths.js  dryRun=${dryRun}  all=${all}  project=${onlyProject || "(none)"}`
  );

  if (!dryRun && !confirm) {
    throw new Error(
      "refusing to run destructive cleanup without --confirm-irreversible (use --dry-run first)"
    );
  }

  // Safety gates
  await preflight(buckets.main, args);

  // Pull manifest
  const manifest = await readManifest(buckets.main);
  if (manifest.length === 0) {
    console.log("manifest is empty; nothing to clean.");
    return;
  }

  let entries = manifest;
  if (onlyProject) {
    entries = manifest.filter((e) => e.oldProjectName === onlyProject);
    if (entries.length === 0) {
      throw new Error(`no manifest entry for ${onlyProject}`);
    }
  } else if (!all) {
    throw new Error("specify --project=<name> or --all");
  }

  console.log(`processing ${entries.length} project(s)`);

  let totalDeleted = 0;
  const cleanedNames = [];
  for (const entry of entries) {
    try {
      const n = await verifyAndDeleteProject(buckets, entry, { dryRun });
      totalDeleted += n;
      cleanedNames.push(entry.oldProjectName);
      if (!dryRun) {
        await appendDelLog(
          buckets.main,
          JSON.stringify({
            oldProjectName: entry.oldProjectName,
            propertyId: entry.propertyId,
            projectId: entry.projectId,
            deleted: n,
            at: new Date().toISOString(),
          })
        );
      }
    } catch (err) {
      console.error(`[${entry.oldProjectName}] FAILED:`, err.message);
      console.error(
        "  Stopping. Old data for this project remains intact. Investigate and re-run."
      );
      process.exit(1);
    }
  }

  // Drop layout:"old" entries from slug-index for cleaned projects.
  await dropLegacyIndexEntries(buckets.main, cleanedNames, dryRun);

  console.log(
    `\n${dryRun ? "would delete" : "deleted"} ${totalDeleted} old object(s) across ${cleanedNames.length} project(s)`
  );
  if (!dryRun) {
    console.log(
      "old paths are gone. Recovery only via GCS soft-delete (within the bucket's soft-delete window)."
    );
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error("fatal:", err);
    process.exit(1);
  });
}

module.exports = { parseArgs };
