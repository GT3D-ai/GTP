# Property migration — handoff state

Snapshot of the multi-phase migration that converted flat `<projectName>/...`
GCS storage into a `Property → Project` model with `<propertyId>/<projectId>/...`
storage. Read this before touching any of: `_platform/slug-index.json`,
`_platform/properties.json`, `_platform/migration-state.json`, the legacy
`<oldName>/...` GCS folders, or the `scripts/migrate-to-properties.js`,
`scripts/switchover.js`, `scripts/cleanup-old-paths.js` scripts.

## Phase status

| Phase | Status | Notes |
|---|---|---|
| 1. Wire-in (resolver + middleware deployed) | ✅ done | live since happy-valley canary |
| 2. Backfill (5 projects migrated) | ✅ done | manifest at `_platform/migration-manifest.jsonl` |
| 3. Switchover (`users.json` keys flipped to compound slugs, `property-order.json` written) | ✅ done **2026-05-01** | snapshot at `_platform/_backup/T0-pre-switchover/` |
| 4. Verification window (≥14 days) | ⏳ active | started 2026-05-01 |
| 5. Cleanup (delete legacy paths, reclaim ~1,800 objects) | ⏳ pending | **earliest 2026-05-15**; `cleanup-old-paths.js` enforces this |

## Architecture in two paragraphs

**Slug-index** (`_platform/slug-index.json`) is the routing source of
truth: a `canonical` map from URL slug → `{layout, propertyId, projectId}`,
plus an `alias` map from old slugs → canonical compound slugs. The
resolver in `project-resolver.js` reads the index, falls through aliases,
then reads `<propertyId>/<projectId>/project.json` (or `<oldName>/project.json`
for `layout: "old"` legacy entries — though we have none of those left
post-Phase-2). Cached in-process with 60s TTL on the index, 30s on
project metadata. Server-side `requireProjectRole` and `/api/me` are
both alias-aware, so users keep access whether `users.json` keys are
the old name or the canonical compound slug.

**Properties** (`_platform/properties.json`) is per-physical-property
metadata: `name`, `slug`, `address`, `coverPhoto`, `projectIds[]`. One
property may host multiple projects (e.g. `prop_bc8fc788af454d38` has
both `happy-valley` and `office-remodel`). The new-project page
(`public/new-project.html`) is mode-aware: picking an existing property
defaults to property-edit mode (`Save Property` submit); clicking
"+ Add a new project to this property" reveals the project section
and switches submit to `Create Project`.

## Live invariants — DO NOT BREAK

1. **Don't delete the legacy `<oldName>/...` GCS folders before 2026-05-15.**
   They still serve as the fallback data tree if anything goes wrong with
   the Phase-2 migration. Phase-5 cleanup script handles this with md5
   verification.
2. **Don't run `node scripts/migrate-to-properties.js --all` again.** All
   5 projects are migrated; the alias-aware idempotency check (commit
   `0ffe1e1`) will skip them, but the script's purpose is exhausted.
3. **Don't hand-edit `_platform/slug-index.json`, `_platform/properties.json`,
   or `_platform/migration-state.json`.** Use the resolver's mutation
   methods (`registerNewProject`, `recordMigration`, etc.) so the
   in-process write queue and version counter stay coherent.
4. **`migration-state.json.phase` is `"switched-over"`.** `cleanup-old-paths.js`
   refuses to run unless this flag has been in place for ≥`min-age-days`
   (default 14). If the file disappears, cleanup will refuse — by design.
5. **Cloud Run cache TTL is 60s.** After any write to `_platform/*.json`,
   expect ≤60s of staleness across instances. Single-instance today, but
   the multi-instance case will surface eventually.

## Key files

### Server / library
- `server.js` — endpoint surface, including `/api/properties`, `/api/property/:id`, `/api/validate-address`, `/api/projects` (slug-index-driven), `/api/me` (alias-expanded), `requireProjectRole` (alias-aware).
- `project-resolver.js` — resolver factory, slug-index reader/writer, `withProject` middleware, `getCanonicalSlug`, `getIndex`, `pathsFor`, mutation methods.
- `user-service.js`, `email-service.js`, `thumbnail.js` — pre-existing modules; no migration-specific changes worth flagging.

### Frontend
- `public/new-project.html` — Property/Project create + edit. Mode-aware (save-property vs create).
- `public/edit-project.html` — Per-project edit page; address writes route to property record for migrated projects.
- `public/projects.html` — Project list; uses slug from `/api/projects` for URLs/perms, `info.name` from `/api/project-info` for display.
- `public/me.js` — `getProjectInfo`, `applyProjectHeadline` (used by 8 pages to show short name not URL slug).
- Category pages (`project-{models,images,plans,pointclouds,documents}.html`) — project name moved from header to body above `.page-head` h1.

### Migration scripts (in `scripts/`, not shipped to Cloud Run)
- `migrate-to-properties.js` — Phase 2. `--dry-run`, `--project=<name>`, `--all --limit=N`, `--rollback=<name>`. Alias-aware idempotency (commit `0ffe1e1`).
- `switchover.js` — Phase 3. `--dry-run`, `--rollback`. Already executed live.
- `cleanup-old-paths.js` — Phase 5. Will refuse to run before the verification window.

## GCS layout reference

| Bucket | Pre-migration | Post-migration |
|---|---|---|
| `gt-platform-360-photos-bucket` (main) | `<projectName>/level X/*.JPG` etc. | `<propertyId>/<projectId>/level X/*.JPG` |
| `gt_platform_image_storage` | `<projectName>/cover.jpg`, `<projectName>/_documents/*` etc. | `<propertyId>/<projectId>/cover.jpg`, `<propertyId>/_property-cover.jpg` |
| `gt_platform_model_storage` | `<projectName>/*` | `<propertyId>/<projectId>/*` |
| `gt_platform_pointcloud_storage` | `<projectName>/*` | `<propertyId>/<projectId>/*` |
| `_thumbs/` (top-level prefix in main bucket) | `_thumbs/<projectName>/...` | `_thumbs/<propertyId>/<projectId>/...` |

Both layouts coexist until Phase 5. The 5 legacy `<oldName>/` folders
are frozen — the resolver routes everything through the new layout via
`canonical[<compound-slug>] → <propertyId>/<projectId>`.

## Open / parked design decisions

- **URL-obscuring** — current slugs include the address (`5179-coronado-ave-oakland-ca-94618--5179-coronado`). User asked about hiding, then deferred. Smallest change is dropping the property prefix entirely (`/<project-slug>` only), with a one-shot backfill of canonical slugs. Two existing canonicals would change; aliases preserve old links.
- **Plans-as-options** — design only; not implemented. Idea is that plans on a remodel project can supersede Existing Conditions plans on the same property after construction. Needs a "supersede" relationship between plans across projects.
- **Property landing page** (`/property/<id>`) — not built. Property data exists; UI doesn't expose it directly.
- **Mobile PDF preview detection** — current modal uses `<iframe>` with the browser's built-in PDF viewer. Works on desktop; iOS Safari typically refuses inline PDF. Fallback hint not implemented.

## Cosmetic to-dos (Save Property UI fixes)

| propertyId | Current name | Suggested fix |
|---|---|---|
| `prop_9339fb5840254f00` | `"500 Treat Ave"` | Address still says `"500 Treat AveSan Francisco, CA 94110"` — fix missing comma |
| `prop_bc8fc788af454d38` | `"Lafayette, CA"` | Address is `"4055 Happy Valley Rd., Lafayette, CA 94549"` — rename to `"4055 Happy Valley Rd"` or similar |
| `prop_5302e587a1b54259`, `prop_2f05d8c9c95d4fd4`, `prop_0f75971f74414aea` | (currently the address) | optional friendlier names ("Schoenberg Guitars", etc.) |

All fixable from `/new-project.html` → pick property → edit fields → Save Property.

## Rollback handles

- **Phase 3 rollback**: `node scripts/switchover.js --rollback`. Restores from `_platform/_backup/T0-pre-switchover/{users,project-order,slug-index}.json`. Sets `migration-state.phase = "backfilled"`.
- **Per-project Phase-2 rollback**: `node scripts/migrate-to-properties.js --rollback=<oldName>`. Reads manifest, deletes new tree across buckets, removes property + slug-index entries, strips `migrating: true` from old project.json.
- **Phase 5 cleanup is irreversible** beyond GCS soft-delete (verify bucket retention before running).

## Recent commits worth knowing

| Commit | What |
|---|---|
| `019c333` | Resolver caching + alias resolution |
| `0cc74cc` | Critical: Express 5 `req.query` is read-only — replace with mutable object |
| `842ee74` | Migration rewrites `mappings.json` internal paths |
| `cdcdd5b` | Migration + cleanup handle `_thumbs/` |
| `5d26812` | Phase-3 prep: alias-aware permissions, slug-index-driven `/api/projects` |
| `0ffe1e1` | `migrate-to-properties` alias-aware idempotency (post-rollback fix) |
| `cc99389` | Switchover pre-flight allows UI-created `layout: "new"` projects |

## What "next" looks like

1. **2026-05-15 onward**: Phase 5 cleanup safe to run.
   `node scripts/cleanup-old-paths.js --dry-run --all` first, then with
   `--all --confirm-irreversible`. Watches for soft-delete window before
   committing destructive deletes.
2. **Anytime**: cosmetic property fixes via the UI; one of the parked
   design decisions if you want to pick one up.
3. **Don't run any migration script before Phase 5** unless creating
   a brand-new legacy project (you wouldn't — the new-project UI creates
   directly into the new layout).
