// Maps a URL slug (`req.params.project`) to a project's physical
// storage location. Single chokepoint for path resolution: route
// handlers ask the middleware for `req.paths.*` and never compute
// `${projectName}/_plans/...` themselves. Also owns writes to the
// slug index so registrations, renames, and migrations stay
// consistent with the resolver's view of the world.

const SLUG_INDEX_PATH = "_platform/slug-index.json";
const CACHE_TTL_MS = 60_000;

// Conservative kebab-case: lowercase, alphanumerics + dashes only,
// collapsed runs, no leading/trailing dashes. Matches what the new
// Property model uses for compound slugs (e.g. "123-main-st--ec-2026-04-29").
function slugify(s) {
  return String(s)
    .trim()
    .toLowerCase()
    .replace(/['"`]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function emptyIndex() {
  return { version: 0, alias: {}, canonical: {} };
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

module.exports = function createProjectResolver({ bucket }) {
  let cache = { data: null, fetchedAt: 0 };

  // Serialize index mutations through a promise chain so two concurrent
  // writes within one process can't clobber each other. Multi-instance
  // deployments need GCS generation-match preconditions instead — TODO
  // when we scale past one Cloud Run instance.
  let writeQueue = Promise.resolve();

  async function loadIndex() {
    if (cache.data && Date.now() - cache.fetchedAt < CACHE_TTL_MS) {
      return cache.data;
    }
    const data = (await readJson(bucket, SLUG_INDEX_PATH)) || emptyIndex();
    cache = { data, fetchedAt: Date.now() };
    return data;
  }

  function invalidateIndex() {
    cache = { data: null, fetchedAt: 0 };
  }

  function pathsFor(ref) {
    const base =
      ref.layout === "new" ? `${ref.propertyId}/${ref.projectId}` : ref.name;
    return {
      base,
      meta: `${base}/project.json`,
      plans: `${base}/_plans/`,
      documents: `${base}/_documents/`,
      videos: `${base}/_videos/`,
      thumbs: `${base}/_thumbs/`,
      mappings: `${base}/mappings.json`,
      obj: (rel) => `${base}/${rel}`,
    };
  }

  async function resolveProject(slugFromUrl) {
    if (!slugFromUrl) return null;
    const idx = await loadIndex();
    const canonicalSlug = idx.alias[slugFromUrl] || slugFromUrl;
    const ref = idx.canonical[canonicalSlug];
    if (!ref) return null;

    const paths = pathsFor(ref);
    const meta = await readJson(bucket, paths.meta);
    if (!meta) return null;

    return {
      ...meta,
      canonicalSlug,
      requestedSlug: slugFromUrl,
      isAlias: canonicalSlug !== slugFromUrl,
      layout: ref.layout,
      paths,
    };
  }

  // Express middleware. `redirectAliases: false` for API routes that
  // should resolve under the old slug without bouncing the client.
  function withProject(opts = {}) {
    const redirectAliases = opts.redirectAliases !== false;
    return async function (req, res, next) {
      try {
        const project = await resolveProject(req.params.project);
        if (!project) return res.status(404).send("Project not found");

        if (redirectAliases && project.isAlias && req.method === "GET") {
          const oldSeg = `/${encodeURIComponent(project.requestedSlug)}`;
          const newSeg = `/${encodeURIComponent(project.canonicalSlug)}`;
          const newUrl = req.originalUrl.replace(oldSeg, newSeg);
          if (newUrl !== req.originalUrl) {
            return res.redirect(301, newUrl);
          }
        }

        req.project = project;
        req.paths = project.paths;
        next();
      } catch (err) {
        next(err);
      }
    };
  }

  function mutateIndex(fn) {
    const next = writeQueue.then(async () => {
      const idx = (await readJson(bucket, SLUG_INDEX_PATH)) || emptyIndex();
      await fn(idx);
      idx.version = (idx.version || 0) + 1;
      await writeJson(bucket, SLUG_INDEX_PATH, idx);
      invalidateIndex();
      return idx;
    });
    // Swallow rejections on the queue so one failed mutation doesn't
    // poison every subsequent caller — each caller still sees its own
    // rejection via the returned `next` promise.
    writeQueue = next.catch(() => {});
    return next;
  }

  async function registerNewProject({ propertyId, projectId, compoundSlug }) {
    return mutateIndex((idx) => {
      idx.canonical[compoundSlug] = { layout: "new", propertyId, projectId };
    });
  }

  async function renameProject({ oldSlug, newSlug }) {
    return mutateIndex((idx) => {
      const ref = idx.canonical[oldSlug];
      if (!ref) throw new Error(`renameProject: ${oldSlug} not in index`);
      idx.canonical[newSlug] = ref;
      delete idx.canonical[oldSlug];
      idx.alias[oldSlug] = newSlug;
    });
  }

  async function recordMigration({
    oldName,
    propertyId,
    projectId,
    compoundSlug,
  }) {
    return mutateIndex((idx) => {
      delete idx.canonical[oldName];
      idx.canonical[compoundSlug] = { layout: "new", propertyId, projectId };
      idx.alias[oldName] = compoundSlug;
      // Also alias the kebab form in case anything links by slugified name.
      const oldSlug = slugify(oldName);
      if (oldSlug && oldSlug !== oldName) idx.alias[oldSlug] = compoundSlug;
    });
  }

  // Phase-1 indexing: register every existing project as a `layout: "old"`
  // entry. Idempotent — re-running won't overwrite already-migrated entries
  // and won't duplicate legacy ones.
  async function buildLegacyIndex(projectNames) {
    return mutateIndex((idx) => {
      for (const name of projectNames) {
        if (!idx.canonical[name]) {
          idx.canonical[name] = { layout: "old", name };
        }
      }
    });
  }

  return {
    resolveProject,
    pathsFor,
    withProject,
    invalidateIndex,
    registerNewProject,
    renameProject,
    recordMigration,
    buildLegacyIndex,
    slugify,
  };
};
