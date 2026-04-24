const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { Storage } = require("@google-cloud/storage");
const { generateThumbnail, generateThumbnailFromGCS, getThumbPath, deleteThumbnail } = require("./thumbnail");
const createUserService = require("./user-service");

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

const userService = createUserService({ bucket });

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

// Backward-compat: /projects.html and /projects → / (index is now projects)
app.get(["/projects", "/projects.html"], (req, res) => {
  res.redirect("/");
});

// Pretty URL for map viewer: /map-viewer/<project-name>
app.get("/map-viewer/:project", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "map-viewer.html"));
});

// Public, per-project models showcase: /models/<project-name>
app.get("/models/:project", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "project-models.html"));
});

// Public, per-project plans showcase: /plans/<project-name>
app.get("/plans/:project", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "project-plans.html"));
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

function requireProjectRole(minRole) {
  return async (req, res, next) => {
    if (!req.user) return res.status(401).json({ error: "Authentication required" });
    if (req.user.isAdmin) return next();
    const project = resolveProjectFromRequest(req);
    if (!project) return res.status(400).json({ error: "project required" });
    if (project === "_thumbs" || project === "_platform") {
      return res.status(403).json({ error: "Access denied" });
    }
    const ok = await userService.hasProjectAccess(req.user.email, project, minRole);
    if (!ok) return res.status(403).json({ error: `${minRole} access to ${project} required` });
    req.projectName = project;
    next();
  };
}

// All /api/* routes go through resolveUser
app.use("/api", resolveUser);

// List top-level project folders, filtered by what the current user can access.
// Anonymous callers get [] — they don't enumerate projects, they arrive on a
// specific /map-viewer/<project> URL and ask for scoped data directly.
app.get("/api/projects", async (req, res) => {
  try {
    if (!req.user) return res.json([]);
    const [, , apiResponse] = await bucket.getFiles({ delimiter: "/", autoPaginate: false });
    const prefixes = apiResponse.prefixes || [];
    const allProjects = prefixes
      .map((p) => p.replace(/\/$/, ""))
      .filter((p) => p !== "_thumbs" && p !== "_platform");
    const accessible = await userService.accessibleProjects(req.user.email, allProjects);
    res.json(accessible);
  } catch (err) {
    console.error("List projects error:", err.message);
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

    // Create default level folders
    for (const lvl of DEFAULT_LEVELS) {
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

    console.log(`Created project: ${name} (with ${DEFAULT_LEVELS.length} levels)`);
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
  if (bucketKind === "model" || bucketKind === "plan") {
    if (!req.user?.isAdmin) return res.status(403).json({ error: `Admin required for ${bucketKind} uploads` });
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
  else { target = bucket; bucketName = BUCKET_NAME; }

  let dest = fileName;
  if (bucketKind === "plan") {
    // Plans live under a reserved _plans/ prefix so they don't leak into the
    // regular 2D images list. The caller passes fileName without the prefix.
    dest = `${project}/_plans/${fileName}`;
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
    const file = bucket.file(`${project}/mappings.json`);
    const [exists] = await file.exists();
    if (!exists) return res.json({ floorPlanImage: null, pins: [] });
    const [content] = await file.download();
    res.json(JSON.parse(content.toString()));
  } catch (err) {
    console.error("Get mappings error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Save mappings — deliberately NOT at /api/mappings. The LB sends the
// /api/mappings path to the public (no-IAP) backend so the public map-viewer
// can read pins anonymously; saves live at a separate path that routes through
// the private (IAP-protected) backend.
const saveMappingsHandler = async (req, res) => {
  const { project, data } = req.body;
  if (!project || !data) return res.status(400).json({ error: "project and data are required" });
  try {
    const file = bucket.file(`${project}/mappings.json`);
    await file.save(JSON.stringify(data, null, 2), { contentType: "application/json" });
    console.log(`Saved mappings: gs://${BUCKET_NAME}/${project}/mappings.json`);
    res.json({ success: true });
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
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/")) // plans are managed separately
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
    const list = files
      .filter((f) => !f.name.endsWith("/"))
      .filter((f) => !f.name.includes("/_plans/"))
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

// ---- Model file endpoints (gt_platform_model_storage) ----

// List models in a project — PUBLIC (anyone with the project name can see).
// Convention: a model at project/foo.glb may have a sibling thumbnail at
// project/foo.glb.thumb.jpg (any image type, but we always store with the
// .thumb.jpg suffix on the model's filename). Thumbnails are hidden from the
// main list and surfaced as the `thumbnail` field on the associated model.
app.get("/api/model/files", async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: "project is required" });
    const prefix = project + "/";
    const [files] = await modelBucket.getFiles({ prefix });
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

    res.json({ success: true });
  } catch (err) {
    console.error("Model delete error:", err.message);
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

// Public per-project home page at /<project-name>. Placed LAST so all static
// files, pretty URLs (/map-viewer, /models, /plans), and /api/* routes take
// priority. Anything with a "." in the path is treated as a filename and
// falls through to 404 (so the static handler already tried it).
app.get("/:project", (req, res, next) => {
  const project = req.params.project;
  if (!project || project.includes(".") || project.startsWith("_")) return next();
  // Reserved top-level words that are not projects
  const reserved = new Set([
    "api", "map-viewer", "models", "plans", "projects", "robots.txt",
    "tokens.css", "app.css", "me.js",
  ]);
  if (reserved.has(project)) return next();
  res.sendFile(path.join(__dirname, "public", "project-home.html"));
});

app.listen(PORT, () => {
  console.log(`GCS Uploader running on port ${PORT}`);
});
