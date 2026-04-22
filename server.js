const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { Storage } = require("@google-cloud/storage");
const { generateThumbnail, generateThumbnailFromGCS, getThumbPath, deleteThumbnail } = require("./thumbnail");

const BUCKET_NAME = "gt-platform-360-photos-bucket";
const PORT = process.env.PORT || 3000;

const app = express();
const upload = multer({ dest: path.join(__dirname, "tmp") });
const storage = new Storage();
const bucket = storage.bucket(BUCKET_NAME);

const IMAGE_BUCKET_NAME = "gt_platform_image_storage";
const imageBucket = storage.bucket(IMAGE_BUCKET_NAME);

app.use(express.static(path.join(__dirname, "public")));

// Backward-compat: /projects.html and /projects → / (index is now projects)
app.get(["/projects", "/projects.html"], (req, res) => {
  res.redirect("/");
});

// Pretty URL for map viewer: /map-viewer/<project-name>
app.get("/map-viewer/:project", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "map-viewer.html"));
});

// List top-level project folders
app.get("/api/projects", async (req, res) => {
  try {
    const [, , apiResponse] = await bucket.getFiles({ delimiter: "/", autoPaginate: false });
    const prefixes = apiResponse.prefixes || [];
    const projects = prefixes.map((p) => p.replace(/\/$/, "")).filter((p) => p !== "_thumbs");
    res.json(projects);
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
    const levels = prefixes.map((p) => p.replace(project + "/", "").replace(/\/$/, "")).filter((l) => l !== "_thumbs");
    res.json(levels);
  } catch (err) {
    console.error("List levels error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Create a new project with default levels + metadata
const DEFAULT_LEVELS = ["level 0", "level 1", "level 2", "level 3", "attic", "garage", "exterior"];

app.post("/api/create-project", upload.single("coverPhoto"), async (req, res) => {
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
app.post("/api/update-project", upload.single("coverPhoto"), async (req, res) => {
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
app.use(express.json());
app.post("/api/create-level", async (req, res) => {
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
app.post("/api/assign-level", async (req, res) => {
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
// Client sends { bucket: "360"|"2d", project, fileName, contentType, level? }
// Returns { uploadUrl, gcsPath }. The browser then PUTs the file bytes directly
// to uploadUrl with the matching Content-Type, bypassing the Cloud Run 32 MiB cap.
app.post("/api/upload-url", async (req, res) => {
  const { bucket: bucketKind, project, fileName, contentType, level } = req.body || {};
  if (!bucketKind || !fileName || !contentType) {
    return res.status(400).json({ error: "bucket, fileName, and contentType are required" });
  }
  const target = bucketKind === "2d" ? imageBucket : bucket;

  let dest = fileName;
  if (project && level) dest = `${project}/${level}/${fileName}`;
  else if (project) dest = `${project}/${fileName}`;

  try {
    const [url] = await target.file(dest).getSignedUrl({
      version: "v4",
      action: "write",
      expires: Date.now() + 15 * 60 * 1000, // 15 minutes
      contentType,
    });
    res.json({
      uploadUrl: url,
      gcsPath: dest,
      bucket: bucketKind === "2d" ? IMAGE_BUCKET_NAME : BUCKET_NAME,
    });
  } catch (err) {
    console.error("Signed URL error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Upload file, optionally into a project folder
// Single file upload
app.post("/api/upload", upload.single("file"), async (req, res) => {
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
app.post("/api/upload-multiple", upload.array("files", 50), async (req, res) => {
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
app.post("/api/delete", async (req, res) => {
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
app.post("/api/generate-thumbnails", async (req, res) => {
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

app.post("/api/mappings", async (req, res) => {
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
});

// ---- 2D Image endpoints (gt_platform_image_storage) ----

// List 2D images in a project
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

// Upload 2D image
app.post("/api/2d/upload", upload.single("file"), async (req, res) => {
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
app.post("/api/2d/delete", async (req, res) => {
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

// Proxy 2D image from GCS
app.get("/api/2d/image", async (req, res) => {
  try {
    const filePath = req.query.file;
    const file = imageBucket.file(filePath);
    const [exists] = await file.exists();
    if (!exists) {
      return res.status(404).send("File not found");
    }
    const [metadata] = await file.getMetadata();
    res.set("Content-Type", metadata.contentType || "application/octet-stream");
    file.createReadStream().pipe(res);
  } catch (err) {
    console.error("2D image proxy error:", err.message);
    res.status(500).send(err.message);
  }
});

app.listen(PORT, () => {
  console.log(`GCS Uploader running on port ${PORT}`);
});
