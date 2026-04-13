const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { Storage } = require("@google-cloud/storage");

const BUCKET_NAME = "gt-platform-360-photos-bucket";
const PORT = 3000;

const app = express();
const upload = multer({ dest: path.join(__dirname, "tmp") });
const storage = new Storage();
const bucket = storage.bucket(BUCKET_NAME);

const IMAGE_BUCKET_NAME = "gt-platform-image-storage";
const imageBucket = storage.bucket(IMAGE_BUCKET_NAME);

app.use(express.static(path.join(__dirname, "public")));

// Redirect root to projects page
app.get("/", (req, res) => {
  res.redirect("/projects.html");
});

// List top-level project folders
app.get("/api/projects", async (req, res) => {
  try {
    const [, , apiResponse] = await bucket.getFiles({ delimiter: "/", autoPaginate: false });
    const prefixes = apiResponse.prefixes || [];
    const projects = prefixes.map((p) => p.replace(/\/$/, ""));
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
    const levels = prefixes.map((p) => p.replace(project + "/", "").replace(/\/$/, ""));
    res.json(levels);
  } catch (err) {
    console.error("List levels error:", err.message);
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

// Move a file to a level (copy + delete)
app.post("/api/assign-level", async (req, res) => {
  const { file: srcPath, project, level } = req.body;
  if (!srcPath || !project || !level) {
    return res.status(400).json({ error: "file, project, and level are required" });
  }
  try {
    const fileName = srcPath.split("/").pop();
    const destPath = `${project}/${level}/${fileName}`;
    const srcFile = bucket.file(srcPath);
    const destFile = bucket.file(destPath);
    await srcFile.copy(destFile);
    await srcFile.delete();
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
        .filter((f) => !f.name.endsWith("/"))
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
      .filter((f) => !f.name.endsWith("/")) // exclude folder placeholders
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

// ---- 2D Image endpoints (gt-platform-image-storage) ----

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
  console.log(`GCS Uploader running at http://localhost:${PORT}`);
});
