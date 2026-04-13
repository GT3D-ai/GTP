const { Storage } = require("@google-cloud/storage");
const path = require("path");
const fs = require("fs");

const BUCKET_NAME = "gt-platform-360-photos-bucket";

async function uploadFile(filePath) {
  if (!filePath) {
    console.error("Usage: node upload.js <file-path>");
    process.exit(1);
  }

  const resolvedPath = path.resolve(filePath);

  if (!fs.existsSync(resolvedPath)) {
    console.error(`File not found: ${resolvedPath}`);
    process.exit(1);
  }

  const fileName = path.basename(resolvedPath);
  const fileSize = fs.statSync(resolvedPath).size;
  const storage = new Storage();
  const bucket = storage.bucket(BUCKET_NAME);

  console.log(`Uploading "${fileName}" (${(fileSize / 1024 / 1024).toFixed(2)} MB) to gs://${BUCKET_NAME}/`);

  const file = bucket.file(fileName);
  const readStream = fs.createReadStream(resolvedPath);
  const writeStream = file.createWriteStream({
    resumable: true,
    metadata: {
      contentType: "application/octet-stream",
    },
  });

  let uploaded = 0;

  readStream.on("data", (chunk) => {
    uploaded += chunk.length;
    const percent = ((uploaded / fileSize) * 100).toFixed(1);
    process.stdout.write(`\rProgress: ${percent}% (${(uploaded / 1024 / 1024).toFixed(2)} / ${(fileSize / 1024 / 1024).toFixed(2)} MB)`);
  });

  await new Promise((resolve, reject) => {
    readStream
      .pipe(writeStream)
      .on("error", (err) => {
        console.error("\nUpload failed:", err.message);
        reject(err);
      })
      .on("finish", () => {
        console.log(`\nUpload complete: gs://${BUCKET_NAME}/${fileName}`);
        resolve();
      });
  });
}

uploadFile(process.argv[2]);
