const sharp = require("sharp");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { Storage } = require("@google-cloud/storage");

const BUCKET_NAME = "gt-platform-360-photos-bucket";
const THUMB_PREFIX = "_thumbs/";
const THUMB_SIZE = 120;

const storage = new Storage();
const bucket = storage.bucket(BUCKET_NAME);

function getThumbPath(gcsFilePath) {
  const ext = path.extname(gcsFilePath);
  const base = ext ? gcsFilePath.slice(0, -ext.length) : gcsFilePath;
  return THUMB_PREFIX + base + ".jpg";
}

async function generateThumbnail(localFilePath, gcsDestPath) {
  const thumbPath = getThumbPath(gcsDestPath);
  const buffer = await sharp(localFilePath)
    .resize(THUMB_SIZE, THUMB_SIZE, { fit: "cover" })
    .jpeg({ quality: 80 })
    .toBuffer();

  const file = bucket.file(thumbPath);
  await file.save(buffer, { contentType: "image/jpeg" });
  console.log(`Thumbnail created: gs://${BUCKET_NAME}/${thumbPath}`);
  return thumbPath;
}

async function generateThumbnailFromGCS(gcsFilePath) {
  // Skip non-image files
  if (!/\.(jpg|jpeg|png|webp|tiff?)$/i.test(gcsFilePath)) return null;

  const thumbPath = getThumbPath(gcsFilePath);
  const thumbFile = bucket.file(thumbPath);
  const [exists] = await thumbFile.exists();
  if (exists) return null; // already has a thumbnail

  const tmpFile = path.join(os.tmpdir(), "thumb_" + Date.now() + "_" + path.basename(gcsFilePath));
  try {
    await bucket.file(gcsFilePath).download({ destination: tmpFile });
    return await generateThumbnail(tmpFile, gcsFilePath);
  } finally {
    fs.unlink(tmpFile, () => {});
  }
}

async function deleteThumbnail(gcsFilePath) {
  const thumbPath = getThumbPath(gcsFilePath);
  const file = bucket.file(thumbPath);
  const [exists] = await file.exists();
  if (exists) {
    await file.delete();
    console.log(`Thumbnail deleted: gs://${BUCKET_NAME}/${thumbPath}`);
  }
}

module.exports = { generateThumbnail, generateThumbnailFromGCS, getThumbPath, deleteThumbnail };
