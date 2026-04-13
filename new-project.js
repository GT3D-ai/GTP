const { Storage } = require("@google-cloud/storage");

const BUCKETS = [
  "gt-platform-360-photos-bucket",
  "gt-platform-image-storage",
  "gt-platform-model-storage",
];

async function createProject(projectName) {
  if (!projectName) {
    console.error("Usage: node new-project.js <project-name>");
    process.exit(1);
  }

  const storage = new Storage();

  for (const bucketName of BUCKETS) {
    const bucket = storage.bucket(bucketName);
    const filePath = `${projectName}/`;
    const file = bucket.file(filePath);

    try {
      await file.save("", { contentType: "application/x-directory" });
      console.log(`Created: gs://${bucketName}/${filePath}`);
    } catch (err) {
      console.error(`Failed: gs://${bucketName}/${filePath} — ${err.message}`);
    }
  }

  console.log(`\nProject "${projectName}" created in all buckets.`);
}

createProject(process.argv[2]);
