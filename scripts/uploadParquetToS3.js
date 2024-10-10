import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs";

const uploadFile = async (filePath, date) => {
  try {
    const fileContent = fs.readFileSync(filePath);
    const s3Client = new S3Client({ region: "us-east-1" });
    const uploadParams = {
      Bucket: "tech-challenge-b3",
      Key: `date=${date}/data.parquet`,
      Body: fileContent,
    };
    await s3Client.send(new PutObjectCommand(uploadParams));
    console.log("File uploaded successfully");
  } catch (error) {
    console.error("Error uploading file to S3");
    console.error(error);
  }
};

// Note: Skipping the first two arguments (node and script path)
const args = process.argv.slice(2);
const [filePath, date] = args;

if (!filePath || !date) {
  console.error("Please provide a file path and a date as arguments.");
  process.exit(1);
}

if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
  console.error("Invalid date. Please use a valid date format (YYYY-MM-DD).");
  process.exit(1);
}

uploadFile(filePath, date);
