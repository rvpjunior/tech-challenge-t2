# Tech Challenge Term 2

## Setup

1. Ensure that you have Node.js v20 installed.
2. Install the dependencies by running `npm install`.
3. Set environment variables for AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_REGION`).

## Scripts

### Extracting B3 Data to a Parquet File

To extract B3 data to a parquet file, run the following command:

```sh
npm run extract-data <date>
```

Replace `<date>` with the desired date in the format `YYYY-MM-DD`.

The generated parquet file will be located in the `/outputs` folder.

### Upload the Parquet File to S3

To upload the parquet file to S3, run the following command:

```sh
npm run upload-data <file-path> <date>
```

Replace `<date>` with the date of the file in the format `YYYY-MM-DD` and `<file-path>` with the path to the file.