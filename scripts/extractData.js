import fetch from "node-fetch";
import parquet from "parquetjs-lite";

const appendRecordsToParquetFile = async (parquetWriter, records) => {
  for (const record of records) {
    await parquetWriter.appendRow({
      date: record[0],
      trackerSymbol: record[1],
      isin: record[2],
      segment: record[3],
      minPrice: record[4],
      maxPrice: record[5],
      averagePrice: record[6],
      finalPrice: record[7],
      oscillation: record[8],
      priceAdjustment: record[9],
      taxAdjustment: record[10],
      referencePrice: record[11],
      tradeCount: record[12],
      contractTradedCount: record[13],
      financialVolume: record[14],
    });
  }
};

const fetchCSV = async (date) => {
  const token = await fetchDownloadToken(date);

  const url = `https://arquivos.b3.com.br/api/download/?token=${token}`;

  const response = await fetch(url);

  if (!response.ok) {
    console.error("Error fetching CSV from B3");
    process.exit(1);
  }

  const data = await response.text();

  return data;
};

const fetchDownloadToken = async (date) => {
  const url = `https://arquivos.b3.com.br/api/download/requestname?fileName=TradeInformationConsolidatedFile&date=${date}`;

  const response = await fetch(url);
  if (!response.ok) {
    console.error("Error fetching download path from B3");
    process.exit(1);
  }

  const data = await response.json();

  return data.token;
};

const fetchRecords = async (date) => {
  const dataCSV = await fetchCSV(date);

  const records = dataCSV.split("\n").map((line) => line.split(";"));

  return records.slice(2);
};

const fetchAndWriteData = async (date) => {
  try {
    const parquetSchema = new parquet.ParquetSchema({
      date: { type: "UTF8", optional: true },
      trackerSymbol: { type: "UTF8", optional: true },
      isin: { type: "UTF8", optional: true },
      segment: { type: "UTF8", optional: true },
      minPrice: { type: "UTF8", optional: true },
      maxPrice: { type: "UTF8", optional: true },
      averagePrice: { type: "UTF8", optional: true },
      ByteLengthQueuingStrategyPrice: { type: "UTF8", optional: true },
      oscillation: { type: "UTF8", optional: true },
      priceAdjustment: { type: "UTF8", optional: true },
      taxAdjustment: { type: "UTF8", optional: true },
      referencePrice: { type: "UTF8", optional: true },
      tradeCount: { type: "UTF8", optional: true },
      contractTradedCount: { type: "UTF8", optional: true },
      financialVolume: { type: "UTF8", optional: true },
    });

    const parquetFilePath = `outputs/output_${date}.parquet`;

    const parquetWriter = await parquet.ParquetWriter.openFile(
      parquetSchema,
      parquetFilePath
    );

    const records = await fetchRecords(date);

    await appendRecordsToParquetFile(parquetWriter, records);
    await parquetWriter.close();
    console.info("Data fetched and written to output.parquet");
  } catch (error) {
    console.log(error);
    console.error("Error fetching data from B3");
    process.exit(1);
  }
};

// Note: Skipping the first two arguments (node and script path)
const args = process.argv.slice(2);
const [date] = args;

if (!date) {
  console.error("Please provide a date in the format YYYY-MM-DD");
  process.exit(1);
}

if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
  console.error("Invalid date format. Please use YYYY-MM-DD");
  process.exit(1);
}

console.info(`Fetching data from B3 from ${date}`);
fetchAndWriteData(date);
