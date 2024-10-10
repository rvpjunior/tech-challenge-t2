import fetch from "node-fetch";
import parquet from "parquetjs-lite";

const toFloat = (value) => {
  return value !== "" && value !== undefined
    ? parseFloat(value.replace(",", "."))
    : undefined;
};

const toInt = (value) => {
  return value !== "" && value !== undefined ? parseInt(value) : undefined;
};

const appendRecordsToParquetFile = async (parquetWriter, records) => {
  for (const record of records) {
    await parquetWriter.appendRow({
      rptDt: record[0],
      tckrSymb: record[1],
      isin: record[2],
      sgmtNm: record[3],
      minPric: toFloat(record[4]),
      maxPric: toFloat(record[5]),
      tradAvrgPric: toFloat(record[6]),
      lastPric: toFloat(record[7]),
      oscnPctg: toFloat(record[8]),
      adjstdQt: toFloat(record[9]),
      adjstdQtTax: toFloat(record[10]),
      refPric: toFloat(record[11]),
      tradQty: toInt(record[12]),
      finInstrmQty: toInt(record[13]),
      ntlFinVol: toFloat(record[14]),
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

  return records.slice(2).filter((record) => record.length > 1);
};

const fetchAndWriteData = async (date) => {
  try {
    const parquetSchema = new parquet.ParquetSchema({
      rptDt: { type: "UTF8" },
      tckrSymb: { type: "UTF8" },
      isin: { type: "UTF8" },
      sgmtNm: { type: "UTF8" },
      minPric: { type: "FLOAT", optional: true },
      maxPric: { type: "FLOAT", optional: true },
      tradAvrgPric: { type: "FLOAT", optional: true },
      lastPric: { type: "FLOAT", optional: true },
      oscnPctg: { type: "FLOAT", optional: true },
      adjstdQt: { type: "FLOAT", optional: true },
      adjstdQtTax: { type: "FLOAT", optional: true },
      refPric: { type: "FLOAT", optional: true },
      tradQty: { type: "INT64", optional: true },
      finInstrmQty: { type: "INT64", optional: true },
      ntlFinVol: { type: "FLOAT", optional: true },
    });

    const parquetFilePath = `outputs/output_${date}.parquet`;

    const parquetWriter = await parquet.ParquetWriter.openFile(
      parquetSchema,
      parquetFilePath
    );

    const records = await fetchRecords(date);

    await appendRecordsToParquetFile(parquetWriter, records);
    await parquetWriter.close();
    console.info(`Data fetched and written to ${parquetFilePath}`);
  } catch (error) {
    console.error("Error fetching data from B3");
    console.log(error);
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
