// index.js
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');
const axios = require('axios');
const FormData = require('form-data');
const { google } = require('googleapis');
const sharp = require('sharp');

// === chống Google 429: retry 5 lần ===
async function fetchPdfWithRetry(url, headers, attempt = 1) {
  try {
    return await axios.get(url, {
      responseType: 'arraybuffer',
      headers,
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    });
  } catch (err) {
    if (err.response && err.response.status === 429 && attempt < 5) {
      const delay = 3000 + Math.floor(Math.random() * 3000);
      console.log(`⚠️ Google 429 — retry ${attempt}/5 after ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
      return fetchPdfWithRetry(url, headers, attempt + 1);
    }
    throw err;
  }
}

// Convert PDF → PNG + trim
function convertPdfToPng(pdfPath, outPrefix) {
  return new Promise((resolve, reject) => {
    execFile('pdftoppm', ['-png', '-singlefile', '-r', '150', pdfPath, outPrefix], async (err) => {
      if (err) return reject(err);
      const pngPath = outPrefix + '.png';
      if (!fs.existsSync(pngPath)) return reject(new Error('PNG conversion failed'));

      try {
        const img = sharp(pngPath);
        const trimmedBuffer = await img.trim().toBuffer();
        await fs.promises.writeFile(pngPath, trimmedBuffer);
        resolve(pngPath);
      } catch (e) {
        reject(e);
      }
    });
  });
}

async function main() {
  try {
    const serviceAccountJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
    const SHEET_NAMES = (process.env.SHEET_NAMES || 'Ladi,Mydu').split(',').map(s => s.trim()).filter(Boolean);
    const START_COL = process.env.START_COL || 'F';
    const END_COL = process.env.END_COL || 'AD';
    const MAX_ROWS_PER_FILE = Number(process.env.MAX_ROWS_PER_FILE || '40');
    const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
    const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

    if (!serviceAccountJson || !SPREADSHEET_ID || !TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
      throw new Error('Missing required environment variables');
    }

    const creds = JSON.parse(serviceAccountJson);

    // === Authorize ===
    const jwtClient = new google.auth.JWT(
      creds.client_email,
      null,
      creds.private_key,
      ['https://www.googleapis.com/auth/drive.readonly','https://www.googleapis.com/auth/spreadsheets.readonly'],
      null
    );
    await jwtClient.authorize();
    const tokenObj = await jwtClient.getAccessToken();
    const accessToken = tokenObj?.token;
    if (!accessToken) throw new Error('Failed to obtain access token');

    const sheetsApi = google.sheets({ version: 'v4', auth: jwtClient });

    // === TMP DIR ===
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sheetpdf-'));

    // ============================
    //   ✅ GỌI METADATA 1 LẦN
    // ============================
    const metadata = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const allSheets = metadata.data.sheets || [];
    console.log(`📄 Loaded metadata for ${allSheets.length} sheets`);

    // ============================
    //   XỬ LÝ TỪNG SHEET
    // ============================
    for (const sheetName of SHEET_NAMES) {
      console.log('--- Processing sheet:', sheetName);

      // tìm sheetInfo từ metadata đã cache
      const sheetInfo = allSheets.find(s => s.properties?.title === sheetName);
      if (!sheetInfo) { console.log(`⚠️ Sheet "${sheetName}" not found — skipping`); continue; }
      const gid = sheetInfo.properties.sheetId;

      // Lấy F5:K6
      const rangeRes = await sheetsApi.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${sheetName}!F5:K6`
      });
      const values = rangeRes.data.values || [];
      const f5 = values[0]?.[0] || '';
      const j5 = values[0]?.[4] || '';
      const k5 = values[0]?.[5] || '';
      const k6 = values[1]?.[5] || '';
      if (!k6) { console.log(`⚠️ Sheet "${sheetName}" K6 trống — bỏ qua`); continue; }
      const captionText = `${f5}    ${j5}    ${k5}`;

      // Last row col K
      const colRes = await sheetsApi.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${sheetName}!K1:K2000`
      });
      const colVals = colRes.data.values || [];
      let lastRow = 1;
      for (let i = colVals.length - 1; i >= 0; i--) {
        if (colVals[i]?.[0]) { lastRow = i + 1; break; }
      }
      console.log('Last row detected (col K):', lastRow);

      // build chunks
      let chunks = [];
      let startRow = 1;
      while (startRow <= lastRow) {
        const endRow = Math.min(startRow + MAX_ROWS_PER_FILE - 1, lastRow);
        chunks.push({ startRow, endRow });
        startRow = endRow + 1;
      }
      if (chunks.length > 1) {
        const lastChunk = chunks[chunks.length - 1];
        const sz = lastChunk.endRow - lastChunk.startRow + 1;
        if (sz < 9) {
          chunks[chunks.length - 2].endRow = lastChunk.endRow;
          chunks.pop();
          console.log(`⚡ Gộp chunk cuối nhỏ (${sz} rows)`);
        }
      }

      // Export từng chunk
      const albumImages = [];

      for (const chunk of chunks) {
        const rangeParam = `${sheetName}!${START_COL}${chunk.startRow}:${END_COL}${chunk.endRow}`;
        const exportUrl =
          `https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/export?format=pdf` +
          `&portrait=false&size=A4&fitw=true&sheetnames=false&printtitle=false&pagenumbers=false` +
          `&gridlines=false&fzr=false&gid=${gid}&range=${encodeURIComponent(rangeParam)}`;

        console.log(`➡ Export PDF for ${sheetName} rows ${chunk.startRow}-${chunk.endRow}`);
        const pdfResp = await fetchPdfWithRetry(exportUrl, { Authorization: `Bearer ${accessToken}` });

        const pdfName = `${sheetName}_${chunk.startRow}-${chunk.endRow}.pdf`;
        const pdfPath = path.join(tmpDir, pdfName);
        fs.writeFileSync(pdfPath, Buffer.from(pdfResp.data));

        const outPrefix = path.join(tmpDir, path.basename(pdfName, '.pdf'));
        const pngPath = await convertPdfToPng(pdfPath, outPrefix);

        // Delay chunk nhỏ
        const sz = chunk.endRow - chunk.startRow + 1;
        if (sz < MAX_ROWS_PER_FILE) {
          const extra = 500 + (MAX_ROWS_PER_FILE - sz) * 100;
          console.log(`⏱ Delay thêm ${extra}ms cho chunk nhỏ (${sz} rows)`);
          await new Promise(r => setTimeout(r, extra));
        }

        albumImages.push({
          path: pngPath,
          fileName: path.basename(pngPath)
        });
      }

      // SEND ALBUM
      console.log(`📤 Sending ALBUM for sheet ${sheetName}`);
      const formAlbum = new FormData();
      formAlbum.append('chat_id', TELEGRAM_CHAT_ID);

      const media = albumImages.map((img, i) => ({
        type: "photo",
        media: `attach://${img.fileName}`,
        caption: i === 0 ? captionText : undefined
      }));
      formAlbum.append('media', JSON.stringify(media));
      albumImages.forEach(img => formAlbum.append(img.fileName, fs.createReadStream(img.path)));

      const tgResp = await axios.post(
        `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMediaGroup`,
        formAlbum,
        { headers: formAlbum.getHeaders() }
      );
      console.log('📸 Album result:', tgResp.data);

      // Cleanup
      albumImages.forEach(img => {
        try { fs.unlinkSync(img.path); } catch {}
        try {
          const pdfPath = path.join(tmpDir, img.fileName.replace('.png', '.pdf'));
          fs.unlinkSync(pdfPath);
        } catch {}
      });
    }

    // Cleanup temp folder
    fs.rmSync(tmpDir, { recursive: true, force: true });
    console.log('🎉 All sheets processed successfully');
  } catch (err) {
    console.error('ERROR:', err?.message || err);
    process.exit(1);
  }
}

main();
