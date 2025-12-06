// index.js
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');
const axios = require('axios');
const FormData = require('form-data');
const { google } = require('googleapis');
const pLimit = require('p-limit');

// === fetch PDF với retry chống 429 ===
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
      const delay = 2000 * attempt;
      console.log(`⚠️ Google 429 — retry ${attempt}/5 after ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
      return fetchPdfWithRetry(url, headers, attempt + 1);
    }
    throw err;
  }
}

// Promise wrapper cho pdftoppm
function convertPdfToPng(pdfPath, outPrefix) {
  return new Promise((resolve, reject) => {
    execFile('pdftoppm', ['-png', '-singlefile', '-r', '180', pdfPath, outPrefix], (err) => {
      if (err) return reject(err);
      const pngPath = outPrefix + '.png';
      if (!fs.existsSync(pngPath)) return reject(new Error('PNG conversion failed'));
      resolve(pngPath);
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

    // === Authorize Google ===
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

    // === tmpDir chung ===
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sheetpdf-'));

    // limit song song
    const limit = pLimit(4); // tăng concurrency từ 2 -> 4

    for (const sheetName of SHEET_NAMES) {
      console.log('--- Processing sheet:', sheetName);

      const meta = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
      const sheetInfo = (meta.data.sheets || []).find(s => s.properties?.title === sheetName);
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

      // last row col K
      const colRes = await sheetsApi.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!K1:K2000` });
      const colVals = colRes.data.values || [];
      let lastRow = 1;
      for (let i = colVals.length - 1; i >= 0; i--) {
        if (colVals[i]?.[0]) { lastRow = i + 1; break; }
      }
      console.log('Last row detected (col K):', lastRow);

      // --- build chunks ---
      let chunks = [];
      let startRow = 1;
      while (startRow <= lastRow) {
        const endRow = Math.min(startRow + MAX_ROWS_PER_FILE - 1, lastRow);
        chunks.push({ startRow, endRow });
        startRow = endRow + 1;
      }

      // --- 1. Export PDF song song ---
      const pdfResults = await Promise.all(chunks.map(chunk => limit(async () => {
        const rangeParam = `${sheetName}!${START_COL}${chunk.startRow}:${END_COL}${chunk.endRow}`;
        const exportUrl =
          `https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/export?format=pdf` +
          `&portrait=false&size=A4&fitw=true&sheetnames=false&printtitle=false&pagenumbers=false` +
          `&gridlines=false&fzr=false&gid=${gid}&range=${encodeURIComponent(rangeParam)}`;
        console.log(`➡ Export PDF for ${sheetName} rows ${chunk.startRow}-${chunk.endRow}`);
        const pdfResp = await fetchPdfWithRetry(exportUrl, { Authorization: `Bearer ${accessToken}` });
        const pdfPath = path.join(tmpDir, `${sheetName}_${chunk.startRow}-${chunk.endRow}.pdf`);
        fs.writeFileSync(pdfPath, Buffer.from(pdfResp.data));
        return { pdfPath, startRow: chunk.startRow };
      })));

      pdfResults.sort((a,b) => a.startRow - b.startRow);

      // --- 2. Convert PDF → PNG song song ---
      const pngResults = await Promise.all(pdfResults.map(r => limit(async () => {
        const outPrefix = r.pdfPath.replace('.pdf','');
        const pngPath = await convertPdfToPng(r.pdfPath, outPrefix);
        return { pngPath, fileName: path.basename(pngPath), startRow: r.startRow, pdfPath: r.pdfPath };
      })));

      pngResults.sort((a,b) => a.startRow - b.startRow);

      // --- 3. Gửi album Telegram ---
      console.log(`📤 Sending ALBUM for sheet ${sheetName} with ${pngResults.length} images`);
      const formAlbum = new FormData();
      formAlbum.append('chat_id', TELEGRAM_CHAT_ID);
      const media = pngResults.map((img,index)=>({
        type:"photo",
        media:`attach://${img.fileName}`,
        caption:index===0?captionText:undefined
      }));
      formAlbum.append('media', JSON.stringify(media));
      pngResults.forEach(img=>formAlbum.append(img.fileName, fs.createReadStream(img.pngPath)));

      const tgResp = await axios.post(
        `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMediaGroup`,
        formAlbum,
        { headers: formAlbum.getHeaders() }
      );
      console.log('📸 Album result:', tgResp.data);

      // --- 4. Cleanup PNG + PDF ---
      pngResults.forEach(img=>{
        try{ fs.unlinkSync(img.pngPath); } catch{}
        try{ fs.unlinkSync(img.pdfPath); } catch{}
      });
    }

    // --- Cleanup tmpDir chung ---
    fs.rmSync(tmpDir, { recursive: true, force: true });
    console.log('🎉 All sheets processed successfully');

  } catch(err) {
    console.error('ERROR:', err?.message || err);
    process.exit(1);
  }
}

main();
