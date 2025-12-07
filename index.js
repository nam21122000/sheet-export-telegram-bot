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
      // delay random 3–6s
      const delay = 3000 + Math.floor(Math.random() * 3000);
      console.log(`⚠️ Google 429 — retry ${attempt}/5 after ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
      return fetchPdfWithRetry(url, headers, attempt + 1);
    }
    throw err;
  }
}

// Convert PDF → PNG + trim khoảng trắng
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

    // === tmpDir chung ===
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sheetpdf-'));

    const meta = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const allSheets = meta.data.sheets || [];

for (const sheetName of SHEET_NAMES) {
  console.log('--- Processing sheet:', sheetName);

  // (các phần lấy sheetInfo, F5:K6, last row, chunks...)

  // ============================
  // PHẢI ĐƯA VÀO TRONG VÒNG FOR
  // ============================
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

    // delay chunk nhỏ
    const chunkSize = chunk.endRow - chunk.startRow + 1;
    if (chunkSize < MAX_ROWS_PER_FILE) {
      const extraDelay = 500 + (MAX_ROWS_PER_FILE - chunkSize) * 100;
      console.log(`⏱ Delay thêm ${extraDelay}ms cho chunk nhỏ (${chunkSize} row)`);
      await new Promise(r => setTimeout(r, extraDelay));
    }

    albumImages.push({ path: pngPath, fileName: path.basename(pngPath) });
  }

  // --- SEND ALBUM ---
  console.log(`📤 Sending ALBUM for sheet ${sheetName} with ${albumImages.length} images`);
  const formAlbum = new FormData();
  formAlbum.append('chat_id', TELEGRAM_CHAT_ID);

  const media = albumImages.map((img, index) => ({
    type: 'photo',
    media: `attach://${img.fileName}`,
    caption: index === 0 ? captionText : undefined
  }));

  formAlbum.append('media', JSON.stringify(media));
  albumImages.forEach(img => formAlbum.append(img.fileName, fs.createReadStream(img.path)));

  const tgResp = await axios.post(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMediaGroup`,
    formAlbum,
    { headers: formAlbum.getHeaders() }
  );
  console.log('📸 Album result:', tgResp.data);

  // cleanup files
  albumImages.forEach(img => {
    try { fs.unlinkSync(img.path); } catch {}
    const pdfPath = path.join(tmpDir, img.fileName.replace('.png', '.pdf'));
    try { fs.unlinkSync(pdfPath); } catch {}
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
