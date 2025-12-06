const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn, execFile } = require('child_process');
const axios = require('axios');
const FormData = require('form-data');
const { google } = require('googleapis');
const pLimit = require('p-limit');

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
      const delay = 2000 * attempt;
      console.log(`⚠️ Google 429 — retry ${attempt}/5 after ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
      return fetchPdfWithRetry(url, headers, attempt + 1);
    }
    throw err;
  }
}

function getMagickBinary() {
  const candidates = process.platform === 'win32'
    ? ['magick']                // Windows
    : ['convert', 'magick'];    // Linux, GitHub Actions

  for (const bin of candidates) {
    try {
      execFile(bin, ['-version']);
      return bin;
    } catch (_) {}
  }

  throw new Error('❌ No ImageMagick binary found');
}


// === Convert PDF → PNG + crop bằng pipeline không tạo file trung gian ===
function convertPdfToPngOptimized(pdfPath, outputPngPath) {
  const magickBin = getMagickBinary();

  return new Promise((resolve, reject) => {

    //
    // 1) pdftoppm xuất PPM chuẩn
    //
    const pdftoppm = spawn('pdftoppm', [
      '-singlefile',
      '-f', '1',
      '-l', '1',
      '-r', '180',
      '-color',        // ép xuất PPM 24-bit
      '-aa', 'yes',
      pdfPath,
      '-'
    ]);

    //
    // 2) ImageMagick đọc PPM từ stdin, trim và convert → PNG
    //
    const magick = spawn(magickBin, [
      'ppm:-',               // định dạng input chắc chắn là PPM
      '-flatten',            // tránh lỗi PPM có alpha
      '-trim',
      'png:' + outputPngPath
    ]);

    pdftoppm.stdout.pipe(magick.stdin);

    let errLog = '';
    pdftoppm.stderr.on('data', d => errLog += d.toString());
    magick.stderr.on('data', d => errLog += d.toString());

    magick.on('close', (code) => {
      if (code !== 0) {
        return reject(new Error(`ImageMagick error (${magickBin}): ${errLog}`));
      }
      resolve(outputPngPath);
    });

    pdftoppm.on('error', reject);
    magick.on('error', reject);
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

    // giới hạn số PDF xử lý song song
    const limit = pLimit(2);

    // ==========================
    //      PROCESS EACH SHEET
    // ==========================
    for (const sheetName of SHEET_NAMES) {
      console.log('--- Processing sheet:', sheetName);

      // sheetInfo + gid
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

      // ==========================
      //     CONVERT PDF → PNG
      // ==========================
      const albumImages = [];
      const promises = chunks.map(chunk => limit(async () => {

        const rangeParam = `${sheetName}!${START_COL}${chunk.startRow}:${END_COL}${chunk.endRow}`;

        const exportUrl =
          `https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/export?format=pdf` +
          `&portrait=false&size=A4&fitw=true&sheetnames=false&printtitle=false&pagenumbers=false` +
          `&gridlines=false&fzr=false&gid=${gid}&range=${encodeURIComponent(rangeParam)}`;

        console.log(`➡ Export PDF for ${sheetName} rows ${chunk.startRow}-${chunk.endRow}`);

        // Download PDF
        const pdfResp = await fetchPdfWithRetry(exportUrl, { Authorization: `Bearer ${accessToken}` });

        const pdfName = `${sheetName}_${chunk.startRow}-${chunk.endRow}.pdf`;
        const pdfPath = path.join(tmpDir, pdfName);
        fs.writeFileSync(pdfPath, Buffer.from(pdfResp.data));

        // Convert + crop trong 1 bước, không file trung gian
        const finalPngPath = path.join(tmpDir, `${sheetName}_${chunk.startRow}-${chunk.endRow}.png`);
        await convertPdfToPngOptimized(pdfPath, finalPngPath);

        return {
          path: finalPngPath,
          fileName: path.basename(finalPngPath),
          startRow: chunk.startRow
        };
      }));

      const results = await Promise.all(promises);

      // sắp xếp bảo toàn thứ tự
      results.sort((a, b) => a.startRow - b.startRow);
      albumImages.push(...results);

      // ================
      //  SEND ALBUM
      // ================
      console.log(`📤 Sending ALBUM for sheet ${sheetName} with ${albumImages.length} images`);

      const formAlbum = new FormData();
      formAlbum.append('chat_id', TELEGRAM_CHAT_ID);

      const media = albumImages.map((img, index) => ({
        type: "photo",
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

      // cleanup PNG
      albumImages.forEach(img => {
        try { fs.unlinkSync(img.path); } catch {}
      });
    }

    // cleanup tmpDir
    fs.rmSync(tmpDir, { recursive: true, force: true });
    console.log('🎉 All sheets processed successfully');

  } catch (err) {
    console.error('ERROR:', err?.message || err);
    process.exit(1);
  }
}

main();
