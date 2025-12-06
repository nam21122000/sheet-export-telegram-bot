// index.js
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');
const axios = require('axios');
const FormData = require('form-data');
const { google } = require('googleapis');

async function main() {
  try {
    // === Env / Config ===
    const serviceAccountJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
    const SHEET_NAMES = (process.env.SHEET_NAMES || 'Ladi,Mydu').split(',').map(s => s.trim()).filter(Boolean);
    const START_COL = process.env.START_COL || 'F';
    const END_COL = process.env.END_COL || 'AD';
    const MAX_ROWS_PER_FILE = Number(process.env.MAX_ROWS_PER_FILE || '40');
    const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
    const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

    if (!serviceAccountJson || !SPREADSHEET_ID || !TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
      throw new Error('Missing required environment variables. Please set GOOGLE_SERVICE_ACCOUNT_JSON, SPREADSHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID');
    }

    const creds = JSON.parse(serviceAccountJson);

    // === Authorize as service account and get access token ===
    const jwtClient = new google.auth.JWT(
      creds.client_email,
      null,
      creds.private_key,
      [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/spreadsheets.readonly'
      ],
      null
    );

    await jwtClient.authorize();
    const tokenObj = await jwtClient.getAccessToken();
    const accessToken = tokenObj && tokenObj.token;
    if (!accessToken) throw new Error('Failed to obtain access token from service account');

    const sheetsApi = google.sheets({ version: 'v4', auth: jwtClient });

    // === START PROCESS EACH SHEET ===
    for (const sheetName of SHEET_NAMES) {
      console.log('--- Processing sheet:', sheetName);

      // Get sheet metadata to find gid
      const meta = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
      const sheetInfo = (meta.data.sheets || []).find(s => s.properties && s.properties.title === sheetName);
      if (!sheetInfo) {
        console.log(`⚠️ Sheet "${sheetName}" not found — skipping`);
        continue;
      }
      const gid = sheetInfo.properties.sheetId;

      // find lastRow by reading column K (col 11)
      const colToCheck = 'K';
      const colRange = `${sheetName}!${colToCheck}1:${colToCheck}2000`;
      const colRes = await sheetsApi.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: colRange });
      const colVals = colRes.data.values || [];
      let lastRow = 1;
      for (let i = colVals.length - 1; i >= 0; i--) {
        if (colVals[i] && colVals[i][0] !== '' && colVals[i][0] !== null) {
          lastRow = i + 1;
          break;
        }
      }
      console.log('Last row detected (col K):', lastRow);

      // Get caption parts F5, J5, K5
      const f5 = (await sheetsApi.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!F5` })).data.values?.[0]?.[0] || '';
      const j5 = (await sheetsApi.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!J5` })).data.values?.[0]?.[0] || '';
      const k5 = (await sheetsApi.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!K5` })).data.values?.[0]?.[0] || '';
      const captionText = `${f5}    ${j5}    ${k5}`;

      // --- NEW: store PNGs for album ---
      let albumImages = [];

      let startRow = 1;
      while (startRow <= lastRow) {
        const endRow = Math.min(startRow + MAX_ROWS_PER_FILE - 1, lastRow);

        const rangeParam = `${sheetName}!${START_COL}${startRow}:${END_COL}${endRow}`;
        const exportUrl =
          `https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/export?format=pdf` +
          `&portrait=false&size=A4&fitw=true` +
          `&sheetnames=false&printtitle=false&pagenumbers=false` +
          `&gridlines=false&fzr=false` +
          `&gid=${gid}` +
          `&range=${encodeURIComponent(rangeParam)}`;

        console.log(`➡ Export PDF for ${sheetName} rows ${startRow}-${endRow}`);
        const pdfResp = await axios.get(exportUrl, {
          responseType: 'arraybuffer',
          headers: { Authorization: `Bearer ${accessToken}` },
          maxContentLength: Infinity,
          maxBodyLength: Infinity
        });

        // Save PDF to temp file
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sheetpdf-'));
        const pdfName = `${sheetName}_${startRow}-${endRow}.pdf`;
        const pdfPath = path.join(tmpDir, pdfName);
        fs.writeFileSync(pdfPath, Buffer.from(pdfResp.data));

        // Convert PDF -> PNG
        const outPrefix = path.join(tmpDir, path.basename(pdfName, '.pdf'));
        console.log('🔁 Converting PDF -> PNG via pdftoppm');

        try {
          execFileSync('pdftoppm', ['-png', '-singlefile', '-r', '180', pdfPath, outPrefix], { stdio: 'inherit' });
        } catch (err) {
          console.error('❌ pdftoppm failed:', err.message);
          throw err;
        }

        const pngPath = outPrefix + '.png';
        if (!fs.existsSync(pngPath)) {
          console.log('❌ PNG not found after conversion. Listing tmpDir:', fs.readdirSync(tmpDir));
          throw new Error('PNG conversion failed');
        }

        // PUSH PNG to album list instead of sending immediately
        albumImages.push({
          path: pngPath,
          fileName: path.basename(pngPath)
        });

        // Anti-rate limit delay
        await new Promise(resolve => setTimeout(resolve, 1500));

        startRow = endRow + 1;
      } // end while chunks

      // === NEW: SEND MEDIA GROUP (ALBUM FOR THE WHOLE SHEET) ===
      console.log(`📤 Sending ALBUM for sheet ${sheetName} with ${albumImages.length} images`);

      const formAlbum = new FormData();
      formAlbum.append('chat_id', TELEGRAM_CHAT_ID);

      // Create media metadata
      const media = albumImages.map((img, index) => ({
        type: "photo",
        media: `attach://${img.fileName}`,
        caption: index === 0 ? captionText : undefined
      }));

      formAlbum.append('media', JSON.stringify(media));

      // Attach files
      albumImages.forEach(img => {
        formAlbum.append(img.fileName, fs.createReadStream(img.path));
      });

      const tgAlbumUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMediaGroup`;
      const tgRespAlbum = await axios.post(tgAlbumUrl, formAlbum, {
        headers: formAlbum.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity
      });

      console.log('📸 Album result:', JSON.stringify(tgRespAlbum.data, null, 2));

      // Cleanup temp png files
      for (const img of albumImages) {
        try { fs.unlinkSync(img.path); } catch {}
      }

      albumImages = [];
    } // end for each sheet

    console.log('🎉 All sheets processed successfully');
  } catch (err) {
    console.error('ERROR:', err && err.message ? err.message : err);
    process.exit(1);
  }
}

main();
