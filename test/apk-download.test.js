const request = require('supertest');
const fs = require('fs');
const path = require('path');
const app = require('../src/index');

const APK_DIR = path.join(__dirname, '..', 'downloads');
const APK_PATH = path.join(APK_DIR, 'elite-bet.apk');

describe('GET /downloads/elite-bet.apk', () => {
  describe('when APK does not exist', () => {
    beforeAll(() => {
      // Ensure no APK file exists
      if (fs.existsSync(APK_PATH)) {
        fs.renameSync(APK_PATH, APK_PATH + '.bak');
      }
    });

    afterAll(() => {
      // Restore if we backed it up
      if (fs.existsSync(APK_PATH + '.bak')) {
        fs.renameSync(APK_PATH + '.bak', APK_PATH);
      }
    });

    test('returns 404 with error message', async () => {
      const res = await request(app)
        .get('/downloads/elite-bet.apk')
        .expect(404);
      expect(res.body.error).toBe('APK not available yet.');
    });
  });

  describe('when APK exists', () => {
    beforeAll(() => {
      // Create a fake APK file for testing
      if (!fs.existsSync(APK_DIR)) {
        fs.mkdirSync(APK_DIR, { recursive: true });
      }
      fs.writeFileSync(APK_PATH, 'fake-apk-content-for-testing');
    });

    afterAll(() => {
      // Clean up the fake APK
      if (fs.existsSync(APK_PATH)) {
        fs.unlinkSync(APK_PATH);
      }
    });

    test('returns 200 with correct content type and disposition', async () => {
      const res = await request(app)
        .get('/downloads/elite-bet.apk')
        .expect(200);

      expect(res.headers['content-type']).toBe(
        'application/vnd.android.package-archive'
      );
      expect(res.headers['content-disposition']).toBe(
        'attachment; filename="elite-bet.apk"'
      );
      expect(res.text).toBe('fake-apk-content-for-testing');
    });

    test('does not require authentication', async () => {
      // No Authorization header — should still succeed
      const res = await request(app)
        .get('/downloads/elite-bet.apk')
        .expect(200);

      expect(res.status).toBe(200);
    });
  });
});
