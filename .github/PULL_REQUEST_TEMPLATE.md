## 📌 Description
This PR removes hardcoded CORS origins from `backend/fastapi/api/main.py` and externalizes them into environment variables driven by Pydantic's `BaseSettings`. This addresses potential deployment blockers and security risks associated with hardcoded origins.

**Key Changes:**
- **Moved CORS origins** from `main.py` to `BACKEND_CORS_ORIGINS` in `backend/fastapi/api/config.py`.
- **Implemented a validator** in the `BaseAppSettings` class that dynamically handles environment variables passed as either comma-separated strings (CSV) or JSON arrays.
- **Created `.env.example`** to document the new `BACKEND_CORS_ORIGINS` variable and provide examples for local, staging, and production configurations.
- **Added `.env.example` exception** to `.gitignore` to ensure documentation is available for other developers.

Fixes: # (issue number, if applicable)

---

## 🔧 Type of Change
Please mark the relevant option(s):

- [ ] 🐛 Bug fix
- [x] ✨ New feature
- [x] 📝 Documentation update
- [x] ♻️ Refactor / Code cleanup
- [ ] 🎨 UI / Styling change
- [x] 🚀 Other (Security Hardening): Prevents unauthorized local domains from hitting production backends.

---

## 🧪 How Has This Been Tested?
Describe the tests you ran to verify your changes.

- [x] Manual testing
- [x] Automated tests (Verified with a custom configuration test script `test_cors_config.py` against both CSV and JSON inputs)

---

## 📸 Screenshots (if applicable)
N/A (Backend logic change)

---

## ✅ Checklist
Please confirm the following:

- [x] My code follows the project’s coding style
- [x] I have tested my changes
- [x] I have updated documentation where necessary
- [x] This PR does not introduce breaking changes

---

## 📝 Additional Notes
The `BACKEND_CORS_ORIGINS` attribute in `config.py` uses the `Any` type hint and a `mode="before"` validator. This is specifically designed to bypass Pydantic's default JSON parsing for list fields, which often fails when environment variables are provided as standard comma-separated strings.
