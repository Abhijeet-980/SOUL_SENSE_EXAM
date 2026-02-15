## 📌 Description
Implemented a high-performance, interactive `HistoryChart` component for visualizing exam score trends over time. The component supports overall performance tracking along with optional category-specific trend overlays.

**Key Changes:**
- Created `frontend-web/src/components/results/history-chart.tsx` using **Recharts**.
- Added time-range filtering logic ('7d', '30d', '90d', 'all').
- Implemented premium UI features: glassmorphism, custom tooltips, and smooth animations.
- Handled empty states for new users with no historical data.
- Exported the component via `frontend-web/src/components/results/index.ts`.
- **Fix**: Updated `.github/workflows/auto-tracker.yml` permissions and token to resolve "Resource not accessible" errors.
- **Fix**: Resolved `ModuleNotFoundError` in tests by using relative import for `User` model in `api/routers/auth.py`.

Fixes: # (issue number, if applicable)

---

## 🔧 Type of Change
Please mark the relevant option(s):

- [x] 🐛 Bug fix
- [x] ✨ New feature
- [ ] 📝 Documentation update
- [ ] ♻️ Refactor / Code cleanup
- [x] 🎨 UI / Styling change
- [ ] 🚀 Other (please describe):

---

## 🧪 How Has This Been Tested?
Describe the tests you ran to verify your changes.

- [x] Manual testing (verified logic for time filtering and sorting)
- [ ] Automated tests
- [ ] Not tested (please explain why)

---

## 📸 Screenshots (if applicable)
Add screenshots or screen recordings to show UI changes.

---

## ✅ Checklist
Please confirm the following:

- [x] My code follows the project’s coding style
- [x] I have tested my changes
- [x] I have updated documentation where necessary
- [x] This PR does not introduce breaking changes

---

## 📝 Additional Notes
The component uses `date-fns` for robust date handling and is styled using the project's consistent Tailwind theme tokens (`hsl(var(--primary))`). It is fully responsive and optimized for both dark and light modes.
