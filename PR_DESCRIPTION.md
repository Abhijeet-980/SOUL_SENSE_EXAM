## 📌 Description
Implemented static authentication pages (Login, Register, and Forgot Password) with a premium design matching the Soul Sense UI. These pages include full form validation using React Hook Form and Zod, password visibility toggles, a real-time password strength indicator, and social login placeholders.

Fixes: #431

---

## 🔧 Type of Change
- [ ] 🐛 Bug fix
- [x] ✨ New feature
- [ ] 📝 Documentation update
- [ ] ♻️ Refactor / Code cleanup
- [x] 🎨 UI / Styling change
- [ ] 🚀 Other (please describe):

---

## 🧪 How Has This Been Tested?
- [x] Manual testing: Verified all routes (`/login`, `/register`, `/forgot-password`) serve HTTP 200 and function as expected on the local dev server (port 3005).
- [ ] Automated tests
- [ ] Not tested (please explain why)

---

## 📸 Screenshots (if applicable)
[Include screenshots of the new Login, Register, and Forgot Password pages here]

---

## ✅ Checklist
- [x] My code follows the project’s coding style
- [x] I have tested my changes
- [x] I have updated documentation where necessary
- [x] This PR does not introduce breaking changes

---

## 📝 Additional Notes
- The implementation uses `framer-motion` for smooth transitions and a glassmorphic design consistent with the landing page.
- Password strength indicator provides real-time feedback on complexity requirements.
- Social login buttons are currently placeholders for future integration.
