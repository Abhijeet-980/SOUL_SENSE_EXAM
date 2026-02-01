# Contact Us Page (JSON)

## ✨ Feature Description
Add a simple Contact Us page where users can enter their name, email, subject, and message. Submissions are stored in a single server-side JSON file for later review.

## 🤔 Problem It Solves
- Provides a lightweight, structured way for users to send feedback without requiring a database.
- Centralizes messages for the team to review or export.

## 💡 Proposed Solution
- Frontend: a form with `name`, `email`, `subject`, and `message` fields and basic client-side validation.
- Backend: receive submissions, validate server-side, and append each entry to the JSON store with an `id` and `timestamp`.
- Safety: enforce size limits, basic rate limiting, input sanitization, and atomic writes to avoid corruption.

## 🔄 Alternatives Considered
- Use a database for structured storage.
- Send submissions via email only.
- Integrate a third-party form service.

## 📌 Additional Context
- Privacy: avoid storing sensitive data beyond contact details and messages; apply size limits and sanitization.
- Future: add an admin listing, export, email notifications, and periodic archival of the JSON store.