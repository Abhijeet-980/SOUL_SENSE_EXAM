# 🌐 Soul Sense Web Frontend

A modern Next.js 14 dashboard for the Soul Sense EQ Test platform.

---

## 🚀 Getting Started

### 1. Tauri Desktop Shell (Recommended)

The modern way to run Soul Sense is via the Tauri shell, which manages both the frontend and the backend sidecar.

```bash
# Install dependencies
npm install

# Run in Development mode
npm run tauri dev
```

### 2. Standalone Web Frontend

If you want to run the web app in a browser:

```bash
# Terminal 1: Backend
python ../backend/fastapi/start_server.py --y

# Terminal 2: Frontend
npm install
npm run dev
```

👉 Open [http://localhost:3005](http://localhost:3005)

---

## 🖥️ Desktop Architecture (Tauri)

This project uses **Tauri v2** to provide a native desktop experience:

- **Frontend**: Served from the Next.js `out` directory (Static Export).
- **Backend Sidecar**: The Python FastAPI server is bundled as a standalone executable and automatically managed by the Tauri shell.
- **IPC**: Communication happens over Localhost/HTTP.

> [!NOTE]
> **Port Management**: In Tauri mode, the shell automatically handles port 3005 (Frontend) and port 8000 (Backend). Do not run standalone servers simultaneously to avoid port conflicts.
>
> **Live Updates**:
>
> - **Frontend**: Saving files triggers **HMR** (instant updates in the window).
> - **Backend**: Changes to Python code require a **rebuild** of the sidecar binary. Run `.\scripts\setup_tauri_env.ps1` from the root to update.

---

## 🏗️ Architecture

This project follows **Domain-Driven, Feature-Sliced** architecture.

- **Components**: UI primitives in `/ui`, layout in `/layout`, sections in `/sections`.
- **Standards**: Absolute imports (`@/`), strict architectural boundaries, and barrel files.
- **Reference**: See [ADR 001: Frontend Architecture](../docs/architecture/001-frontend-structure.md) for full details.

---

## 🛠️ Tech Stack

- **Core**: Next.js 14 (App Router), React 18, TypeScript
- **Style**: Tailwind CSS, Framer Motion (Animations)
- **UI**: Radix UI, Lucide Icons, Recharts (Data Viz)
- **Logic**: Zod + React Hook Form, Recharts

---

## ✅ Quality Gates

Before pushing, ensure:

- `npm run lint` - Code style & architecture check
- `npm run build` - Production build verification
