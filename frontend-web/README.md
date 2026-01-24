# 🧠 SoulSense Web Frontend

The web frontend for SoulSense - a comprehensive Emotional Intelligence (EQ) assessment platform built with Next.js 14+, TypeScript, and Tailwind CSS.

## 🚀 Features

- **Next.js 14+** with App Router architecture
- **TypeScript** with strict type checking
- **Tailwind CSS** for styling
- **ESLint** with Next.js recommended rules
- **Prettier** for code formatting
- **Responsive design** for all devices
- **SEO optimized** with proper meta tags
- **Image optimization** with Next.js Image component

## 🛠️ Tech Stack

- **Framework:** Next.js 14+
- **Language:** TypeScript
- **Styling:** Tailwind CSS
- **Linting:** ESLint
- **Formatting:** Prettier
- **Font:** Inter (Google Fonts)

## 📁 Project Structure

```
frontend-web/
├── public/
│   ├── favicon.ico
│   └── images/
├── src/
│   ├── app/
│   │   ├── globals.css
│   │   ├── layout.tsx
│   │   └── page.tsx
│   └── components/
│       └── common/
│           └── Button.tsx
├── .env.local.example
├── .eslintrc.json
├── .gitignore
├── .prettierrc
├── next.config.ts
├── package.json
├── tsconfig.json
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Node.js 18+ and npm
- The SoulSense backend API running (see main project README)

### Installation

1. **Install dependencies:**

   ```bash
   npm install
   ```

2. **Set up environment variables:**

   ```bash
   cp .env.local.example .env.local
   # Edit .env.local with your configuration
   ```

3. **Start the development server:**

   ```bash
   npm run dev
   ```

4. **Open [http://localhost:3000](http://localhost:3000)** in your browser

## 📜 Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Create production build
- `npm run start` - Start production server
- `npm run lint` - Run ESLint
- `npm run lint:fix` - Run ESLint with auto-fix
- `npm run format` - Format code with Prettier
- `npm run format:check` - Check code formatting
- `npm run type-check` - Run TypeScript type checking
- `npm run clean` - Clean build artifacts

## 🔧 Configuration

### Environment Variables

Create a `.env.local` file with the following variables:

```env
# API Configuration
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_API_BASE_PATH=/api/v1

# Application Settings
NEXT_PUBLIC_APP_NAME=Soul Sense
NEXT_PUBLIC_APP_VERSION=1.0.0

# Feature Flags
NEXT_PUBLIC_ENABLE_ANALYTICS=false
NEXT_PUBLIC_ENABLE_DEBUG_MODE=false
```

### TypeScript Configuration

The project uses strict TypeScript configuration with:

- Strict type checking enabled
- Absolute imports configured (`@/*`)
- Path mapping for cleaner imports

### ESLint Configuration

ESLint is configured with:

- Next.js recommended rules
- TypeScript support
- React hooks rules
- Import sorting

### Prettier Configuration

Code formatting is configured with:

- Single quotes
- Semicolons
- 2-space indentation
- 80 character line width

## 🎨 Styling

The project uses Tailwind CSS with:

- Custom color palette
- Responsive design utilities
- Dark mode support (ready for implementation)
- Component-based styling approach

## 📱 Components

### Common Components

- `Button` - Reusable button component with variants

### Component Structure

Components are organized in the `src/components/` directory:

- `common/` - Shared, reusable components
- Future: `layout/`, `forms/`, `assessment/`, etc.

## 🔍 Development

### Code Quality

- **Linting:** `npm run lint`
- **Formatting:** `npm run format`
- **Type Checking:** `npm run type-check`

### Building for Production

```bash
npm run build
npm run start
```

### Testing

Testing setup is ready for implementation:

- Jest configuration included
- Testing library ready
- Test scripts prepared

## 🚀 Deployment

The application is ready for deployment on:

- **Vercel** (recommended for Next.js)
- **Netlify**
- **Docker** (with included Dockerfile)
- **Traditional hosting**

### Environment Setup

For production deployment, ensure these environment variables are set:

- `NEXT_PUBLIC_API_URL` - Production API URL
- `NEXT_PUBLIC_APP_ENV=production`

## 🤝 Contributing

1. Follow the established code style
2. Run `npm run lint && npm run format && npm run type-check` before committing
3. Use conventional commit messages
4. Test your changes thoroughly

## 📄 License

This project is part of the SoulSense platform. See the main project LICENSE file for details.
