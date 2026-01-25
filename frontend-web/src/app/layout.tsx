import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'SOUL SENSE',
  description: 'Form Validation System with Zod',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}