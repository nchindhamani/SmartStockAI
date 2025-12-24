import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'

const inter = Inter({ 
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-inter',
})

export const metadata: Metadata = {
  title: 'SmartStock AI - Enterprise Financial Intelligence',
  description: 'Agentic RAG for Financial Analysis - Intelligent stock research powered by AI, real-time market data, and SEC filings',
  keywords: ['stock analysis', 'financial AI', 'market research', 'SEC filings', 'investment intelligence'],
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className={inter.variable}>
      <body className={inter.className}>{children}</body>
    </html>
  )
}
