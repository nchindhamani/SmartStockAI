import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        // SmartStock AI Brand Colors
        'brand-dark': '#1e3a8a',      // Dark Blue - Header/Footer
        'brand-darker': '#1e3050',    // Darker blue for card backgrounds
        'chat-bg': '#f3f4f6',         // Light Gray - Chat Background
        'user-bubble': '#1e3a8a',     // Dark Blue - User Messages
        'assistant-border': '#4f46e5', // Indigo - Assistant Bubble Border
        // Metric Card Colors
        'metric-red': '#fef2f2',
        'metric-red-text': '#dc2626',
        'metric-blue': '#eff6ff',
        'metric-blue-text': '#2563eb',
        'metric-yellow': '#fefce8',
        'metric-yellow-text': '#ca8a04',
        'metric-green': '#f0fdf4',
        'metric-green-text': '#16a34a',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}

export default config

