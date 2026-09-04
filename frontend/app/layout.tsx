import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';

const sans = Geist({ variable: '--font-geist-sans', subsets: ['latin'] });
const mono = Geist_Mono({ variable: '--font-geist-mono', subsets: ['latin'] });

export const metadata: Metadata = {
  metadataBase: new URL(process.env.NEXT_PUBLIC_SITE_URL ?? 'https://matchpulse-job-tracker.cheeky-spice-1122.chatgpt.site'),
  title: 'MatchPulse — AI Job Tracker',
  description: 'Track career boards and surface the roles that best match your active resume.',
  openGraph: {
    title: 'MatchPulse — AI Job Tracker',
    description: 'AI-powered job matching and career-board monitoring.',
    images: ['/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'MatchPulse — AI Job Tracker',
    description: 'AI-powered job matching and career-board monitoring.',
    images: ['/og.png'],
  },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="en"><body className={`${sans.variable} ${mono.variable}`}>{children}</body></html>;
}
