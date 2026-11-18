import type { Metadata } from 'next';
import { Plus_Jakarta_Sans, Public_Sans, JetBrains_Mono } from 'next/font/google';
import './globals.css';

const plusJakarta = Plus_Jakarta_Sans({
  subsets: ['latin'],
  weight: ['500', '600', '700'],
  variable: '--font-display',
});

const publicSans = Public_Sans({
  subsets: ['latin'],
  weight: ['400', '500', '600'],
  variable: '--font-sans',
});

const jetbrainsMono = JetBrains_Mono({
  subsets: ['latin'],
  weight: ['400', '500', '600'],
  variable: '--font-mono',
});

export const metadata: Metadata = {
  title: 'CinderSearch — S3-First Drop-In Elasticsearch Replacement in Go',
  description:
    'Concurrent, sharded Go search engine backed by S3 object storage, NVMe tiered read/write caching, CRC32C bit-rot protection, and zero-hop presigned S3 document retrieval.',
  openGraph: {
    title: 'CinderSearch — S3-First Drop-In Elasticsearch Replacement in Go',
    description:
      'Concurrent, sharded Go search engine backed by S3 object storage, NVMe tiered read/write caching, CRC32C bit-rot protection, and zero-hop presigned S3 document retrieval.',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'CinderSearch — S3-First Drop-In Elasticsearch Replacement in Go',
    description:
      'S3-first, NVMe-cached distributed search database prototype in pure Go with repeatable Makefile builds.',
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html
      lang="en"
      className={`${plusJakarta.variable} ${publicSans.variable} ${jetbrainsMono.variable} dark`}
    >
      <body
        suppressHydrationWarning
        className="bg-slate-950 text-slate-100 font-sans antialiased selection:bg-blue-600/30 selection:text-blue-200"
      >
        {children}
      </body>
    </html>
  );
}
