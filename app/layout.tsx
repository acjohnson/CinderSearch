import type { Metadata } from 'next';
import './globals.css';

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
    <html lang="en" className="dark">
      <body
        suppressHydrationWarning
        className="bg-slate-950 text-slate-100 font-sans antialiased selection:bg-blue-600/30 selection:text-blue-200"
      >
        {children}
      </body>
    </html>
  );
}
