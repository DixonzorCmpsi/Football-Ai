/**
 * Player headshots come from the NFL's Cloudinary CDN, which serves the
 * ORIGINAL asset when the transformation segment only says `f_auto,q_auto`.
 * In practice that is a 3400x2450 (8.3 megapixel) JPEG — roughly 600KB on the
 * wire and ~33MB of decoded bitmap — for an avatar we render at 28-40px.
 *
 * Measured on the Ranks page: 59 image requests totalling 34MB, and every
 * frame over 50ms during a scroll was an image decode. Blocking images made
 * scrolling a flat 16.7ms/frame with zero long tasks.
 *
 * Injecting an explicit `c_fill,w_,h_` into the transformation makes the CDN
 * do the resize, so the browser decodes a thumbnail instead of a poster.
 */
const UPLOAD = '/image/upload/';

// Looks like a Cloudinary transformation segment ("f_auto,q_auto", "w_100,c_fill", ...)
const TRANSFORM_RE = /(^|,)[a-z]{1,3}_[^/,]+/;

export function sizedPlayerImage(url: string | null | undefined, px = 48): string | undefined {
  if (!url) return undefined;
  const at = url.indexOf(UPLOAD);
  if (at === -1) return url; // not a Cloudinary URL — leave it alone

  const head = url.slice(0, at + UPLOAD.length);
  const parts = url.slice(at + UPLOAD.length).split('/');
  if (parts.length === 0) return url;

  // Render at 2x so the thumbnail still looks sharp on retina displays.
  const edge = Math.max(16, Math.round(px * 2));
  const transform = `c_fill,g_face,w_${edge},h_${edge},f_auto,q_auto`;

  if (TRANSFORM_RE.test(parts[0])) parts[0] = transform;
  else parts.unshift(transform);

  return head + parts.join('/');
}
