/**
 * Lets the assistant use the app with "hands": read the screen, click, type,
 * press keys, pick from dropdowns, scroll.
 *
 * It runs in the user's own tab, so everything it does happens on the page they
 * are watching (with a highlight on each element it touches). The backend's UI
 * tools (backend/agent/ui_control.py) send one command at a time; each returns
 * a fresh snapshot, so the model sees the result of every step before the next.
 *
 * Snapshot format, which the model reads:
 *
 *   Page: /ranks · THE SPOT AI
 *   Elements you can act on:
 *   e3 button "Ranks" [selected]
 *   e7 textbox "Search players" value="Pur"
 *   ...
 *   Text on screen:
 *   ...
 *
 * Refs (e7) stay attached to the same DOM element for as long as it exists, so a
 * ref from one snapshot still works after typing re-renders the list around it.
 *
 * Anything inside [data-agent-ignore] (the assistant's own dock, panel and
 * settings, where API keys are typed) is invisible and untouchable here.
 */

export type UiCommand = {
  id: string;
  op: 'snapshot' | 'click' | 'type' | 'key' | 'select' | 'scroll';
  target?: string;
  text?: string;
  press_enter?: boolean;
  key?: string;
  option?: string;
  direction?: 'up' | 'down' | 'top' | 'bottom';
};

export type UiResult = { ok: boolean; text: string };

const MAX_ELEMENTS = 150;
const MAX_SCREEN_TEXT = 3500;
const MAX_LABEL = 80;

const INTERACTIVE =
  'a[href], button, input, textarea, select, summary, [role="button"], [role="link"], [role="tab"], ' +
  '[role="checkbox"], [role="switch"], [role="menuitem"], [role="option"], [role="radio"], [tabindex]:not([tabindex="-1"]), [contenteditable="true"]';

// --- refs ------------------------------------------------------------------

const refs = new WeakMap<Element, string>();
let nextRef = 1;

function refFor(el: Element): string {
  let ref = refs.get(el);
  if (!ref) {
    ref = `e${nextRef++}`;
    refs.set(el, ref);
    el.setAttribute('data-agent-ref', ref);
  }
  return ref;
}

// --- in-flight requests, so "settled" means the result has loaded -----------

let inFlight = 0;
let fetchPatched = false;

function patchFetch() {
  if (fetchPatched || typeof window === 'undefined') return;
  fetchPatched = true;
  const original = window.fetch.bind(window);
  window.fetch = async (...args: Parameters<typeof fetch>) => {
    // The assistant's own stream stays open for the whole answer; counting it
    // would mean the page never looks settled.
    const url = String(args[0] instanceof Request ? args[0].url : args[0]);
    const counted = !/\/agent\//.test(url);
    if (counted) inFlight++;
    try {
      return await original(...args);
    } finally {
      if (counted) inFlight--;
    }
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Wait until requests finish and the DOM stops changing (or give up at maxMs). */
async function settle(maxMs = 8000, quietMs = 450): Promise<void> {
  const start = Date.now();
  let lastChange = Date.now();
  const observer = new MutationObserver(() => {
    lastChange = Date.now();
  });
  observer.observe(document.body, { subtree: true, childList: true, attributes: true, characterData: true });
  try {
    await sleep(120);
    while (Date.now() - start < maxMs) {
      if (inFlight === 0 && Date.now() - lastChange >= quietMs) return;
      await sleep(80);
    }
  } finally {
    observer.disconnect();
  }
}

// --- reading the page ------------------------------------------------------

function ignored(el: Element): boolean {
  return !!el.closest('[data-agent-ignore]');
}

function visible(el: Element): boolean {
  if (!(el instanceof HTMLElement)) return false;
  if (el.closest('[aria-hidden="true"], [hidden], [inert]')) return false;
  const rects = el.getClientRects();
  if (!rects.length) return false;
  const rect = el.getBoundingClientRect();
  if (rect.width < 2 || rect.height < 2) return false;
  // Off-canvas drawers sit beside the viewport; only vertical scrolling reaches things.
  if (rect.left >= window.innerWidth || rect.right <= 0) return false;
  const style = getComputedStyle(el);
  return style.visibility !== 'hidden' && style.opacity !== '0';
}

function clean(s: string | null | undefined, max = MAX_LABEL): string {
  const t = (s || '').replace(/\s+/g, ' ').trim();
  return t.length > max ? `${t.slice(0, max - 1)}…` : t;
}

function kindOf(el: Element): string {
  const role = el.getAttribute('role');
  if (role) return role === 'link' ? 'link' : role;
  const tag = el.tagName.toLowerCase();
  if (tag === 'a') return 'link';
  if (tag === 'select') return 'dropdown';
  if (tag === 'textarea') return 'textbox';
  if (tag === 'input') {
    const type = (el as HTMLInputElement).type;
    if (type === 'checkbox' || type === 'radio') return type;
    if (type === 'button' || type === 'submit') return 'button';
    if (type === 'range') return 'slider';
    return type === 'search' ? 'searchbox' : 'textbox';
  }
  if (tag === 'button' || tag === 'summary') return 'button';
  return 'clickable';
}

function labelOf(el: Element): string {
  const aria = el.getAttribute('aria-label');
  if (aria) return clean(aria);
  const labelledBy = el.getAttribute('aria-labelledby');
  if (labelledBy) {
    const text = labelledBy.split(/\s+/).map((id) => document.getElementById(id)?.textContent || '').join(' ');
    if (clean(text)) return clean(text);
  }
  if (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement || el instanceof HTMLSelectElement) {
    const label = el.id ? document.querySelector(`label[for="${CSS.escape(el.id)}"]`) : el.closest('label');
    if (label && clean(label.textContent)) return clean(label.textContent);
    if ('placeholder' in el && el.placeholder) return clean(el.placeholder);
    if (el.name) return clean(el.name);
  }
  const text = clean((el as HTMLElement).innerText);
  if (text) return text;
  const title = el.getAttribute('title');
  if (title) return clean(title);
  const img = el.querySelector('img[alt]');
  if (img) return clean(img.getAttribute('alt'));
  // Icon-only buttons: lucide renders <svg class="lucide lucide-plus">. The icon's
  // name plus the card it sits in ("plus icon, in Kyler Murray…") is enough to act on.
  const icon = Array.from(el.querySelector('svg')?.classList || []).find((c) => c.startsWith('lucide-'));
  if (icon) {
    // The nearest enclosing text with a word in it names the card ("Kyler Murray"),
    // not a stray number beside the icon ("-266K").
    let context = '';
    for (let p = el.parentElement; p && p !== document.body && !context; p = p.parentElement) {
      const text = clean(p.innerText, 40);
      if (/[A-Za-z]{3,}/.test(text)) context = text;
    }
    return `${icon.slice(7).replace(/-/g, ' ')} icon${context ? ` (on ${context})` : ''}`;
  }
  return '';
}

const REGION_ORDER = ['popup', 'page', 'header', 'left panel', 'right panel', 'other'];
const REGION_CAP: Record<string, number> = { popup: 80, page: 100, header: 25, 'left panel': 12, 'right panel': 12, other: 20 };
const POPUP = '[role="dialog"], [aria-modal="true"], .fixed.inset-0';

/** Which part of the app an element is in, so the page's own content is listed first. */
function regionOf(el: Element): string {
  if (el.closest(POPUP)) return 'popup';
  return el.closest('[data-agent-region]')?.getAttribute('data-agent-region') || 'other';
}

function stateOf(el: Element): string {
  const parts: string[] = [];
  if (el instanceof HTMLInputElement) {
    if (el.type === 'checkbox' || el.type === 'radio') parts.push(el.checked ? 'checked' : 'unchecked');
    else if (el.type === 'password') parts.push('value hidden');
    else parts.push(`value="${clean(el.value, 60)}"`);
  } else if (el instanceof HTMLTextAreaElement) {
    parts.push(`value="${clean(el.value, 60)}"`);
  } else if (el instanceof HTMLSelectElement) {
    const options = Array.from(el.options).map((o) => clean(o.text, 30));
    const shown = options.length > 12 ? [...options.slice(0, 12), `…${options.length - 12} more`] : options;
    parts.push(`selected="${clean(el.selectedOptions[0]?.text, 40)}"`, `options: ${shown.join(' | ')}`);
  }
  if ((el as HTMLButtonElement).disabled || el.getAttribute('aria-disabled') === 'true') parts.push('disabled');
  if (el.getAttribute('aria-selected') === 'true' || el.getAttribute('aria-pressed') === 'true') parts.push('selected');
  if (el.getAttribute('aria-expanded') === 'true') parts.push('expanded');
  if (document.activeElement === el) parts.push('focused');
  return parts.length ? ` [${parts.join(', ')}]` : '';
}

/**
 * Elements a person could act on. Explicitly interactive ones, plus React's
 * click-handler divs (cards, rows), which only reveal themselves through a
 * pointer cursor. A pointer element inside another listed one is skipped: the
 * card is one target, not its name, photo and badge separately.
 */
function actionable(): Element[] {
  const found = new Set<Element>();
  document.querySelectorAll(INTERACTIVE).forEach((el) => found.add(el));
  document.querySelectorAll('body *').forEach((el) => {
    if (found.has(el) || !(el instanceof HTMLElement)) return;
    if (getComputedStyle(el).cursor !== 'pointer') return;
    const parent = el.parentElement;
    if (parent && getComputedStyle(parent).cursor === 'pointer') return;
    found.add(el);
  });
  const list = Array.from(found).filter((el) => !ignored(el) && visible(el));
  // Drop an element whose ancestor is also listed and has the same label
  // (a button wrapping a span with a pointer cursor).
  const set = new Set(list);
  return list.filter((el) => {
    for (let p = el.parentElement; p; p = p.parentElement) {
      if (set.has(p) && kindOf(el) === 'clickable') return false;
    }
    return true;
  });
}

function inViewport(el: Element): boolean {
  const r = el.getBoundingClientRect();
  return r.bottom > 0 && r.top < window.innerHeight && r.right > 0 && r.left < window.innerWidth;
}

function screenText(): string {
  // The page (or an open popup) is what the user is reading; the side panels'
  // trending lists would crowd it out. innerText respects layout, so hidden views
  // are skipped. The assistant's own text is subtracted.
  const popups = Array.from(document.querySelectorAll<HTMLElement>(POPUP)).filter((d) => !ignored(d) && visible(d));
  const page = document.querySelector<HTMLElement>('[data-agent-region="page"]');
  const sources = popups.length ? popups : page ? [page] : [document.body];
  let text = sources.map((el) => el.innerText || '').join('\n');
  document.querySelectorAll('[data-agent-ignore]').forEach((n) => {
    const own = (n as HTMLElement).innerText;
    if (own) text = text.split(own).join('');
  });
  const lines = text.split('\n').map((l) => l.replace(/\s+/g, ' ').trim()).filter(Boolean);
  const joined = lines.join('\n');
  return joined.length > MAX_SCREEN_TEXT ? `${joined.slice(0, MAX_SCREEN_TEXT)}\n…(more text below)` : joined;
}

export function snapshot(note = ''): string {
  const byRegion = new Map<string, Element[]>();
  for (const el of actionable()) {
    const region = regionOf(el);
    byRegion.set(region, [...(byRegion.get(region) || []), el]);
  }
  const lines: string[] = [];
  let budget = MAX_ELEMENTS;
  for (const region of REGION_ORDER) {
    const els = byRegion.get(region);
    if (!els?.length || budget <= 0) continue;
    // What's in view first, then what scrolling reaches.
    const ordered = [...els.filter(inViewport), ...els.filter((el) => !inViewport(el))];
    const shown = ordered.slice(0, Math.min(REGION_CAP[region] ?? 20, budget));
    budget -= shown.length;
    lines.push(`[${region === 'popup' ? 'popup (open; Escape usually closes it)' : region}]`);
    for (const el of shown) {
      const label = labelOf(el);
      // An unlabelled click target gives the model nothing to reason about.
      if (!label) continue;
      const offscreen = inViewport(el) ? '' : ' (scroll to see)';
      lines.push(`${refFor(el)} ${kindOf(el)} "${label}"${stateOf(el)}${offscreen}`);
    }
    if (ordered.length > shown.length) lines.push(`…${ordered.length - shown.length} more here; scroll to reach them.`);
  }
  return [
    note,
    `Page: ${location.pathname}${location.search} · ${document.title}`,
    'Elements you can act on, by area:',
    ...lines,
    '--- screen start ---',
    screenText(),
    '--- screen end ---',
  ].filter(Boolean).join('\n');
}

// --- acting ----------------------------------------------------------------

function find(target: string): { el?: HTMLElement; error?: string } {
  const t = target.trim();
  if (/^e\d+$/i.test(t)) {
    const el = document.querySelector(`[data-agent-ref="${t.toLowerCase()}"]`);
    if (!el || !document.contains(el)) return { error: `${t} is no longer on the screen. Here is the current screen:` };
    if (ignored(el)) return { error: `${t} is part of the assistant itself and can't be used.` };
    return { el: el as HTMLElement };
  }
  const needle = t.toLowerCase();
  const all = actionable();
  const exact = all.filter((el) => labelOf(el).toLowerCase() === needle);
  const partial = exact.length ? exact : all.filter((el) => labelOf(el).toLowerCase().includes(needle));
  if (partial.length === 1) return { el: partial[0] as HTMLElement };
  if (!partial.length) return { error: `Nothing on screen is labelled "${t}".` };
  const options = partial.slice(0, 6).map((el) => `${refFor(el)} "${labelOf(el)}"`).join(', ');
  return { error: `"${t}" matches ${partial.length} elements (${options}). Use a ref.` };
}

let overlay: HTMLDivElement | null = null;

/** Show the user which element the assistant is about to touch. */
async function highlight(el: HTMLElement) {
  el.scrollIntoView({ block: 'center', inline: 'nearest', behavior: 'smooth' });
  await sleep(250);
  const r = el.getBoundingClientRect();
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.setAttribute('data-agent-ignore', '');
    overlay.setAttribute('data-testid', 'agent-highlight');
    Object.assign(overlay.style, {
      position: 'fixed', pointerEvents: 'none', zIndex: '2147483646', borderRadius: '8px',
      boxShadow: '0 0 0 3px rgba(37,99,235,.9), 0 0 0 7px rgba(37,99,235,.25)', transition: 'all 120ms ease',
    });
    document.body.appendChild(overlay);
  }
  Object.assign(overlay.style, {
    display: 'block', left: `${r.left - 3}px`, top: `${r.top - 3}px`, width: `${r.width + 6}px`, height: `${r.height + 6}px`,
  });
  window.setTimeout(() => {
    if (overlay) overlay.style.display = 'none';
  }, 1400);
}

/** Set a value so React sees it: its onChange listens to the native setter + input event. */
function setNativeValue(el: HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement, value: string) {
  const proto = Object.getPrototypeOf(el);
  const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
  if (setter) setter.call(el, value);
  else el.value = value;
}

function keyEvent(el: Element, type: 'keydown' | 'keyup' | 'keypress', key: string) {
  const code = key === ' ' || key === 'Space' ? 'Space' : key;
  el.dispatchEvent(new KeyboardEvent(type, { key: key === 'Space' ? ' ' : key, code, bubbles: true, cancelable: true }));
}

function pressOn(el: Element, key: string) {
  keyEvent(el, 'keydown', key);
  if (key === 'Enter') {
    keyEvent(el, 'keypress', key);
    const form = (el as HTMLInputElement).form;
    if (form) form.requestSubmit();
  }
  if (key === 'Tab') {
    const focusables = Array.from(document.querySelectorAll<HTMLElement>(INTERACTIVE)).filter((f) => !ignored(f) && visible(f));
    const i = focusables.indexOf(el as HTMLElement);
    focusables[(i + 1) % focusables.length]?.focus();
  }
  if ((key === 'Backspace' || key === 'Delete') && (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement)) {
    setNativeValue(el, el.value.slice(0, -1));
    el.dispatchEvent(new Event('input', { bubbles: true }));
  }
  keyEvent(el, 'keyup', key);
}

function scrollContainer(): HTMLElement | Window {
  // The app scrolls inside its main column, not the window.
  const candidates = Array.from(document.querySelectorAll<HTMLElement>('main, div'))
    .filter((el) => !ignored(el) && el.scrollHeight > el.clientHeight + 40 && /(auto|scroll)/.test(getComputedStyle(el).overflowY) && visible(el))
    .sort((a, b) => b.clientWidth * b.clientHeight - a.clientWidth * a.clientHeight);
  return candidates[0] || window;
}

async function run(command: UiCommand): Promise<UiResult> {
  if (command.op === 'snapshot') return { ok: true, text: snapshot() };

  if (command.op === 'scroll') {
    const box = scrollContainer();
    const height = box === window ? window.innerHeight : (box as HTMLElement).clientHeight;
    const top = command.direction === 'top' ? 0 : command.direction === 'bottom' ? 1e9 : undefined;
    if (top !== undefined) box.scrollTo({ top, behavior: 'smooth' });
    else box.scrollBy({ top: (command.direction === 'up' ? -1 : 1) * height * 0.8, behavior: 'smooth' });
    await settle(2500, 300);
    return { ok: true, text: snapshot(`Scrolled ${command.direction}.`) };
  }

  if (command.op === 'key' && !command.target) {
    const el = (document.activeElement && !ignored(document.activeElement) ? document.activeElement : document.body) as HTMLElement;
    pressOn(el, command.key || 'Enter');
    await settle();
    return { ok: true, text: snapshot(`Pressed ${command.key}.`) };
  }

  const { el, error } = find(command.target || '');
  if (!el) return { ok: false, text: `${error}\n${snapshot()}` };
  if ((el as HTMLButtonElement).disabled) return { ok: false, text: `${command.target} is disabled right now.\n${snapshot()}` };
  const name = `${refFor(el)} "${labelOf(el)}"`;
  await highlight(el);

  switch (command.op) {
    case 'click': {
      el.focus({ preventScroll: true });
      for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup'] as const) {
        el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
      }
      el.click();
      await settle();
      return { ok: true, text: snapshot(`Clicked ${name}.`) };
    }
    case 'type': {
      if (!(el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement)) {
        return { ok: false, text: `${name} is not a text box.\n${snapshot()}` };
      }
      if (el instanceof HTMLInputElement && el.type === 'password') {
        return { ok: false, text: `${name} is a password field; the user has to fill that in themselves.\n${snapshot()}` };
      }
      el.focus({ preventScroll: true });
      setNativeValue(el, '');
      el.dispatchEvent(new Event('input', { bubbles: true }));
      // Type visibly, a character at a time, capped so long text isn't slow.
      const text = command.text || '';
      const delay = Math.max(8, Math.min(45, 900 / Math.max(1, text.length)));
      for (const ch of text) {
        keyEvent(el, 'keydown', ch);
        setNativeValue(el, el.value + ch);
        el.dispatchEvent(new Event('input', { bubbles: true }));
        keyEvent(el, 'keyup', ch);
        await sleep(delay);
      }
      if (command.press_enter) {
        await settle(2500, 250);
        pressOn(el, 'Enter');
      }
      await settle();
      return { ok: true, text: snapshot(`Typed "${text}" into ${name}${command.press_enter ? ' and pressed Enter' : ''}.`) };
    }
    case 'key': {
      el.focus({ preventScroll: true });
      pressOn(el, command.key || 'Enter');
      await settle();
      return { ok: true, text: snapshot(`Pressed ${command.key} on ${name}.`) };
    }
    case 'select': {
      if (!(el instanceof HTMLSelectElement)) return { ok: false, text: `${name} is not a dropdown.\n${snapshot()}` };
      const wanted = (command.option || '').toLowerCase();
      const options = Array.from(el.options);
      const match =
        options.find((o) => o.text.trim().toLowerCase() === wanted || o.value.toLowerCase() === wanted) ||
        options.find((o) => o.text.toLowerCase().includes(wanted));
      if (!match) {
        return { ok: false, text: `${name} has no option "${command.option}". Options: ${options.map((o) => o.text.trim()).join(' | ')}` };
      }
      setNativeValue(el, match.value);
      el.dispatchEvent(new Event('change', { bubbles: true }));
      await settle();
      return { ok: true, text: snapshot(`Selected "${match.text.trim()}" in ${name}.`) };
    }
    default:
      return { ok: false, text: `Unknown command ${command.op}.` };
  }
}

/** Run one command; never throws, since the model is waiting on an answer either way. */
export async function executeUiCommand(command: UiCommand): Promise<UiResult> {
  patchFetch();
  try {
    return await run(command);
  } catch (err) {
    let screen = '';
    try {
      screen = snapshot();
    } catch {
      /* the page itself is broken */
    }
    return { ok: false, text: `The action failed in the browser (${(err as Error)?.message || err}).\n${screen}` };
  }
}

// Start counting requests as soon as the app loads, not at the first command,
// so a request already running when the first command arrives is seen.
patchFetch();
