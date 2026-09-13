"""What a storyline is about, without leaving the app.

A storyline row is a headline and, at best, a one-line description. Opening one
shows a short summary and what it means for the player in fantasy terms:

* The article text comes from ESPN's content API for stories. Rotowire items are
  already the whole blurb, and videos have only their description.
* A model writes the summary through the same inference path as the assistant:
  the user's own key if they have one set, else the free house tier. That draws on
  the same daily question allowance, since it is the same budget.
* Without a model (none configured, allowance used, or the call failed) the
  summary is the article's own opening sentences about the player, labelled as
  such. The popup never comes back empty.

AI summaries are cached per article and shared: the second person to open a
story costs nothing.
"""

from __future__ import annotations

import html
import json
import re
import threading
import urllib.request
from collections import OrderedDict
from html.parser import HTMLParser

from ..config import logger
from . import llm_proxy as proxy

CONTENT_URL = "https://content.core.api.espn.com/v1/sports/news/{article_id}"
HTTP_TIMEOUT = 10
MAX_ARTICLE_CHARS = 6000     # what the model reads; long features are mostly color after this
EXCERPT_CHARS = 700          # what the popup shows of the article itself
SUMMARY_MAX_TOKENS = 400
CACHE_SIZE = 500

_summaries: "OrderedDict[str, dict]" = OrderedDict()
_texts: "OrderedDict[str, str]" = OrderedDict()
_lock = threading.Lock()


def _remember(store: OrderedDict, key: str, value) -> None:
    with _lock:
        store[key] = value
        store.move_to_end(key)
        while len(store) > CACHE_SIZE:
            store.popitem(last=False)


def _recall(store: OrderedDict, key: str):
    with _lock:
        value = store.get(key)
        if value is not None:
            store.move_to_end(key)
        return value


# --- article text ---------------------------------------------------------------

class _Text(HTMLParser):
    """Paragraph text from ESPN story HTML, dropping embeds like <video1>."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.paragraphs: list[str] = []
        self._buf: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in ("p", "h2", "h3", "li", "br"):
            self._flush()

    def handle_endtag(self, tag):
        if tag in ("p", "h2", "h3", "li"):
            self._flush()

    def handle_data(self, data):
        self._buf.append(data)

    def _flush(self):
        text = re.sub(r"\s+", " ", "".join(self._buf)).strip()
        self._buf = []
        # "<video1>" and similar placeholders parse as unknown tags; what is left is empty.
        if text and not re.fullmatch(r"<?\w+\d>?", text):
            self.paragraphs.append(text)

    def close(self):
        super().close()
        self._flush()


def html_to_paragraphs(story_html: str) -> list[str]:
    parser = _Text()
    parser.feed(story_html or "")
    parser.close()
    return parser.paragraphs


def _fetch_story(article_id: str) -> str:
    url = CONTENT_URL.format(article_id=article_id)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        data = json.load(resp) or {}
    headline = (data.get("headlines") or [{}])[0]
    return "\n\n".join(html_to_paragraphs(headline.get("story") or ""))


def article_text(item: dict) -> str:
    """The best available text for a storyline row."""
    article_id = str(item.get("article_id") or "")
    headline = (item.get("headline") or "").strip()
    description = (item.get("description") or "").strip()
    kind = (item.get("story_type") or "").lower()

    if kind in ("story", "headlinenews", "preview", "recap") and article_id.isdigit():
        cached = _recall(_texts, article_id)
        if cached is None:
            try:
                cached = _fetch_story(article_id)
            except Exception as exc:
                logger.info("storyline %s: article text unavailable (%s)", article_id, exc)
                cached = ""
            _remember(_texts, article_id, cached)
        if cached:
            return cached
    return "\n\n".join(part for part in (headline, description) if part)


# --- summaries --------------------------------------------------------------------

def _sentences(text: str) -> list[str]:
    """Sentences, split within paragraphs so a subheading never fuses onto the next line.

    ESPN recaps use unpunctuated subheads ("San Francisco 49ers (1-0)"); flattening
    the text first glued them into the following sentence.
    """
    paragraphs = [re.sub(r"\s+", " ", p).strip() for p in re.split(r"\n\s*\n", text or "")]
    paragraphs = [p for p in paragraphs if p]
    prose = [p for p in paragraphs if re.search(r"[.!?][\"')\]]*$", p)]
    # Skip headings only when there is prose to skip them for: a bare headline
    # ("Purdy throws three TDs") is still the whole story.
    out: list[str] = []
    for paragraph in prose or paragraphs:
        out.extend(s.strip() for s in re.split(r"(?<=[.!?])\s+(?=[A-Z\"'])", paragraph) if s.strip())
    return out


def extract_summary(text: str, player_name: str, limit: int = 3) -> str:
    """The article's own sentences about the player: no model, nothing invented."""
    sentences = _sentences(text)
    if not sentences:
        return ""
    surname = (player_name or "").replace(".", "").split()
    surname = [t for t in surname if t.lower() not in ("jr", "sr", "ii", "iii", "iv")]
    key = surname[-1].lower() if surname else ""
    about = [s for s in sentences if key and key in s.lower()]
    chosen = (about or sentences)[:limit]
    return " ".join(chosen)


def excerpt(text: str) -> str:
    if len(text) <= EXCERPT_CHARS:
        return text
    cut = text[:EXCERPT_CHARS]
    return cut[: cut.rfind(" ")].rstrip(",;: ") + "…"


def _prompt(player_name: str, item: dict, text: str) -> list[dict]:
    body = text[:MAX_ARTICLE_CHARS]
    return [
        {
            "role": "system",
            "content": (
                "You summarize NFL news for fantasy football players. Use only the article. "
                "Never invent stats, injuries or quotes. Answer in exactly this format:\n"
                "SUMMARY: <two or three plain sentences on what happened>\n"
                "POINTS:\n- <short point>\n- <short point>\n- <short point>\n"
                "FANTASY: <one sentence on what this means for the named player's fantasy value, "
                "or 'No clear fantasy impact.' if the article does not say>"
            ),
        },
        {
            "role": "user",
            "content": f"Player: {player_name}\nHeadline: {item.get('headline', '')}\n\nArticle:\n{body}",
        },
    ]


def parse_model_summary(raw: str) -> dict | None:
    """SUMMARY / POINTS / FANTASY sections out of a model reply. None if unusable."""
    text = (raw or "").replace("**", "").strip()
    # Some free models think out loud before answering; keep only the answer.
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    summary = re.search(r"SUMMARY:\s*(.+?)(?=\n\s*POINTS:|\n\s*FANTASY:|\Z)", text, re.S | re.I)
    points = re.search(r"POINTS:\s*(.+?)(?=\n\s*FANTASY:|\Z)", text, re.S | re.I)
    fantasy = re.search(r"FANTASY:\s*(.+)", text, re.S | re.I)
    if not summary:
        return None
    bullets = []
    if points:
        for line in points.group(1).splitlines():
            line = re.sub(r"^\s*[-*•\d.)]+\s*", "", line).strip()
            if line:
                bullets.append(line)
    return {
        "text": re.sub(r"\s+", " ", summary.group(1)).strip(),
        "key_points": bullets[:4],
        "fantasy_impact": re.sub(r"\s+", " ", fantasy.group(1)).strip() if fantasy else None,
    }


def cached_summary(article_id: str) -> dict | None:
    return _recall(_summaries, str(article_id))


async def model_summary(player_name: str, item: dict, text: str, upstream: "proxy.Upstream") -> dict:
    """Ask a model. Raises RuntimeError with a user-facing reason on failure."""
    response = await proxy.forward(
        {"messages": _prompt(player_name, item, text), "max_tokens": SUMMARY_MAX_TOKENS,
         "temperature": 0.2, "stream": False},
        upstream,
    )
    payload = json.loads(response.body)
    if response.status_code >= 400:
        raise RuntimeError((payload.get("error") or {}).get("message") or f"model returned {response.status_code}")
    choice = (payload.get("choices") or [{}])[0]
    content = (choice.get("message") or {}).get("content") or ""
    parsed = parse_model_summary(html.unescape(content))
    if parsed is None:
        raise RuntimeError("The model's reply wasn't a usable summary.")
    parsed.update({"source": "ai", "model": payload.get("model") or upstream.models[0], "tier": upstream.tier})
    if upstream.tier == "house":
        # Shared across users: the same story never costs the free tier twice.
        _remember(_summaries, str(item.get("article_id")), parsed)
    return parsed


def fallback_summary(player_name: str, text: str, note: str | None) -> dict:
    return {
        "text": extract_summary(text, player_name),
        "key_points": [],
        "fantasy_impact": None,
        "source": "extract",
        "note": note,
    }
