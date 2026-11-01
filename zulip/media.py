"""Zulip attachment helpers: find upload links in raw Markdown, classify and cache downloads,
format outbound upload links.

Zulip has no attachment list on a message: an upload is a Markdown link to ``/user_uploads/...``
(relative, or absolute on the same server) anywhere in the content, and one message may carry any
number of them.
"""

from __future__ import annotations

import mimetypes
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

from gateway.platforms.event import MessageType

try:
    from gateway.platforms.event import clean_attachment_filename
except ImportError:  # Hermes < 0.23: the helper landed upstream after v0.22.5
    import unicodedata

    def clean_attachment_filename(name: object) -> str:
        """Upstream's helper, verbatim: an untrusted filename reduced to a display-safe basename."""
        if not isinstance(name, str):
            return ""
        base = name.replace("\\", "/").rsplit("/", 1)[-1]
        base = "".join(ch for ch in base if unicodedata.category(ch) not in {"Cc", "Cf"}).strip()
        if base in {"", ".", ".."}:
            return ""
        if len(base) > 255:
            stem, dot, ext = base.rpartition(".")
            keep = f"{dot}{ext}" if stem and len(ext) <= 16 else ""
            base = base[: 255 - len(keep)] + keep
        return base

_UPLOAD_LINK_RE = re.compile(r"\[([^\]\n]*)\]\(((?:https?://[^\s)/]+)?/user_uploads/[^\s)]+)\)")

_CATEGORY_MESSAGE_TYPES = {"image": MessageType.PHOTO, "audio": MessageType.VOICE,
                           "video": MessageType.VIDEO}


@dataclass(frozen=True)
class UploadLink:
    name: str
    path: str  # "/user_uploads/..." on this server
    markdown: str  # the original link text, for in-place replacement


def find_uploads(content: str, site: str) -> list[UploadLink]:
    """Upload links on ``site`` in order of appearance (absolute links to other hosts are skipped)."""
    site_host = urlparse(site).netloc.lower()
    links: list[UploadLink] = []
    for match in _UPLOAD_LINK_RE.finditer(content or ""):
        label, url = match.group(1), match.group(2)
        parsed = urlparse(url)
        if parsed.netloc and parsed.netloc.lower() != site_host:
            continue
        name = clean_attachment_filename(label) or clean_attachment_filename(unquote(parsed.path.rsplit("/", 1)[-1]))
        links.append(UploadLink(name=name or "attachment", path=parsed.path, markdown=match.group(0)))
    return links


def replace_link(content: str, link: UploadLink, replacement: str) -> str:
    return content.replace(link.markdown, replacement, 1)


def category(mime: str, name: str) -> str:
    mime = (mime or "").split(";")[0].strip().lower()
    if not mime or mime == "application/octet-stream":
        mime = mimetypes.guess_type(name)[0] or ""
    head = mime.split("/", 1)[0]
    return head if head in _CATEGORY_MESSAGE_TYPES else "document"


def effective_mime(mime: str, name: str) -> str:
    mime = (mime or "").split(";")[0].strip().lower()
    if not mime or mime == "application/octet-stream":
        return mimetypes.guess_type(name)[0] or "application/octet-stream"
    return mime


def message_type_for(mimes: list[str]) -> MessageType:
    """PHOTO only when every attachment is an image; a lone audio/video keeps its own type."""
    categories = {category(m, "") for m in mimes}
    if len(categories) == 1:
        return _CATEGORY_MESSAGE_TYPES.get(categories.pop(), MessageType.DOCUMENT)
    return MessageType.DOCUMENT


async def cache_download(data: bytes, name: str, mime: str) -> str:
    """Store a downloaded attachment in Hermes's media cache; returns the local path."""
    from gateway.platforms.base import (
        cache_audio_from_bytes_async, cache_document_from_bytes_async,
        cache_image_from_bytes_async, cache_video_from_bytes_async)
    ext = Path(name).suffix or mimetypes.guess_extension(mime) or ""
    kind = category(mime, name)
    if kind == "image":
        return await cache_image_from_bytes_async(data, ext or ".png")
    if kind == "audio":
        return await cache_audio_from_bytes_async(data, ext or ".ogg")
    if kind == "video":
        return await cache_video_from_bytes_async(data, ext or ".mp4")
    return await cache_document_from_bytes_async(data, name)


def upload_markdown(name: str, url: str) -> str:
    """Link text Zulip's own compose box produces for an upload (``[`` / ``]`` stripped)."""
    label = (clean_attachment_filename(name) or "file").replace("[", "").replace("]", "")
    return f"[{label}]({url})"
