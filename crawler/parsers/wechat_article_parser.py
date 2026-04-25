from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any


@dataclass
class ParsedArticle:
    article_id: str
    source_url: str
    crawl_time: str
    parser_version: str
    title: str
    author: str
    account_name: str
    publish_time: str
    content_html: str
    content_text: str
    html_sha256: str
    status: str
    error: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "article_id": self.article_id,
            "source_url": self.source_url,
            "crawl_time": self.crawl_time,
            "parser_version": self.parser_version,
            "title": self.title,
            "author": self.author,
            "account_name": self.account_name,
            "publish_time": self.publish_time,
            "content_html": self.content_html,
            "content_text": self.content_text,
            "html_sha256": self.html_sha256,
            "status": self.status,
            "error": self.error,
        }


class WechatArticleParser:
    PARSER_VERSION = "v1.0.0"

    @staticmethod
    def _extract_by_selectors(payload: dict[str, Any], selectors: list[str]) -> str:
        for key in selectors:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _normalize_text(text: str) -> str:
        text = text.replace("\u00a0", " ").replace("\r", "\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    @staticmethod
    def _safe_json_loads(raw: str) -> dict[str, Any]:
        try:
            loaded = json.loads(raw)
            return loaded if isinstance(loaded, dict) else {}
        except json.JSONDecodeError:
            return {}

    def parse(self, source_url: str, extracted_payload: dict[str, Any], html: str) -> ParsedArticle:
        now = datetime.now(timezone.utc).isoformat()
        html_sha256 = hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()
        content_text = self._normalize_text(extracted_payload.get("content_text", ""))
        title = self._extract_by_selectors(extracted_payload, ["title", "msg_title", "window_title"])
        author = self._extract_by_selectors(extracted_payload, ["author", "writer"])
        account_name = self._extract_by_selectors(extracted_payload, ["account_name", "biz_name"])
        publish_time = self._extract_by_selectors(extracted_payload, ["publish_time", "ct", "create_time"])

        if re.fullmatch(r"\d{10}", publish_time):
            publish_time = datetime.fromtimestamp(int(publish_time), tz=timezone.utc).isoformat()

        article_id = hashlib.sha256(source_url.encode("utf-8")).hexdigest()[:16]
        status = "success" if content_text else "empty"
        error = "" if status == "success" else "content_text_empty"

        return ParsedArticle(
            article_id=article_id,
            source_url=source_url,
            crawl_time=now,
            parser_version=self.PARSER_VERSION,
            title=title,
            author=author,
            account_name=account_name,
            publish_time=publish_time,
            content_html=extracted_payload.get("content_html", ""),
            content_text=content_text,
            html_sha256=html_sha256,
            status=status,
            error=error,
        )
