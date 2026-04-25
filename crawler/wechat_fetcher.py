from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
from typing import Any

import pandas as pd

from crawler.parsers.wechat_article_parser import WechatArticleParser


@dataclass
class CrawlConfig:
    timeout_ms: int = 30_000
    retries: int = 2
    wait_after_load_ms: int = 1_500


class WechatFetcher:
    def __init__(self, config: CrawlConfig, output_dir: Path) -> None:
        self.config = config
        self.output_dir = output_dir
        self.content_dir = output_dir / "raw_html_content"
        
        # 只创建content目录
        self.content_dir.mkdir(parents=True, exist_ok=True)
        self.parser = WechatArticleParser()

    @staticmethod
    def _sanitize_filename(value: str, fallback: str) -> str:
        cleaned = re.sub(r"[\\/:*?\"<>|]", "_", value).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = cleaned[:80].strip()
        return cleaned or fallback

    @staticmethod
    def _sanitize_folder_name(value: str) -> str:
        """Sanitize folder name for account"""
        cleaned = re.sub(r"[\\/:*?\"<>|]", "_", value).strip()
        cleaned = re.sub(r"\s+", "_", cleaned)
        cleaned = cleaned[:50].strip()
        return cleaned or "unknown_account"

    @staticmethod
    def _normalize_date(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return ""
        match = re.search(r"(\d{4})[-/年.](\d{1,2})[-/月.](\d{1,2})", text)
        if match:
            y, m, d = match.groups()
            return f"{int(y):04d}{int(m):02d}{int(d):02d}"
        if re.fullmatch(r"\d{8}", text):
            return text
        return ""

    @staticmethod
    def _load_account_filters(accounts: str, accounts_file: Path | None) -> set[str]:
        result = set()
        if accounts:
            for item in accounts.split(","):
                name = item.strip()
                if name:
                    result.add(name)
        if accounts_file:
            if accounts_file.exists():
                for line in accounts_file.read_text(encoding="utf-8").splitlines():
                    name = line.strip()
                    if name:
                        result.add(name)
            else:
                raise FileNotFoundError(f"Accounts file not found: {accounts_file}")
        return result

    @staticmethod
    def _load_records(path: Path) -> list[dict[str, str]]:
        url_pattern = re.compile(r"https?://[^\s\"'<>]+")

        def dedupe_keep_order(items: list[dict[str, str]]) -> list[dict[str, str]]:
            seen = set()
            result = []
            for item in items:
                url = item.get("source_url", "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                result.append(item)
            return result

        def infer_account_field(row_map: dict[str, str]) -> str:
            for key, value in row_map.items():
                k = key.lower()
                if "公众号" in key or "账号" in key or "author" in k or "account" in k or "来源" in key:
                    return value
            return ""

        if path.suffix.lower() in {".xlsx", ".xls"}:
            # 读取所有Sheet而不是只读第一个
            excel_file = pd.ExcelFile(path)
            all_records = []
            
            for sheet_name in excel_file.sheet_names:
                logging.info(f"Loading data from sheet: {sheet_name}")
                
                # 读取当前Sheet
                df = pd.read_excel(path, sheet_name=sheet_name)
                
                records: list[dict[str, str]] = []
                for _, row in df.iterrows():
                    row_map = {str(col): str(row[col]).strip() for col in df.columns if not pd.isna(row[col])}
                    row_url = ""
                    row_title = ""
                    row_date = ""
                    row_account = infer_account_field(row_map)

                    for value in row.values.tolist():
                        if pd.isna(value):
                            continue
                        text = str(value).strip()
                        if not text:
                            continue
                        if not row_url:
                            matched = url_pattern.findall(text)
                            if matched:
                                row_url = matched[0]
                                continue
                        if not row_date:
                            row_date = WechatFetcher._normalize_date(value)
                        if not row_title and not url_pattern.search(text) and len(text) >= 4:
                            row_title = text

                    if row_url:
                        # 使用Sheet名称作为公众号名称
                        record = {
                            "source_url": row_url,
                            "input_title": row_title,
                            "input_publish_date": row_date,
                            "input_account_name": sheet_name,  # 使用Sheet名称作为公众号名称
                            "source_sheet": sheet_name  # 记录来源Sheet
                        }
                        records.append(record)
                
                all_records.extend(records)
                logging.info(f"Found {len(records)} URLs in sheet '{sheet_name}'")
            
            logging.info(f"Total {len(all_records)} URLs found across all sheets")
            return dedupe_keep_order(all_records)

        if path.suffix.lower() == ".json":
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)
            if isinstance(data, list):
                return dedupe_keep_order(
                    [
                        {
                            "source_url": str(item).strip(),
                            "input_title": "",
                            "input_publish_date": "",
                            "input_account_name": "",
                        }
                        for item in data
                    ]
                )
            raise ValueError("JSON input must be a URL list.")

        if path.suffix.lower() == ".csv":
            rows = path.read_text(encoding="utf-8").splitlines()
            records: list[dict[str, str]] = []
            for row in rows[1:]:
                for hit in url_pattern.findall(row):
                    records.append(
                        {
                            "source_url": hit,
                            "input_title": "",
                            "input_publish_date": "",
                            "input_account_name": "",
                        }
                    )
            return dedupe_keep_order(records)

        return dedupe_keep_order(
            [
                {
                    "source_url": hit,
                    "input_title": "",
                    "input_publish_date": "",
                    "input_account_name": "",
                }
                for hit in url_pattern.findall(path.read_text(encoding="utf-8"))
            ]
        )

    @staticmethod
    def _article_script() -> str:
        return """
        () => {
          const textNode = document.querySelector('#js_content');
          const titleNode = document.querySelector('#activity-name') || document.querySelector('#js_msg_title');
          const authorNode = document.querySelector('#js_name') || document.querySelector('.rich_media_meta_text');
          const accountNode = document.querySelector('#js_profile_qrcode > div > strong') || document.querySelector('#js_profile_qrcode > div > p');
          const publishNode = document.querySelector('#publish_time') || document.querySelector('#js_publish_time');
          return {
            title: titleNode ? titleNode.innerText.trim() : document.title,
            author: authorNode ? authorNode.innerText.trim() : '',
            account_name: accountNode ? accountNode.innerText.trim() : '',
            publish_time: publishNode ? publishNode.innerText.trim() : '',
            raw_html_content: textNode ? textNode.innerHTML : '',
            content_text: textNode ? textNode.innerText : '',
            video_count: document.querySelectorAll('video').length,
            iframe_count: document.querySelectorAll('iframe').length
          };
        }
        """

    @staticmethod
    def _date_in_range(day: str, start_date: str, end_date: str) -> bool:
        if not day:
            return True
        if start_date and day < start_date:
            return False
        if end_date and day > end_date:
            return False
        return True

    async def _fetch_one(self, page: Any, record: dict[str, str]) -> dict[str, Any]:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError

        url = record["source_url"]
        for attempt in range(1, self.config.retries + 2):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=self.config.timeout_ms)
                await page.wait_for_timeout(self.config.wait_after_load_ms)
                full_html = await page.content()
                extracted = await page.evaluate(self._article_script())
                parsed = self.parser.parse(url, extracted, full_html)

                publish_date = record.get("input_publish_date", "") or self._normalize_date(parsed.publish_time)
                if not publish_date:
                    publish_date = datetime.now(timezone.utc).strftime("%Y%m%d")
                
                title = record.get("input_title", "") or parsed.title or parsed.article_id
                # 使用Sheet名称作为公众号名称
                account_name = record.get("input_account_name", "") or record.get("source_sheet", "unknown")
                
                # 生成安全的文件名和账户文件夹名
                safe_name = f"{publish_date}_{self._sanitize_filename(title, parsed.article_id)}.html"
                account_folder_name = self._sanitize_folder_name(account_name)
                
                # 创建账户专属目录（使用Sheet名称作为文件夹名）
                account_content_dir = self.content_dir / account_folder_name
                account_content_dir.mkdir(parents=True, exist_ok=True)
                
                # 只保存content部分
                content_path = account_content_dir / safe_name
                content_path.write_text(extracted.get("raw_html_content", ""), encoding="utf-8")
                
                content_html_path = str(content_path)

                result = parsed.as_dict()
                if record.get("input_title"):
                    result["title"] = record["input_title"]
                if record.get("input_publish_date"):
                    result["publish_time"] = record["input_publish_date"]
                if record.get("input_account_name"):
                    result["account_name"] = record["input_account_name"]

                # 添加来源Sheet信息到结果中
                if record.get("source_sheet"):
                    result["source_sheet"] = record["source_sheet"]

                media_flags = []
                if int(extracted.get("video_count", 0)) > 0:
                    media_flags.append("video")
                if int(extracted.get("iframe_count", 0)) > 0:
                    media_flags.append("iframe")
                result["media_flags"] = media_flags
                result["content_html_path"] = content_html_path
                # 移除raw_html_path字段，因为我们不再保存完整HTML
                result.pop("raw_html_path", None)
                return result
            except PlaywrightTimeoutError as exc:
                logging.warning("Timeout for %s (attempt %s): %s", url, attempt, exc)
                error_text = "timeout"
            except OSError as exc:
                logging.warning("Failed for %s (attempt %s): %s", url, attempt, exc)
                error_text = str(exc)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Failed for %s (attempt %s): %s", url, attempt, exc)
                error_text = str(exc)

            if attempt > self.config.retries:
                ts = datetime.now(timezone.utc).isoformat()
                # 使用Sheet名称作为公众号名称
                account_name = record.get("input_account_name", "") or record.get("source_sheet", "unknown")
                account_folder_name = self._sanitize_folder_name(account_name)
                
                # 为失败的文章也创建账户专属目录
                account_content_dir = self.content_dir / account_folder_name
                account_content_dir.mkdir(parents=True, exist_ok=True)
                
                return {
                    "article_id": f"failed_{abs(hash(url))}",
                    "source_url": url,
                    "crawl_time": ts,
                    "parser_version": WechatArticleParser.PARSER_VERSION,
                    "title": "",
                    "author": "",
                    "account_name": record.get("input_account_name", ""),
                    "publish_time": record.get("input_publish_date", ""),
                    "raw_html_content": "",
                    "content_text": "",
                    "html_sha256": "",
                    "status": "failed",
                    "error": error_text,
                    "content_html_path": "",  # 失败时不保存文件
                    "media_flags": [],
                    "source_sheet": record.get("source_sheet", ""),
                }
            await page.wait_for_timeout(800)
        raise RuntimeError("Unexpected retry loop exit.")

    async def run(
        self,
        input_file: Path,
        output_file: Path,
        max_urls: int = 0,
        start_date: str = "",
        end_date: str = "",
        account_filters: set[str] | None = None,
    ) -> None:
        from playwright.async_api import async_playwright

        records = self._load_records(input_file)
        existing_urls = set()
        if output_file.exists():
            with output_file.open("r", encoding="utf-8") as file:
                for line in file:
                    if line.strip():
                        existing_urls.add(json.loads(line).get("source_url"))

        filtered_records = []
        for record in records:
            day = record.get("input_publish_date", "")
            if not self._date_in_range(day, start_date, end_date):
                continue
            if account_filters:
                account = record.get("input_account_name", "")
                if not account or account not in account_filters:
                    continue
            if record["source_url"] in existing_urls:
                continue
            filtered_records.append(record)

        if max_urls > 0:
            filtered_records = filtered_records[:max_urls]

        logging.info("Loaded %s URLs from all sheets, pending %s", len(records), len(filtered_records))
        unprocessed_file = self.output_dir / "unprocessed_elements.jsonl"

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            with output_file.open("a", encoding="utf-8") as sink, unprocessed_file.open("a", encoding="utf-8") as flag_sink:
                for i, record in enumerate(filtered_records):
                    data = await self._fetch_one(page, record)
                    
                    # 写入主结果文件
                    sink.write(json.dumps(data, ensure_ascii=False) + "\n")
                    
                    # 如果有媒体元素，写入未处理元素文件
                    if data.get("media_flags"):
                        flag_sink.write(
                            json.dumps(
                                {
                                    "article_id": data.get("article_id", ""),
                                    "source_url": data.get("source_url", ""),
                                    "title": data.get("title", ""),
                                    "content_html_path": data.get("content_html_path", ""),
                                    "unprocessed_elements": data["media_flags"],
                                    "source_sheet": data.get("source_sheet", ""),  # 添加来源Sheet信息
                                    "account_name": data.get("account_name", "")  # 添加公众号名称
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
            await browser.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch WeChat article content by URLs.")
    parser.add_argument("--input", required=True, type=Path, help="Input URL file (txt/csv/json/xlsx).")
    parser.add_argument("--output", default=Path("outputs/fetched_articles.jsonl"), type=Path)
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--wait-ms", type=int, default=1_500)
    parser.add_argument("--max-urls", type=int, default=0, help="Limit fetch count for validation run (0 = all).")
    parser.add_argument("--start-date", default="", help="Start date filter, format YYYYMMDD.")
    parser.add_argument("--end-date", default="", help="End date filter, format YYYYMMDD.")
    parser.add_argument("--accounts", default="", help="Comma-separated account names to include.")
    parser.add_argument("--accounts-file", type=Path, default=None, help="Text file for account names (one per line).")
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("outputs/crawl.log", encoding="utf-8"), logging.StreamHandler()],
    )
    args = build_parser().parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    config = CrawlConfig(timeout_ms=args.timeout_ms, retries=args.retries, wait_after_load_ms=args.wait_ms)
    fetcher = WechatFetcher(config=config, output_dir=args.output.parent)
    account_filters = fetcher._load_account_filters(args.accounts, args.accounts_file)
    asyncio.run(
        fetcher.run(
            input_file=args.input,
            output_file=args.output,
            max_urls=args.max_urls,
            start_date=args.start_date,
            end_date=args.end_date,
            account_filters=account_filters,
        )
    )


if __name__ == "__main__":
    main()


