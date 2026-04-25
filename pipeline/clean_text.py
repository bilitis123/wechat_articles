from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import unicodedata


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\u00a0", " ").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    noise_patterns = [
        r"阅读原文",
        r"轻触.*阅读原文",
        r"点击.*关注",
        r"欢迎.*转发",
        r"免责声明[:：]?.*",
        r"^\s*赞\s*$",
        r"^\s*分享\s*$",
        r".*推荐.*",
        r".*在看.*",
    ]
    lines: list[str] = []
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        if any(re.search(pattern, line, flags=re.IGNORECASE) for pattern in noise_patterns):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def chinese_ratio(text: str) -> float:
    if not text:
        return 0.0
    chinese_chars = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    return chinese_chars / len(text)


def run(input_file: Path, output_file: Path, min_length: int, min_cn_ratio: float) -> None:
    seen_hash = set()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with input_file.open("r", encoding="utf-8") as source, output_file.open("w", encoding="utf-8") as sink:
        for line in source:
            if not line.strip():
                continue
            item = json.loads(line)
            original = item.get("content_text", "")
            cleaned = normalize_text(original)
            digest = hashlib.sha256(cleaned.encode("utf-8")).hexdigest() if cleaned else ""
            quality_flags = []
            if len(cleaned) < min_length:
                quality_flags.append("too_short")
            if chinese_ratio(cleaned) < min_cn_ratio:
                quality_flags.append("low_chinese_ratio")
            if digest in seen_hash and digest:
                quality_flags.append("duplicate")
            seen_hash.add(digest)

            item["clean_text"] = cleaned
            item["clean_text_sha256"] = digest
            item["quality_flags"] = quality_flags
            item["quality_passed"] = len(quality_flags) == 0
            sink.write(json.dumps(item, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean fetched article text and add quality gates.")
    parser.add_argument("--input", default=Path("outputs/fetched_articles.jsonl"), type=Path)
    parser.add_argument("--output", default=Path("outputs/clean_articles.jsonl"), type=Path)
    parser.add_argument("--min-length", default=120, type=int)
    parser.add_argument("--min-cn-ratio", default=0.25, type=float)
    args = parser.parse_args()
    run(args.input, args.output, args.min_length, args.min_cn_ratio)


if __name__ == "__main__":
    main()
