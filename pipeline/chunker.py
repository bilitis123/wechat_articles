from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re


def split_chunks(text: str, max_chars: int = 420, overlap: int = 60) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n+", text) if p.strip()]
    chunks: list[str] = []
    current = ""
    for para in paragraphs:
        candidate = f"{current}\n{para}".strip() if current else para
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(para) <= max_chars:
            current = para
        else:
            start = 0
            while start < len(para):
                end = min(start + max_chars, len(para))
                chunks.append(para[start:end])
                start = end - overlap if end < len(para) else end
            current = ""
    if current:
        chunks.append(current)
    return chunks


def simple_keywords(text: str, top_k: int = 8) -> list[str]:
    words = re.findall(r"[\u4e00-\u9fffA-Za-z]{2,}", text)
    freq: dict[str, int] = {}
    for word in words:
        freq[word] = freq.get(word, 0) + 1
    ordered = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)
    return [w for w, _ in ordered[:top_k]]


def simple_topic_tags(text: str) -> list[str]:
    mapping = {
        "政策": ["政策", "条例", "规划", "方案"],
        "产业": ["产业", "企业", "供应链", "市场"],
        "技术": ["技术", "算法", "模型", "系统", "平台"],
        "治理": ["治理", "监管", "机制", "责任"],
    }
    tags = [tag for tag, hits in mapping.items() if any(hit in text for hit in hits)]
    return tags or ["未分类"]


def run(input_file: Path, output_file: Path, max_chars: int, overlap: int) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with input_file.open("r", encoding="utf-8") as source, output_file.open("w", encoding="utf-8") as sink:
        for line in source:
            if not line.strip():
                continue
            article = json.loads(line)
            if not article.get("quality_passed", False):
                continue
            article_id = article.get("article_id")
            text = article.get("clean_text", "")
            for idx, chunk in enumerate(split_chunks(text, max_chars=max_chars, overlap=overlap), start=1):
                chunk_id = hashlib.sha256(f"{article_id}:{idx}:{chunk}".encode("utf-8")).hexdigest()[:24]
                row = {
                    "chunk_id": chunk_id,
                    "article_id": article_id,
                    "source_url": article.get("source_url"),
                    "topic_tags": simple_topic_tags(chunk),
                    "chunk_text": chunk,
                    "keywords": simple_keywords(chunk),
                    "confidence": 0.65,
                    "embedding_model": "",
                    "embedding_vector_ref": "",
                    "index_status": "pending",
                }
                sink.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build topic chunks from cleaned article text.")
    parser.add_argument("--input", default=Path("outputs/clean_articles.jsonl"), type=Path)
    parser.add_argument("--output", default=Path("outputs/topic_chunks.jsonl"), type=Path)
    parser.add_argument("--max-chars", default=420, type=int)
    parser.add_argument("--overlap", default=60, type=int)
    args = parser.parse_args()
    run(args.input, args.output, args.max_chars, args.overlap)


if __name__ == "__main__":
    main()
