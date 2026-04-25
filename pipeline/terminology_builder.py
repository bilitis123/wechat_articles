from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re


DEFAULT_BLACKLIST = {"我们", "你们", "他们", "这个", "那个", "进行", "相关", "工作", "方面", "需要", "形成", "确保"}
DOMAIN_TERMS = [
    "数字治理",
    "产业数字化",
    "平台建设",
    "政策支持",
    "地方治理",
    "公共服务",
    "数据共享",
    "流程再造",
    "统一口径",
    "实施指南",
    "数字化转型",
    "制度保障",
    "组织协同",
]


def extract_candidate_terms(text: str) -> list[str]:
    candidates: list[str] = []
    for term in DOMAIN_TERMS:
        if term in text:
            candidates.append(term)
    candidates.extend(re.findall(r"[\u4e00-\u9fff]{2,4}", text))
    return [term for term in candidates if term not in DEFAULT_BLACKLIST]


def pick_definition(text: str, term: str) -> str:
    for sentence in re.split(r"[。！？\n]", text):
        sentence = sentence.strip()
        if term in sentence and len(sentence) >= 12:
            return sentence[:160]
    return f"{term}：需人工补充定义。"


def build_scope(text: str) -> str:
    if any(word in text for word in ["全国", "国家", "中央"]):
        return "national"
    if any(word in text for word in ["地方", "省", "市", "县"]):
        return "regional"
    return "general"


def run(input_file: Path, output_file: Path, min_count: int) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    term_counter = Counter()
    term_sources: dict[str, list[dict[str, str]]] = defaultdict(list)
    term_snippets: dict[str, str] = {}

    with input_file.open("r", encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                continue
            row = json.loads(line)
            chunk_text = row.get("chunk_text", "")
            article_id = row.get("article_id", "")
            source_url = row.get("source_url", "")
            terms = extract_candidate_terms(chunk_text)
            for term in terms:
                term_counter[term] += 1
                if len(term_sources[term]) < 5:
                    term_sources[term].append({"article_id": article_id, "source_url": source_url})
                if term not in term_snippets:
                    term_snippets[term] = chunk_text

    with output_file.open("w", encoding="utf-8") as sink:
        for term, count in term_counter.items():
            if count < min_count:
                continue
            snippet = term_snippets.get(term, "")
            row = {
                "term": term,
                "definition": pick_definition(snippet, term),
                "scope": build_scope(snippet),
                "example_sentence": pick_definition(snippet, term),
                "source_refs": term_sources.get(term, []),
                "version": "v1",
                "score": count,
                "conflict_candidates": [],
                "embedding_model": "",
                "embedding_vector_ref": "",
                "index_status": "pending",
            }
            sink.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build terminology glossary from topic chunks.")
    parser.add_argument("--input", default=Path("outputs/topic_chunks.jsonl"), type=Path)
    parser.add_argument("--output", default=Path("outputs/term_glossary.jsonl"), type=Path)
    parser.add_argument("--min-count", default=2, type=int)
    args = parser.parse_args()
    run(args.input, args.output, args.min_count)


if __name__ == "__main__":
    main()
