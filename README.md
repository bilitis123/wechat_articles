# WeChat Collect to Fragment Library

This project implements the selected steps:

1. Fetch article content from existing WeChat article URLs.
2. Clean and quality-check text.
3. Build topic chunk library.
4. Build terminology glossary library.

## Install

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Run

```bash
python run_pipeline.py --input "./公众号文章采集.xlsx"
```

Validation run (first 10 URLs):

```bash
python run_pipeline.py --input "./公众号文章采集.xlsx" --max-urls 10
```

If you already have fetched JSONL:

```bash
python run_pipeline.py --input input/urls_example.txt --skip-fetch
```

## Outputs

- `outputs/fetched_articles.jsonl`
- `outputs/clean_articles.jsonl`
- `outputs/topic_chunks.jsonl`
- `outputs/term_glossary.jsonl`
- `outputs/crawl.log`
- `outputs/unprocessed_elements.jsonl`
- `outputs/raw_html_full/` (full page HTML)
- `outputs/raw_html_content/` (content-only HTML from `#js_content`)

## Notes

- Fetching is idempotent by `source_url`: already fetched URLs in output are skipped.
- Input supports `txt/csv/json/xlsx`; for Excel files, URLs are auto-detected from all columns.
- Raw HTML file naming uses: `发布日期_文章名称.html` (prioritizes title/date from Excel list).
- Video/iframe are marked as unprocessed elements for knowledge-base review.
- Supports fetch filters:
  - `--start-date YYYYMMDD`
  - `--end-date YYYYMMDD`
  - `--accounts "公众号A,公众号B"`
  - `--accounts-file input/accounts.txt` (one account name per line)
- `schemas/knowledge_schema.json` reserves RAG-ready fields:
  - `embedding_model`
  - `embedding_vector_ref`
  - `index_status`
