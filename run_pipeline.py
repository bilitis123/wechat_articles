from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import sys


def run_step(command: list[str]) -> None:
    proc = subprocess.run(command, check=False)
    if proc.returncode != 0:
        joined = " ".join(command)
        raise RuntimeError(f"Step failed: {joined}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run WeChat collect -> chunk -> glossary pipeline.")
    parser.add_argument("--input", required=True, type=Path, help="URL list file.")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip crawler and use existing fetched output.")
    parser.add_argument("--max-urls", type=int, default=0, help="Limit fetch count for validation run (0 = all).")
    parser.add_argument("--start-date", default="", help="Start date filter, format YYYYMMDD.")
    parser.add_argument("--end-date", default="", help="End date filter, format YYYYMMDD.")
    parser.add_argument("--accounts", default="", help="Comma-separated account names to include.")
    parser.add_argument("--accounts-file", type=Path, default=None, help="Text file for account names (one per line).")
    args = parser.parse_args()

    py = sys.executable
    if not args.skip_fetch:
        cmd = [py, "-m", "crawler.wechat_fetcher", "--input", str(args.input), "--max-urls", str(args.max_urls)]
        if args.start_date:
            cmd.extend(["--start-date", args.start_date])
        if args.end_date:
            cmd.extend(["--end-date", args.end_date])
        if args.accounts:
            cmd.extend(["--accounts", args.accounts])
        if args.accounts_file:
            cmd.extend(["--accounts-file", str(args.accounts_file)])
        run_step(cmd)
    run_step([py, "-m", "pipeline.clean_text"])
    run_step([py, "-m", "pipeline.chunker"])
    run_step([py, "-m", "pipeline.terminology_builder"])


if __name__ == "__main__":
    main()
