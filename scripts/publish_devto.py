"""Publish or update a Markdown article on Dev.to via the Forem REST API."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import httpx


def publish_to_devto(
    markdown_path: Path,
    api_key: str,
    published: bool = True,
) -> dict:
    """Send Markdown (with YAML frontmatter) to Dev.to /api/articles."""
    content = markdown_path.read_text(encoding="utf-8")
    payload = {
        "article": {
            "body_markdown": content,
            "published": published,
        }
    }
    headers = {
        "api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/vnd.forem.api-v1+json",
    }
    with httpx.Client(timeout=30.0) as client:
        response = client.post("https://dev.to/api/articles", json=payload, headers=headers)
        response.raise_for_status()
        return response.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish a Markdown article to Dev.to.")
    parser.add_argument(
        "markdown_file",
        type=Path,
        nargs="?",
        default=Path("docs/devto_article.md"),
        help="Path to Markdown article with Dev.to frontmatter.",
    )
    parser.add_argument(
        "--draft",
        action="store_true",
        help="Save as unpublished draft instead of publishing live.",
    )
    args = parser.parse_args()

    api_key = os.getenv("DEVTO_API_KEY", "").strip()
    if not api_key:
        print("Error: DEVTO_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    if not args.markdown_file.exists():
        print(f"Error: File not found: {args.markdown_file}", file=sys.stderr)
        sys.exit(1)

    result = publish_to_devto(
        markdown_path=args.markdown_file,
        api_key=api_key,
        published=not args.draft,
    )
    print(f"Success! Dev.to article ID: {result.get('id')}")
    print(f"URL: {result.get('url')}")


if __name__ == "__main__":
    main()
