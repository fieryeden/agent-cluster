"""
Worker Agent — Autonomous agent that polls for tasks, claims them, executes real work, and reports results.

Usage:
    python -m agent_cluster.worker --id worker-1 --coordinator http://localhost:8080 --capabilities web:1.0,research:0.9

Capability handlers:
    research  — Web search via Brave API, result summarization
    web       — URL fetching, content extraction, HTML→markdown
    data      — CSV/JSON analysis, statistics, outlier detection, charts
    ai        — Text generation, summarization, classification via OpenAI-compatible API
    legal     — Document analysis, compliance checks, clause extraction
    file      — File I/O: read, write, search, transform
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import statistics
import sys
import tempfile
import textwrap
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# YAML support (optional — falls back to JSON)
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Optional imports
try:
    import matplotlib
    matplotlib.use("Agg")  # Non-interactive backend
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── Capability Handler Implementations ─────────────────────────────────────

class ResearchHandler:
    """Real research: web search, content fetching, result synthesis."""

    def __init__(self, brave_api_key: str = None):
        self.brave_api_key = brave_api_key or os.environ.get("BRAVE_API_KEY", "")
        if not self.brave_api_key:
            # Try reading from OpenClaw config
            try:
                cfg_path = Path.home() / ".openclaw" / "openclaw.json"
                if cfg_path.exists():
                    import json as _json
                    with open(cfg_path) as f:
                        cfg = _json.load(f)
                    self.brave_api_key = (cfg.get("plugins", {}).get("entries", {})
                                          .get("brave", {}).get("config", {})
                                          .get("webSearch", {}).get("apiKey", ""))
            except Exception:
                pass
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "AgentCluster/0.9.0"})

    def search(self, query: str, count: int = 10) -> Dict[str, Any]:
        """Search the web using Brave Search API (if key available) or DuckDuckGo HTML."""
        if self.brave_api_key:
            return self._search_brave(query, count)
        return self._search_duckduckgo(query, count)

    def _search_brave(self, query: str, count: int) -> Dict[str, Any]:
        """Brave Search API — high quality, rate-limited."""
        try:
            resp = self.session.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": min(count, 20)},
                headers={"X-Subscription-Token": self.brave_api_key},
                timeout=15
            )
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in data.get("web", {}).get("results", [])[:count]:
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "snippet": item.get("description", ""),
                })
            return {"engine": "brave", "query": query, "results": results, "count": len(results)}
        except Exception as e:
            return {"engine": "brave", "query": query, "results": [], "error": str(e)}

    def _search_duckduckgo(self, query: str, count: int) -> Dict[str, Any]:
        """Wikipedia API search — reliable fallback, no API key needed."""
        try:
            # Try Wikipedia API first (most reliable from servers)
            resp = self.session.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query", "list": "search",
                    "srsearch": query, "format": "json", "srlimit": min(count, 10)
                },
                headers={"User-Agent": "AgentCluster/0.9 (research@example.com)"},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in data.get("query", {}).get("search", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": f"https://en.wikipedia.org/wiki/{urllib.parse.quote(item['title'].replace(' ', '_'))}",
                    "snippet": re.sub(r'<[^>]+>', '', item.get("snippet", "")),
                })
            return {"engine": "wikipedia", "query": query, "results": results, "count": len(results)}
        except Exception as e:
            return {"engine": "wikipedia", "query": query, "results": [], "error": str(e)}

    def fetch_url(self, url: str, max_chars: int = 10000) -> Dict[str, Any]:
        """Fetch a URL and extract readable content."""
        try:
            resp = self.session.get(url, timeout=20, headers={
                "User-Agent": "Mozilla/5.0 (compatible; AgentCluster/0.9)"
            })
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "json" in content_type:
                return {"url": url, "type": "json", "content": resp.json(), "status": resp.status_code}
            soup = BeautifulSoup(resp.text, "lxml")
            # Remove nav, script, style
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)
            title = soup.title.get_text(strip=True) if soup.title else ""
            return {
                "url": url, "type": "html", "title": title,
                "content": text[:max_chars], "content_length": len(text),
                "status": resp.status_code
            }
        except Exception as e:
            return {"url": url, "error": str(e), "status": "fetch_failed"}

    def handle(self, params: Dict) -> Dict:
        """Route research tasks."""
        action = params.get("action", "research")
        query = params.get("query", params.get("description", ""))
        if action == "search":
            return self.search(query, params.get("count", 10))
        elif action == "fetch":
            return self.fetch_url(params.get("url", query), params.get("max_chars", 10000))
        else:
            return self.research(query, params.get("depth", "standard"))

    def research(self, query: str, depth: str = "standard") -> Dict[str, Any]:
        """Full research workflow: search → fetch top results → synthesize."""
        search_count = 5 if depth == "quick" else 10 if depth == "standard" else 15
        search_result = self.search(query, count=search_count)

        findings = []
        for r in search_result.get("results", [])[:5]:
            if r.get("url"):
                fetch = self.fetch_url(r["url"], max_chars=5000)
                if "content" in fetch and not fetch.get("error"):
                    findings.append({
                        "title": r.get("title", fetch.get("title", "")),
                        "url": r["url"],
                        "snippet": r.get("snippet", ""),
                        "content_preview": fetch.get("content", "")[:2000],
                        "content_length": fetch.get("content_length", 0),
                    })
                time.sleep(0.5)  # Be polite

        return {
            "type": "research_report",
            "query": query,
            "engine": search_result.get("engine", "unknown"),
            "search_results_count": search_result.get("count", 0),
            "sources_fetched": len(findings),
            "findings": findings,
            "timestamp": _now_iso()
        }


class WebHandler:
    """Real web operations: fetch, extract, transform."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "AgentCluster/0.9.0"})

    def fetch(self, url: str, extract_mode: str = "text", max_chars: int = 20000) -> Dict[str, Any]:
        """Fetch URL and extract content."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")

            if "json" in content_type:
                return {"url": url, "type": "json", "data": resp.json(), "status": resp.status_code}

            if extract_mode == "raw":
                return {"url": url, "type": "raw", "content": resp.text[:max_chars], "status": resp.status_code}

            soup = BeautifulSoup(resp.text, "lxml")
            for tag in soup(["script", "style"]):
                tag.decompose()

            if extract_mode == "markdown":
                return {"url": url, "type": "markdown", "content": self._html_to_markdown(soup)[:max_chars]}

            # Default: text
            text = soup.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)
            title = soup.title.get_text(strip=True) if soup.title else ""
            links = [a.get("href") for a in soup.find_all("a", href=True)][:50]
            return {
                "url": url, "type": "text", "title": title,
                "content": text[:max_chars], "links": links,
                "status": resp.status_code
            }
        except Exception as e:
            return {"url": url, "error": str(e)}

    def _html_to_markdown(self, soup: BeautifulSoup) -> str:
        """Simple HTML→markdown conversion."""
        lines = []
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            level = int(tag.name[1])
            lines.append(f"{'#' * level} {tag.get_text(strip=True)}\n")
        for p in soup.find_all("p"):
            lines.append(p.get_text(strip=True) + "\n")
        for li in soup.find_all("li"):
            lines.append(f"- {li.get_text(strip=True)}\n")
        for a in soup.find_all("a", href=True):
            lines.append(f"[{a.get_text(strip=True)}]({a['href']})\n")
        return "\n".join(lines)

    def check_status(self, urls: List[str]) -> Dict[str, Any]:
        """Check HTTP status for multiple URLs."""
        results = {}
        for url in urls:
            try:
                resp = self.session.head(url, timeout=10, allow_redirects=True)
                results[url] = {"status": resp.status_code, "redirected": str(resp.url) != url}
            except Exception as e:
                results[url] = {"status": "error", "error": str(e)}
        return {"type": "status_check", "results": results, "timestamp": _now_iso()}

    def handle(self, params: Dict) -> Dict:
        """Route web tasks."""
        action = params.get("action", "fetch")
        if action == "fetch":
            url = params.get("url", params.get("description", ""))
            if not url.startswith("http"):
                return {"error": f"Invalid URL: {url}"}
            return self.fetch(url, params.get("mode", "text"), params.get("max_chars", 20000))
        elif action == "status_check":
            urls = params.get("urls", [])
            return self.check_status(urls)
        elif action == "search":
            # Delegate to research handler
            rh = ResearchHandler()
            return rh.search(params.get("query", params.get("description", "")))
        else:
            url = params.get("url", params.get("description", ""))
            if url.startswith("http"):
                return self.fetch(url)
            return {"error": f"Unknown web action: {action}"}


class DataHandler:
    """Real data analysis: CSV/JSON processing, statistics, visualization."""

    def analyze_csv(self, data: str, analysis: str = "summary") -> Dict[str, Any]:
        """Analyze CSV data (as string or file path)."""
        try:
            # Check if it's a file path
            if len(data) < 500 and os.path.isfile(data):
                df = pd.read_csv(data)
            else:
                df = pd.read_csv(io.StringIO(data))

            result = {"type": "data_analysis", "rows": len(df), "columns": list(df.columns)}

            if analysis in ("summary", "full"):
                result["dtypes"] = {col: str(dt) for col, dt in df.dtypes.items()}
                desc = df.describe(include="all").to_dict()
                # Convert numpy types for JSON serialization
                result["summary"] = self._sanitize_json(desc)
                result["null_counts"] = {col: int(df[col].isnull().sum()) for col in df.columns}

            if analysis in ("full", "outliers"):
                outliers = {}
                for col in df.select_dtypes(include=[np.number]).columns:
                    q1 = df[col].quantile(0.25)
                    q3 = df[col].quantile(0.75)
                    iqr = q3 - q1
                    mask = (df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)
                    outlier_count = int(mask.sum())
                    if outlier_count > 0:
                        outliers[col] = {
                            "count": outlier_count,
                            "pct": round(outlier_count / len(df) * 100, 2),
                            "lower_fence": float(q1 - 1.5 * iqr),
                            "upper_fence": float(q3 + 1.5 * iqr),
                        }
                result["outliers"] = outliers

            if analysis in ("full", "correlation") and len(df.select_dtypes(include=[np.number]).columns) > 1:
                corr = df.select_dtypes(include=[np.number]).corr().to_dict()
                result["correlation"] = self._sanitize_json(corr)

            return result
        except Exception as e:
            return {"type": "data_analysis", "error": str(e)}

    def analyze_json(self, data: Any, analysis: str = "structure") -> Dict[str, Any]:
        """Analyze JSON data."""
        try:
            if isinstance(data, str):
                if len(data) < 500 and os.path.isfile(data):
                    with open(data) as f:
                        data = json.load(f)
                else:
                    data = json.loads(data)

            result = {"type": "json_analysis"}

            if analysis == "structure":
                result["structure"] = self._describe_structure(data)
            elif analysis == "flatten":
                flat = self._flatten_json(data)
                result["flattened"] = flat[:100]  # Cap at 100 entries
                result["total_keys"] = len(flat)
            elif analysis == "stats" and isinstance(data, list):
                numeric_vals = [v for v in data if isinstance(v, (int, float))]
                if numeric_vals:
                    result["stats"] = {
                        "count": len(numeric_vals),
                        "mean": statistics.mean(numeric_vals),
                        "median": statistics.median(numeric_vals),
                        "stdev": statistics.stdev(numeric_vals) if len(numeric_vals) > 1 else 0,
                        "min": min(numeric_vals),
                        "max": max(numeric_vals),
                    }
            return result
        except Exception as e:
            return {"type": "json_analysis", "error": str(e)}

    def create_chart(self, data: str, chart_type: str = "bar", x_col: str = None,
                     y_col: str = None, title: str = "Chart") -> Dict[str, Any]:
        """Create a chart from CSV data and return as base64 PNG."""
        if not HAS_MATPLOTLIB:
            return {"error": "matplotlib not available"}
        try:
            if len(data) < 500 and os.path.isfile(data):
                df = pd.read_csv(data)
            else:
                df = pd.read_csv(io.StringIO(data))

            fig, ax = plt.subplots(figsize=(10, 6))

            if chart_type == "bar":
                if x_col and y_col:
                    df.plot.bar(x=x_col, y=y_col, ax=ax)
                else:
                    df.iloc[:20].plot.bar(ax=ax)
            elif chart_type == "line":
                if x_col and y_col:
                    df.plot.line(x=x_col, y=y_col, ax=ax)
                else:
                    df.plot.line(ax=ax)
            elif chart_type == "scatter" and x_col and y_col:
                df.plot.scatter(x=x_col, y=y_col, ax=ax)
            elif chart_type == "hist":
                df.select_dtypes(include=[np.number]).iloc[:, 0].hist(ax=ax, bins=20)
            elif chart_type == "pie" and y_col:
                df.plot.pie(y=y_col, ax=ax)

            ax.set_title(title)
            plt.tight_layout()

            # Save to base64
            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=100)
            plt.close(fig)
            buf.seek(0)
            import base64
            img_b64 = base64.b64encode(buf.read()).decode()

            return {"type": "chart", "chart_type": chart_type, "title": title,
                    "image_base64": img_b64[:200] + "...", "image_size_bytes": len(img_b64),
                    "note": "Full base64 available in result.image_base64"}
        except Exception as e:
            return {"type": "chart", "error": str(e)}

    def _describe_structure(self, obj: Any, prefix: str = "") -> Dict:
        """Recursively describe JSON structure."""
        if isinstance(obj, dict):
            return {f"{prefix}{k}": self._describe_structure(v, f"{prefix}{k}.") for k, v in obj.items()}
        elif isinstance(obj, list):
            if obj and isinstance(obj[0], dict):
                return {"_type": f"list[{len(obj)}]", "_items": self._describe_structure(obj[0], prefix)}
            return {"_type": f"list[{len(obj)}]", "_item_type": type(obj[0]).__name__ if obj else "empty"}
        return {"_type": type(obj).__name__}

    def _flatten_json(self, obj: Any, prefix: str = "") -> List[str]:
        """Flatten nested JSON to dot-notation keys."""
        items = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                items.extend(self._flatten_json(v, f"{prefix}{k}."))
        elif isinstance(obj, list):
            for i, v in enumerate(obj[:10]):
                items.extend(self._flatten_json(v, f"{prefix}[{i}]."))
        else:
            items.append(prefix.rstrip("."))
        return items

    def _sanitize_json(self, obj: Any) -> Any:
        """Convert numpy/pandas types to Python native for JSON serialization."""
        if isinstance(obj, dict):
            return {k: self._sanitize_json(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._sanitize_json(v) for v in obj]
        elif isinstance(obj, (np.integer,)):
            return int(obj)
        elif isinstance(obj, (np.floating,)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        return obj

    def handle(self, params: Dict) -> Dict:
        """Route data tasks."""
        action = params.get("action", "analyze")
        data = params.get("data", params.get("csv", params.get("json", "")))
        description = params.get("description", "")

        if action == "analyze_csv" or (data and ("col" in str(data).lower() or "," in str(data)[:200])):
            return self.analyze_csv(data or description, params.get("analysis", "full"))
        elif action == "analyze_json":
            return self.analyze_json(data or description, params.get("analysis", "structure"))
        elif action == "chart":
            return self.create_chart(data or description, params.get("chart_type", "bar"),
                                     params.get("x_col"), params.get("y_col"), params.get("title", "Chart"))
        elif action == "transform":
            return self._transform(params)
        else:
            # Auto-detect: try CSV first, then JSON
            if data:
                try:
                    pd.read_csv(io.StringIO(data))
                    return self.analyze_csv(data, "full")
                except Exception:
                    pass
                try:
                    json.loads(data)
                    return self.analyze_json(data, "structure")
                except Exception:
                    pass
            return {"type": "data_result", "error": "Could not auto-detect data format. Specify action: analyze_csv, analyze_json, or chart"}

    def _transform(self, params: Dict) -> Dict:
        """Transform data: filter, sort, aggregate."""
        data = params.get("data", "")
        transform = params.get("transform", "")
        try:
            if len(data) < 500 and os.path.isfile(data):
                df = pd.read_csv(data)
            else:
                df = pd.read_csv(io.StringIO(data))

            if transform == "sort":
                col = params.get("sort_by")
                df = df.sort_values(col, ascending=params.get("ascending", True))
            elif transform == "filter":
                col = params.get("filter_col")
                op = params.get("filter_op", "==")
                val = params.get("filter_val")
                if op == "==":
                    df = df[df[col] == val]
                elif op == ">":
                    df = df[df[col] > float(val)]
                elif op == "<":
                    df = df[df[col] < float(val)]

            result_csv = df.to_csv(index=False)
            return {"type": "transform", "transform": transform, "rows": len(df),
                    "result_csv": result_csv[:10000], "truncated": len(result_csv) > 10000}
        except Exception as e:
            return {"type": "transform", "error": str(e)}


class AIHandler:
    """AI capabilities: text analysis, summarization, classification using OpenAI-compatible APIs."""

    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self.base_url = base_url or os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
        self.model = model or os.environ.get("AI_MODEL", "gpt-3.5-turbo")

    def _call_llm(self, prompt: str, system: str = "You are a helpful assistant.", 
                    max_tokens: int = 16384, temperature: float = 0.3) -> Dict[str, Any]:
        """Call OpenAI-compatible chat completion API with retry on 429."""
        if not self.api_key:
            return {"error": "No API key configured. Set ai.api_key in config.yaml."}
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": prompt}
                        ],
                        "max_tokens": max_tokens,
                        "temperature": temperature
                    },
                    timeout=180
                )
                if resp.status_code == 429:
                    wait = min(2 ** attempt * 2, 10)
                    print(f"[AI] Rate limited (429), retry {attempt+1}/{max_retries} in {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                return {
                    "type": "ai_completion",
                    "content": data["choices"][0]["message"]["content"],
                    "model": data.get("model", self.model),
                    "usage": data.get("usage", {}),
                    "finish_reason": data["choices"][0].get("finish_reason", "")
                }
            except requests.exceptions.Timeout:
                print(f"[AI] Timeout on attempt {attempt+1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return {"type": "ai_completion", "error": f"Timeout after {max_retries} attempts"}
            except Exception as e:
                return {"type": "ai_completion", "error": str(e)}
        return {"type": "ai_completion", "error": "Rate limited after max retries"}

        """Call OpenAI-compatible chat completion API."""
        if not self.api_key:
            return {"error": "No API key configured. Set OPENAI_API_KEY environment variable."}
        try:
            resp = requests.post(
                f"{self.base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": max_tokens,
                    "temperature": temperature
                },
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "type": "ai_completion",
                "content": data["choices"][0]["message"]["content"],
                "model": data.get("model", self.model),
                "usage": data.get("usage", {}),
                "finish_reason": data["choices"][0].get("finish_reason", "")
            }
        except Exception as e:
            return {"type": "ai_completion", "error": str(e)}

    def summarize(self, text: str, max_length: int = 500) -> Dict[str, Any]:
        """Summarize text using LLM or extractive fallback."""
        if self.api_key:
            result = self._call_llm(
                "Summarize the following text in under " + str(max_length) + " characters:\n\n" + text[:8000],
                system="You are a precise summarizer. Provide concise, factual summaries."
            )
            if "error" not in result:
                return result
            print(f"[AI] LLM failed ({result.get('error', 'unknown')}), using extractive fallback")
        # Extractive fallback: pick most important sentences
        return self._extractive_summarize(text, max_length)

    def classify(self, text: str, categories: List[str] = None) -> Dict[str, Any]:
        """Classify text into categories."""
        if self.api_key and categories:
            cats_str = ", ".join(categories)
            return self._call_llm(
                f"Classify the following text into one of these categories: {cats_str}\n\n{text[:4000]}\n\nRespond with ONLY the category name.",
                system="You are a text classifier. Respond with only the category name, nothing else.",
                max_tokens=50, temperature=0.0
            )
        # Fallback: keyword matching
        if categories:
            text_lower = text.lower()
            scores = {}
            for cat in categories:
                scores[cat] = sum(1 for kw in cat.lower().split() if kw in text_lower)
            best = max(scores, key=scores.get) if scores else categories[0]
            return {"type": "classification", "category": best, "scores": scores, "method": "keyword_fallback"}
        return {"error": "No categories provided"}

    def extract(self, text: str, fields: List[str] = None) -> Dict[str, Any]:
        """Extract structured information from text."""
        if not fields:
            fields = ["dates", "names", "amounts", "locations"]
        if self.api_key:
            fields_str = ", ".join(fields)
            return self._call_llm(
                f"Extract the following fields from this text: {fields_str}\n\nText:\n{text[:6000]}\n\nRespond in JSON format with each field as a key.",
                system="You extract structured data. Respond in valid JSON only.",
                max_tokens=500, temperature=0.0
            )
        # Regex fallback
        result = {}
        if "dates" in fields:
            result["dates"] = re.findall(r"\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4}", text)
        if "amounts" in fields:
            result["amounts"] = re.findall(r"\$[\d,]+\.?\d*|\d+\.?\d*\s*(?:dollars|USD|million|billion)", text, re.I)
        if "emails" in fields:
            result["emails"] = re.findall(r"[\w.+-]+@[\w-]+\.[\w.]+", text)
        if "urls" in fields:
            result["urls"] = re.findall(r"https?://\S+", text)
        return {"type": "extraction", "fields": result, "method": "regex_fallback"}

    def _extractive_summarize(self, text: str, max_chars: int) -> Dict[str, Any]:
        """Extractive summarization: pick top sentences by keyword density."""
        sentences = re.split(r'[.!?]+\s+', text)
        if len(sentences) <= 3:
            return {"type": "summary", "content": text[:max_chars], "method": "extractive", "sentences": len(sentences)}

        # Score sentences by word frequency
        words = re.findall(r'\b[a-z]{4,}\b', text.lower())
        freq = {}
        for w in words:
            freq[w] = freq.get(w, 0) + 1
        # Top 10 keywords
        top_words = set(w for w, c in sorted(freq.items(), key=lambda x: -x[1])[:10])

        scored = []
        for s in sentences:
            s_words = set(re.findall(r'\b[a-z]{4,}\b', s.lower()))
            score = len(s_words & top_words)
            scored.append((score, s))

        scored.sort(key=lambda x: -x[0])
        summary = ". ".join(s for _, s in scored[:5])
        return {"type": "summary", "content": summary[:max_chars], "method": "extractive",
                "source_sentences": len(sentences), "summary_sentences": min(5, len(sentences))}

    def handle(self, params: Dict) -> Dict:
        """Route AI tasks."""
        action = params.get("action", "summarize")
        text = params.get("text", params.get("prompt", params.get("description", "")))

        if action == "summarize":
            return self.summarize(text, params.get("max_length", 500))
        elif action == "classify":
            return self.classify(text, params.get("categories", []))
        elif action == "extract":
            return self.extract(text, params.get("fields"))
        elif action == "generate":
            return self._call_llm(text, max_tokens=params.get("max_tokens", 16384),
                                  temperature=params.get("temperature", 0.7))
        elif action == "analyze":
            if self.api_key:
                return self._call_llm(f"Analyze the following:\n\n{text[:6000]}",
                                      system="You are an analytical assistant. Provide thorough analysis.",
                                      max_tokens=16384)
            return {"type": "analysis", "word_count": len(text.split()),
                    "char_count": len(text), "method": "basic_stats_only",
                    "note": "Set OPENAI_API_KEY for LLM-powered analysis"}
        else:
            return {"error": f"Unknown AI action: {action}. Available: summarize, classify, extract, generate, analyze"}


class LegalHandler:
    """Legal document analysis: clause extraction, compliance checks, risk assessment."""

    def __init__(self, ai_handler: AIHandler = None):
        self.ai = ai_handler or AIHandler()

    def extract_clauses(self, text: str) -> Dict[str, Any]:
        """Extract legal clauses from document text."""
        # Regex-based clause detection
        clauses = {
            "termination": re.findall(r"(?:terminat|cancel|end\s+this\s+agreement)[^.]*\.", text, re.I),
            "liability": re.findall(r"(?:liab|indemnif|hold\s+harmless)[^.]*\.", text, re.I),
            "confidentiality": re.findall(r"(?:confident|non[- ]disclos|NDA|proprietary\s+information)[^.]*\.", text, re.I),
            "governing_law": re.findall(r"(?:governed\s+by|laws\s+of|jurisdiction)[^.]*\.", text, re.I),
            "payment": re.findall(r"(?:payment|fee|compensat|remunerat|invoice)[^.]*\.", text, re.I),
            "warranty": re.findall(r"(?:warrant|guarant|as[- ]is)[^.]*\.", text, re.I),
            "force_majeure": re.findall(r"(?:force\s+majeure|act\s+of\s+god|unforeseeable)[^.]*\.", text, re.I),
            "intellectual_property": re.findall(r"(?:intellectual\s+property|IP\s+rights|copyright|patent|trademark)[^.]*\.", text, re.I),
        }
        # Remove empty
        clauses = {k: v for k, v in clauses.items() if v}
        return {"type": "clause_extraction", "clauses_found": len(clauses),
                "clause_types": list(clauses.keys()), "clauses": clauses}

    def compliance_check(self, text: str, framework: str = "general") -> Dict[str, Any]:
        """Check text for compliance issues against common frameworks."""
        flags = []
        if framework in ("general", "gdpr"):
            # GDPR checks
            if not re.search(r"data\s+protection|privacy\s+policy|GDPR", text, re.I):
                flags.append({"issue": "No GDPR/privacy reference found", "severity": "high"})
            if not re.search(r"consent|opt[- ]out|right\s+to\s+delet", text, re.I):
                flags.append({"issue": "No user consent/deletion rights mentioned", "severity": "high"})
        if framework in ("general", "sox"):
            if not re.search(r"audit|internal\s+control|SOX|Sarbanes", text, re.I):
                flags.append({"issue": "No SOX/audit controls reference", "severity": "medium"})
        if framework in ("general", "hipaa"):
            if re.search(r"health|medical|patient|PHI|HIPAA", text, re.I):
                if not re.search(r"encrypt|access\s+control|HIPAA", text, re.I):
                    flags.append({"issue": "Health data referenced without HIPAA safeguards", "severity": "high"})

        # Generic red flags
        if re.search(r"unlimited\s+liability|no\s+limitation\s+of\s+liability", text, re.I):
            flags.append({"issue": "Unlimited liability clause detected", "severity": "critical"})
        if re.search(r"may\s+change\s+(?:this|the|any)\s+(?:agreement|terms)\s+(?:at\s+any\s+time|without\s+notice)", text, re.I):
            flags.append({"issue": "Unilateral modification without notice", "severity": "high"})
        if re.search(r"waive\s+(?:all\s+)?rights|no\s+warranty|as[- ]is", text, re.I):
            flags.append({"issue": "Broad rights waiver or as-is clause", "severity": "medium"})

        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for f in flags:
            severity_counts[f.get("severity", "low")] += 1

        return {
            "type": "compliance_check",
            "framework": framework,
            "flags": flags,
            "flag_count": len(flags),
            "severity_counts": severity_counts,
            "risk_level": "critical" if severity_counts["critical"] else
                          "high" if severity_counts["high"] else
                          "medium" if severity_counts["medium"] else "low"
        }

    def handle(self, params: Dict) -> Dict:
        """Route legal tasks."""
        action = params.get("action", "extract_clauses")
        text = params.get("text", params.get("document", params.get("description", "")))

        if action == "extract_clauses":
            return self.extract_clauses(text)
        elif action == "compliance_check":
            return self.compliance_check(text, params.get("framework", "general"))
        elif action == "risk_assessment":
            # Use AI if available, else compliance_check
            if self.ai.api_key:
                return self.ai._call_llm(
                    f"Assess the legal risks in this document:\n\n{text[:6000]}\n\nList risks by severity (critical/high/medium/low).",
                    system="You are a legal risk analyst. Be thorough and specific.",
                    max_tokens=2000
                )
            return self.compliance_check(text)
        elif action == "summarize":
            return self.ai.summarize(text)
        else:
            return self.extract_clauses(text)


class FileHandler:
    """File operations: read, write, search, transform."""

    def read(self, path: str, encoding: str = "utf-8", max_chars: int = 50000) -> Dict[str, Any]:
        """Read file contents."""
        try:
            p = Path(path)
            if not p.exists():
                return {"error": f"File not found: {path}"}
            if p.is_dir():
                items = list(p.iterdir())
                return {"type": "directory_listing", "path": str(p),
                        "items": [{"name": i.name, "type": "dir" if i.is_dir() else "file",
                                   "size": i.stat().st_size if i.is_file() else 0} for i in items[:100]]}
            content = p.read_text(encoding=encoding, errors="replace")
            return {"type": "file_read", "path": str(p), "size_bytes": p.stat().st_size,
                    "content": content[:max_chars], "truncated": len(content) > max_chars}
        except Exception as e:
            return {"error": str(e)}

    def write(self, path: str, content: str, mode: str = "write") -> Dict[str, Any]:
        """Write content to file."""
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            if mode == "append":
                with open(p, "a") as f:
                    f.write(content)
            else:
                p.write_text(content)
            return {"type": "file_write", "path": str(p), "size_bytes": p.stat().st_size, "mode": mode}
        except Exception as e:
            return {"error": str(e)}

    def search(self, directory: str, pattern: str = "*", content_search: str = None,
               max_results: int = 50) -> Dict[str, Any]:
        """Search for files by name pattern and/or content."""
        try:
            p = Path(directory)
            if not p.exists():
                return {"error": f"Directory not found: {directory}"}
            matches = list(p.glob(f"**/{pattern}"))[:max_results]
            results = []
            for m in matches:
                entry = {"path": str(m), "name": m.name, "type": "dir" if m.is_dir() else "file",
                         "size": m.stat().st_size if m.is_file() else 0}
                if content_search and m.is_file():
                    try:
                        text = m.read_text(errors="replace")
                        if content_search.lower() in text.lower():
                            entry["match"] = True
                            # Show context around match
                            idx = text.lower().index(content_search.lower())
                            start = max(0, idx - 100)
                            end = min(len(text), idx + len(content_search) + 100)
                            entry["context"] = text[start:end]
                    except Exception:
                        pass
                results.append(entry)
            if content_search:
                results = [r for r in results if r.get("match")]
            return {"type": "file_search", "directory": str(p), "pattern": pattern,
                    "content_search": content_search, "results": results, "count": len(results)}
        except Exception as e:
            return {"error": str(e)}

    def transform(self, input_path: str, output_path: str = None,
                  operations: List[Dict] = None) -> Dict[str, Any]:
        """Apply transformations to a file (encoding, format conversion, search/replace)."""
        try:
            p = Path(input_path)
            if not p.exists():
                return {"error": f"File not found: {input_path}"}
            content = p.read_text(errors="replace")
            results = []
            for op in (operations or []):
                op_type = op.get("type")
                if op_type == "replace":
                    content = content.replace(op.get("find", ""), op.get("replace", ""))
                    results.append(f"Replaced '{op.get('find')}' → '{op.get('replace')}'")
                elif op_type == "regex_replace":
                    content = re.sub(op.get("pattern", ""), op.get("replace", ""), content)
                    results.append(f"Regex replaced: {op.get('pattern')}")
                elif op_type == "strip_lines":
                    content = "\n".join(line.strip() for line in content.splitlines())
                    results.append("Stripped whitespace from all lines")
                elif op_type == "dedup_lines":
                    seen = set()
                    lines = []
                    for line in content.splitlines():
                        if line not in seen:
                            seen.add(line)
                            lines.append(line)
                    content = "\n".join(lines)
                    results.append("Deduplicated lines")

            out = Path(output_path) if output_path else p
            out.write_text(content)
            return {"type": "file_transform", "input": str(p), "output": str(out),
                    "operations": results, "output_size": len(content)}
        except Exception as e:
            return {"error": str(e)}

    def handle(self, params: Dict) -> Dict:
        """Route file tasks."""
        action = params.get("action", "read")
        if action == "read":
            return self.read(params.get("path", params.get("description", "")),
                             params.get("encoding", "utf-8"), params.get("max_chars", 50000))
        elif action == "write":
            return self.write(params.get("path", ""), params.get("content", ""),
                              params.get("mode", "write"))
        elif action == "search":
            return self.search(params.get("directory", params.get("path", ".")),
                               params.get("pattern", "*"), params.get("content_search"),
                               params.get("max_results", 50))
        elif action == "transform":
            return self.transform(params.get("path", ""), params.get("output_path"),
                                  params.get("operations"))
        else:
            # Default: try to read the path
            path = params.get("path", params.get("description", ""))
            if path and Path(path).exists():
                return self.read(path)
            return {"error": f"Unknown file action: {action}. Available: read, write, search, transform"}


# ─── Worker Agent ────────────────────────────────────────────────────────────

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML or JSON file.

    Search order:
    1. Explicit --config path
    2. ./config.yaml
    3. ./config.json
    4. ~/.agent-cluster/config.yaml
    5. ~/.agent-cluster/config.json

    Returns empty dict if no config found.
    """
    search_paths = [config_path] if config_path else [
        Path("config.yaml"),
        Path("config.json"),
        Path.home() / ".agent-cluster" / "config.yaml",
        Path.home() / ".agent-cluster" / "config.json",
    ]

    for p in search_paths:
        p = Path(p)
        if not p.exists():
            continue
        try:
            with open(p) as f:
                raw = f.read()
            if p.suffix in (".yaml", ".yml"):
                if not HAS_YAML:
                    print(f"[Config] Found {p} but PyYAML not installed. pip install pyyaml")
                    print(f"[Config] Falling back to env vars and defaults.")
                    continue
                cfg = yaml.safe_load(raw)
            else:
                cfg = json.loads(raw)
            print(f"[Config] Loaded from {p}")
            return cfg or {}
        except Exception as e:
            print(f"[Config] Error reading {p}: {e}")
            continue

    # Auto-discover from OpenClaw config if no cluster config found
    oc_path = Path.home() / ".openclaw" / "openclaw.json"
    if oc_path.exists() and not config_path:
        try:
            with open(oc_path) as f:
                oc = json.loads(f.read())
            providers = oc.get("models", {}).get("providers", {})
            brave_key = oc.get("plugins", {}).get("entries", {}).get("brave", {}).get("config", {}).get("webSearch", {}).get("apiKey", "")
            # Find first OpenAI-compatible provider
            ai_cfg = {}
            for pname, pdata in providers.items():
                if pdata.get("api") == "openai-completions" and pdata.get("apiKey"):
                    ai_cfg = {
                        "api_key": pdata["apiKey"],
                        "base_url": pdata.get("baseUrl", ""),
                        "model": pdata.get("models", [{}])[0].get("id", ""),
                    }
                    break
            if ai_cfg or brave_key:
                print(f"[Config] Auto-discovered from OpenClaw config ({oc_path})")
                cfg = {"handlers": {}}
                if ai_cfg:
                    cfg["handlers"]["ai"] = ai_cfg
                    print(f"[Config]   AI: {ai_cfg['base_url']} / {ai_cfg['model']}")
                if brave_key:
                    cfg["handlers"]["research"] = {"brave_api_key": brave_key}
                    print(f"[Config]   Research: Brave API key found")
                return cfg
        except Exception as e:
            print(f"[Config] Error reading OpenClaw config: {e}")

    if config_path:
        print(f"[Config] Specified config not found: {config_path}")
    return {}


class WorkerAgent:
    """Autonomous worker agent that connects to a coordinator and executes tasks."""

    def __init__(self, agent_id: str, coordinator_url: str, capabilities: Dict[str, float],
                 poll_interval: float = 5.0, max_concurrent: int = 1,
                 config: Dict[str, Any] = None):
        self.agent_id = agent_id
        self.coordinator_url = coordinator_url.rstrip("/")
        self.capabilities = capabilities
        self.poll_interval = poll_interval
        self.max_concurrent = max_concurrent
        self.config = config or {}
        self.active_tasks = 0
        self.running = False
        self.stats = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_exec_time": 0.0,
            "capabilities_used": {}
        }
        self._lock = threading.Lock()

        # Extract handler config
        hcfg = self.config.get("handlers", {})
        ai_cfg = hcfg.get("ai", {})
        research_cfg = hcfg.get("research", {})

        # Initialize real capability handlers (config overrides env vars)
        ai_handler = AIHandler(
            api_key=ai_cfg.get("api_key"),
            base_url=ai_cfg.get("base_url"),
            model=ai_cfg.get("model"),
        )
        self._handlers = {
            "research": ResearchHandler(brave_api_key=research_cfg.get("brave_api_key")),
            "web": WebHandler(),
            "data": DataHandler(),
            "ai": ai_handler,
            "legal": LegalHandler(ai_handler=ai_handler),
            "file": FileHandler(),
        }

    def _api_call(self, method: str, path: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request to coordinator."""
        url = f"{self.coordinator_url}{path}"
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            try:
                return json.loads(e.read().decode())
            except Exception:
                return {"error": f"HTTP {e.code}"}
        except Exception as e:
            return {"error": str(e)}

    def register(self) -> bool:
        """Register with coordinator."""
        result = self._api_call("POST", "/register", {
            "agent_id": self.agent_id,
            "capabilities": self.capabilities
        })
        if "error" in result:
            print(f"[ERROR] Registration failed: {result['error']}")
            return False
        print(f"[INFO] Registered as {self.agent_id} with capabilities: {self.capabilities}")
        return True

    def send_heartbeat(self):
        """Send heartbeat to coordinator."""
        self._api_call("POST", "/heartbeat", {
            "agent_id": self.agent_id,
            "load": self.active_tasks / max(self.max_concurrent, 1),
            "status": "alive"
        })

    def poll_tasks(self) -> List[Dict]:
        """Poll for pending tasks from coordinator."""
        result = self._api_call("GET", "/tasks/pending")
        if "error" in result:
            return []
        return result.get("tasks", [])

    def claim_task(self, task_id: str) -> Optional[Dict]:
        """Claim a pending task."""
        result = self._api_call("POST", f"/tasks/{task_id}/claim", {
            "agent_id": self.agent_id
        })
        if result.get("status") == "claimed":
            return result.get("task")
        return None

    def submit_result(self, task_id: str, result_data: Dict[str, Any], status: str = "completed"):
        """Submit task result to coordinator."""
        result = self._api_call("POST", "/tasks/result", {
            "task_id": task_id,
            "agent_id": self.agent_id,
            "status": status,
            "result": result_data
        })
        return result

    def execute_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a task using real capability handlers."""
        capability = task.get("capability", "unknown")
        params = task.get("params", task.get("task_data", {}))
        task_id = task.get("task_id", "unknown")

        # If params is just a description string, wrap it
        if isinstance(params, str):
            params = {"description": params}

        print(f"[TASK] Executing {task_id}: {capability} — {params.get('description', params.get('query', 'no description'))}")
        start_time = time.time()

        try:
            handler = self._handlers.get(capability)
            if handler:
                result = handler.handle(params)
            else:
                result = self._handle_generic(params, capability)

            elapsed = time.time() - start_time
            with self._lock:
                self.stats["tasks_completed"] += 1
                self.stats["total_exec_time"] += elapsed
                self.stats["capabilities_used"][capability] = \
                    self.stats["capabilities_used"].get(capability, 0) + 1

            print(f"[DONE] {task_id} completed in {elapsed:.1f}s")
            return {
                "status": "completed",
                "task_id": task_id,
                "capability": capability,
                "output": result,
                "execution_time": round(elapsed, 2)
            }
        except Exception as e:
            elapsed = time.time() - start_time
            with self._lock:
                self.stats["tasks_failed"] += 1
            print(f"[FAIL] {task_id} failed: {e}")
            return {
                "status": "failed",
                "task_id": task_id,
                "capability": capability,
                "error": str(e),
                "execution_time": round(elapsed, 2)
            }

    def _handle_generic(self, params: Dict, capability: str) -> Dict:
        """Fallback handler for unknown capabilities."""
        return {
            "type": "generic_result",
            "capability": capability,
            "task": params.get("description", "generic task"),
            "status": "processed",
            "note": f"No specific handler for '{capability}'. Install a handler or use a different capability."
        }

    # ── Main Loop ────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Background heartbeat sender."""
        while self.running:
            try:
                self.send_heartbeat()
            except Exception as e:
                print(f"[WARN] Heartbeat failed: {e}")
            time.sleep(300)

    def run(self):
        """Main worker loop — register, poll, claim, execute, report."""
        if not self.register():
            print("[FATAL] Could not register with coordinator. Exiting.")
            sys.exit(1)

        self.running = True

        # Start heartbeat thread
        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb_thread.start()

        print(f"[INFO] Worker {self.agent_id} running. Polling every {self.poll_interval}s")
        print(f"[INFO] Capabilities: {self.capabilities}")
        print(f"[INFO] Handlers: {list(self._handlers.keys())}")

        while self.running:
            try:
                if self.active_tasks < self.max_concurrent:
                    tasks = self.poll_tasks()
                    for task in tasks:
                        task_id = task.get("task_id")
                        cap = task.get("capability")

                        if cap not in self.capabilities:
                            continue

                        claimed = self.claim_task(task_id)
                        if claimed:
                            with self._lock:
                                self.active_tasks += 1
                            result = self.execute_task(claimed)
                            self.submit_result(task_id, result, result["status"])
                            with self._lock:
                                self.active_tasks -= 1
                            break
            except KeyboardInterrupt:
                print(f"\n[INFO] Worker {self.agent_id} shutting down...")
                self.running = False
                break
            except Exception as e:
                print(f"[ERROR] Poll loop error: {e}")

            time.sleep(self.poll_interval)

    def stop(self):
        """Stop the worker."""
        self.running = False

    def get_stats(self) -> Dict:
        """Get worker statistics."""
        with self._lock:
            return {
                "agent_id": self.agent_id,
                "running": self.running,
                "active_tasks": self.active_tasks,
                **self.stats
            }


def parse_capabilities(cap_str: str) -> Dict[str, float]:
    """Parse capability string like 'web:1.0,research:0.9' into dict."""
    caps = {}
    for pair in cap_str.split(","):
        pair = pair.strip()
        if ":" in pair:
            name, conf = pair.split(":", 1)
            caps[name.strip()] = float(conf.strip())
        else:
            caps[pair] = 1.0
    return caps


def main():
    parser = argparse.ArgumentParser(description="Agent Cluster Worker")
    parser.add_argument("--id", required=True, help="Unique agent ID")
    parser.add_argument("--coordinator", default="http://localhost:8080",
                       help="Coordinator URL")
    parser.add_argument("--capabilities", default="generic:1.0",
                       help="Capabilities as name:confidence pairs, comma-separated")
    parser.add_argument("--poll-interval", type=float, default=5.0,
                       help="Seconds between task polls")
    parser.add_argument("--max-concurrent", type=int, default=1,
                       help="Max concurrent tasks")
    parser.add_argument("--config", default=None,
                       help="Path to config file (YAML or JSON). Auto-discovers ./config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    capabilities = parse_capabilities(args.capabilities)
    worker = WorkerAgent(
        agent_id=args.id,
        coordinator_url=args.coordinator,
        capabilities=capabilities,
        poll_interval=args.poll_interval,
        max_concurrent=args.max_concurrent,
        config=config,
    )
    worker.run()


if __name__ == "__main__":
    main()
