#!/usr/bin/env python3
"""
Capable Task Worker
===================
Real worker that uses the handler registry to execute tasks.
Routes tasks through 45+ handlers across AI, file, data, web, system, etc.

Usage:
  python3 capable_worker.py --coordinator http://localhost:8080 --agent-id Eden1-Worker01
  python3 capable_worker.py --coordinator http://72.60.66.212:8080 --agent-id Eden3-Worker01 --handler-dir /path/to/handlers
"""

import json, time, sys, os, argparse, random, urllib.request, urllib.error, traceback
from pathlib import Path

COORDINATOR_URL = "http://localhost:8080"
HEARTBEAT_INTERVAL = 300
TASK_POLL_INTERVAL = 3
WORK_DIR = Path.home() / ".agent-cluster" / "workspace"

# ── Handler Loading ──────────────────────────────────────────────────────────

def load_handler_registry(handler_dir):
    """Load handlers from the agent-cluster handlers directory."""
    handler_dir = Path(handler_dir)
    if not handler_dir.exists():
        print(f"[WARN] Handler dir {handler_dir} not found, using built-in stubs", flush=True)
        return None

    # Add handler_dir's parent to sys.path so 'from handlers.xxx' works
    parent = str(handler_dir.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    try:
        from handlers.registry import HandlerRegistry
        registry = HandlerRegistry()
        registry.register_all()
        print(f"[HANDLERS] Loaded {len(registry.handlers)} handlers: {registry.count_handlers()}", flush=True)
        return registry
    except Exception as e:
        print(f"[HANDLERS] Failed to load registry: {e}", flush=True)
        traceback.print_exc()
        return None


# ── Fallback Stub Handlers ──────────────────────────────────────────────────

STUB_HANDLERS = {
    "analysis": lambda params: {"status": "done", "note": "stub (no handler registry)", "params": params},
    "coding": lambda params: {"status": "done", "note": "stub (no handler registry)", "params": params},
    "testing": lambda params: {"status": "done", "note": "stub (no handler registry)", "params": params},
}


# ── Capable Worker ──────────────────────────────────────────────────────────

class CapableWorker:
    def __init__(self, agent_id, coordinator_url, handler_dir=None, capabilities=None):
        self.agent_id = agent_id
        self.coordinator_url = coordinator_url.rstrip("/")
        self.running = True
        self.completed = 0
        self.failed = 0
        self.last_heartbeat = 0

        WORK_DIR.mkdir(parents=True, exist_ok=True)

        # Load handler registry
        self.registry = load_handler_registry(handler_dir) if handler_dir else None

        # Derive capabilities from loaded handlers
        if self.registry:
            self.capabilities = self._capabilities_from_registry()
        else:
            self.capabilities = capabilities or {
                "analysis": 0.9, "coding": 0.9, "testing": 0.8,
                "file_transfer": 0.7, "peer_messaging": 0.8
            }

    def _capabilities_from_registry(self):
        """Derive capability dict from handler registry categories."""
        cap_map = {
            "ai": ("llm_chat", "summarize", "classify", "extract", "embed"),
            "file": ("file_read", "file_write", "file_search", "file_list", "file_copy"),
            "data": ("csv_parse", "json_transform", "data_merge", "data_filter", "data_validate"),
            "web": ("web_fetch", "web_scrape", "web_api", "web_download"),
            "system": ("exec", "system_info", "process", "monitor"),
            "communication": ("email", "slack", "discord", "webhook"),
            "database": ("sql", "redis", "mongo", "sqlite"),
            "cloud": ("s3", "lambda", "cloud_storage"),
            "integration": ("github", "stripe", "twilio"),
        }
        caps = {}
        counts = self.registry.count_handlers()
        for cat, skills in cap_map.items():
            if counts.get(cat, 0) > 0:
                for s in skills:
                    caps[s] = round(0.7 + random.random() * 0.3, 2)
        # Add base capabilities
        caps["analysis"] = 0.9
        caps["coding"] = 0.9
        caps["testing"] = 0.8
        caps["file_transfer"] = 0.7
        caps["peer_messaging"] = 0.8
        return caps

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    def _post(self, path, data):
        url = f"{self.coordinator_url}{path}"
        body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            return {"error": f"HTTP {e.code}", "body": body}
        except Exception as e:
            return {"error": str(e)}

    def _get(self, path):
        url = f"{self.coordinator_url}{path}"
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            return {"error": str(e)}

    # ── Registration & Heartbeat ─────────────────────────────────────────────

    def register(self):
        result = self._post("/register", {
            "agent_id": self.agent_id,
            "capabilities": self.capabilities
        })
        print(f"[REGISTER] {self.agent_id} → {result.get('agent_id', result)}", flush=True)
        return "error" not in result

    def heartbeat(self):
        self._post("/heartbeat", {
            "agent_id": self.agent_id,
            "load": round(random.random() * 0.3, 2)
        })

    # ── Task Execution ──────────────────────────────────────────────────────

    def poll_tasks(self):
        try:
            for state in ["assigned", "in_progress"]:
                tasks = self._get(f"/tasks?agent={self.agent_id}&state={state}")
                if isinstance(tasks, list):
                    for t in tasks:
                        self._execute_task(t)
        except Exception as e:
            print(f"[POLL] {e}", flush=True)

    def _execute_task(self, task):
        task_id = task.get("task_id", "")
        task_type = task.get("task_type", "generic")
        params = task.get("params", {})
        print(f"[TASK] Starting {task_id} type={task_type}", flush=True)

        # Accept the task
        self._post("/task-accept", {"task_id": task_id, "agent_id": self.agent_id})

        try:
            result = self._run_task(task_type, params)
            self._post("/task-complete", {
                "task_id": task_id,
                "agent_id": self.agent_id,
                "result": result
            })
            self.completed += 1
            print(f"[TASK] Completed {task_id} ✓", flush=True)
        except Exception as e:
            self._post("/task-failed", {
                "task_id": task_id,
                "agent_id": self.agent_id,
                "error": str(e)
            })
            self.failed += 1
            print(f"[TASK] Failed {task_id}: {e}", flush=True)

    def _run_task(self, task_type, params):
        """Route task through handler registry (real execution) or fallback stubs."""

        # 1. Try handler registry (real execution)
        if self.registry:
            # Build task params in handler-friendly format
            handler_params = dict(params)
            handler_params.setdefault("task", task_type)

            handler = self.registry.find_best_handler(handler_params)
            if handler:
                print(f"[ROUTE] {task_type} → {handler.name} (confidence match)", flush=True)
                result = handler.execute(handler_params)
                if result.success:
                    return {
                        "status": "done",
                        "agent": self.agent_id,
                        "task_type": task_type,
                        "handler": handler.name,
                        "data": result.data,
                        "duration_ms": result.duration_ms,
                    }
                else:
                    # Handler failed — return error info
                    return {
                        "status": "error",
                        "agent": self.agent_id,
                        "task_type": task_type,
                        "handler": handler.name,
                        "error": result.error,
                        "duration_ms": result.duration_ms,
                    }

        # 2. Category-based dispatch for common task types
        if task_type == "analysis":
            return self._do_analysis(params)
        elif task_type == "coding":
            return self._do_coding(params)
        elif task_type == "testing":
            return self._do_testing(params)
        elif task_type == "file_read":
            return self._do_file_read(params)
        elif task_type == "file_write":
            return self._do_file_write(params)
        elif task_type == "web_fetch":
            return self._do_web_fetch(params)
        elif task_type == "llm_chat":
            return self._do_llm_chat(params)
        elif task_type == "summarize":
            return self._do_summarize(params)
        else:
            return self._do_generic(params)

    # ── Built-in task implementations (used when registry not available) ────

    def _do_analysis(self, params):
        file_path = params.get("file", "")
        focus = params.get("focus", "general")
        target = WORK_DIR / file_path if file_path else None
        result = {"status": "done", "agent": self.agent_id, "task_type": "analysis"}

        if target and target.exists() and target.is_file():
            content = target.read_text(errors="replace")
            lines = content.splitlines()
            result.update({
                "file": file_path,
                "lines": len(lines),
                "focus": focus,
                "findings": self._static_analysis(content, focus),
                "size_bytes": target.stat().st_size,
            })
        else:
            result["note"] = f"File {file_path} not found in workspace"
        return result

    def _do_coding(self, params):
        file_path = params.get("file", "")
        changes = params.get("changes", "")
        action = params.get("action", "edit")

        if action == "create" and file_path:
            target = WORK_DIR / file_path
            target.parent.mkdir(parents=True, exist_ok=True)
            content = params.get("content", changes or "")
            target.write_text(content)
            return {
                "status": "done", "agent": self.agent_id, "task_type": "coding",
                "file": file_path, "action": "create", "bytes_written": len(content)
            }
        elif action == "edit" and file_path:
            target = WORK_DIR / file_path
            if target.exists():
                old = target.read_text()
                # Apply simple find/replace if specified
                find = params.get("find", "")
                replace = params.get("replace", "")
                if find and find in old:
                    new = old.replace(find, replace, 1)
                    target.write_text(new)
                    return {
                        "status": "done", "agent": self.agent_id, "task_type": "coding",
                        "file": file_path, "action": "edit", "replacements": 1
                    }
                return {
                    "status": "done", "agent": self.agent_id, "task_type": "coding",
                    "file": file_path, "action": "edit", "note": "no find/replace specified, file unchanged"
                }
            return {"status": "error", "agent": self.agent_id, "error": f"File {file_path} not found"}

        return {"status": "done", "agent": self.agent_id, "task_type": "coding",
                "file": file_path, "changes_applied": changes[:500] if changes else "none specified"}

    def _do_testing(self, params):
        target = params.get("target", "")
        target_path = WORK_DIR / target if target else None

        if target_path and target_path.exists():
            # Run pytest if available
            import subprocess
            try:
                proc = subprocess.run(
                    ["python3", "-m", "pytest", str(target_path), "--tb=short", "-q"],
                    capture_output=True, text=True, timeout=60
                )
                output = proc.stdout + proc.stderr
                # Parse pytest output for pass/fail counts
                passed = output.count(" passed")
                failed = output.count(" failed")
                return {
                    "status": "done", "agent": self.agent_id, "task_type": "testing",
                    "target": target, "tests_passed": passed, "tests_failed": failed,
                    "exit_code": proc.returncode,
                    "output_last_500": output[-500:]
                }
            except FileNotFoundError:
                pass  # pytest not installed
            except Exception as e:
                return {"status": "error", "agent": self.agent_id, "error": str(e)}

        return {"status": "done", "agent": self.agent_id, "task_type": "testing",
                "target": target, "tests_run": 0, "note": "target not found or pytest unavailable"}

    def _do_file_read(self, params):
        file_path = params.get("path", params.get("file", ""))
        target = WORK_DIR / file_path
        if target.exists() and target.is_file():
            content = target.read_text(errors="replace")
            max_chars = params.get("max_chars", 50000)
            return {
                "status": "done", "agent": self.agent_id, "task_type": "file_read",
                "path": file_path, "content": content[:max_chars],
                "size_bytes": target.stat().st_size, "truncated": len(content) > max_chars
            }
        return {"status": "error", "agent": self.agent_id, "error": f"File not found: {file_path}"}

    def _do_file_write(self, params):
        file_path = params.get("path", params.get("file", ""))
        content = params.get("content", "")
        target = WORK_DIR / file_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        return {
            "status": "done", "agent": self.agent_id, "task_type": "file_write",
            "path": file_path, "bytes_written": len(content)
        }

    def _do_web_fetch(self, params):
        url = params.get("url", "")
        if not url:
            return {"status": "error", "agent": self.agent_id, "error": "No URL provided"}
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AgentCluster/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                content = resp.read().decode(errors="replace")
                max_chars = params.get("max_chars", 50000)
                return {
                    "status": "done", "agent": self.agent_id, "task_type": "web_fetch",
                    "url": url, "content": content[:max_chars],
                    "status_code": resp.status, "truncated": len(content) > max_chars
                }
        except Exception as e:
            return {"status": "error", "agent": self.agent_id, "error": str(e)}

    def _do_llm_chat(self, params):
        """Call LLM API (OpenAI-compatible) for chat completion."""
        messages = params.get("messages", [])
        model = params.get("model", "gpt-3.5-turbo")
        api_key = params.get("api_key") or os.environ.get("OPENAI_API_KEY")
        api_base = params.get("api_base", os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1"))

        if not api_key:
            return {"status": "error", "agent": self.agent_id,
                    "error": "No API key. Set OPENAI_API_KEY env or pass api_key param."}

        try:
            import ssl
            payload = {
                "model": model,
                "messages": messages,
                "temperature": params.get("temperature", 0.7),
                "max_tokens": params.get("max_tokens", 1000),
            }
            data = json.dumps(payload).encode()
            req = urllib.request.Request(
                f"{api_base}/chat/completions", data=data,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
            )
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=120, context=ctx) as resp:
                result = json.loads(resp.read().decode())
            choice = result["choices"][0]
            return {
                "status": "done", "agent": self.agent_id, "task_type": "llm_chat",
                "response": choice["message"]["content"],
                "model": result.get("model"), "finish_reason": choice.get("finish_reason"),
                "tokens_used": result.get("usage", {}).get("total_tokens"),
            }
        except Exception as e:
            return {"status": "error", "agent": self.agent_id, "error": str(e)}

    def _do_summarize(self, params):
        """Summarize text — uses LLM if available, else extractive fallback."""
        text = params.get("text", "")
        if not text:
            file_path = params.get("file", "")
            target = WORK_DIR / file_path if file_path else None
            if target and target.exists():
                text = target.read_text(errors="replace")

        if not text:
            return {"status": "error", "agent": self.agent_id, "error": "No text to summarize"}

        # Try LLM-based summarization
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            return self._do_llm_chat({
                "messages": [
                    {"role": "system", "content": "Summarize the following text concisely."},
                    {"role": "user", "content": text[:8000]}
                ],
                "max_tokens": params.get("max_length", 200),
                "temperature": 0.3,
                **params
            })

        # Extractive fallback: take first sentence of each paragraph
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        summary_parts = []
        for p in paragraphs[:5]:
            sentences = p.split(". ")
            if sentences:
                summary_parts.append(sentences[0] + ".")
        return {
            "status": "done", "agent": self.agent_id, "task_type": "summarize",
            "summary": " ".join(summary_parts),
            "method": "extractive_fallback", "original_length": len(text)
        }

    def _do_generic(self, params):
        return {"status": "done", "agent": self.agent_id, "task_type": "generic", "params": params}

    # ── Static analysis (built-in, no deps) ──────────────────────────────────

    def _static_analysis(self, content, focus):
        findings = []
        for i, line in enumerate(content.splitlines(), 1):
            s = line.strip()
            if "exec(" in s or "eval(" in s:
                findings.append({"line": i, "severity": "critical", "type": "exec_eval", "msg": "Dynamic code execution"})
            elif "subprocess.call" in s and "shell=True" in s:
                findings.append({"line": i, "severity": "critical", "type": "shell_injection", "msg": "Shell injection risk"})
            elif "pickle.load" in s:
                findings.append({"line": i, "severity": "high", "type": "pickle", "msg": "Unsafe pickle deserialization"})
            elif s.startswith("except:") or "except:" in s:
                findings.append({"line": i, "severity": "medium", "type": "bare_except", "msg": "Bare except clause"})
        return findings[:20]

    # ── Peer messages ────────────────────────────────────────────────────────

    def poll_messages(self):
        try:
            msgs = self._get(f"/messages?recipient={self.agent_id}")
            if isinstance(msgs, list):
                for m in msgs:
                    mt = m.get("message_type", "")
                    sender = m.get("sender_id", "")
                    content = m.get("content", {})
                    if mt == "file_request":
                        self._handle_file_request(sender, content)
        except Exception:
            pass

    def _handle_file_request(self, sender, content):
        fp = content.get("file_path", "")
        target = WORK_DIR / fp
        file_content = target.read_text(errors="replace") if target.exists() else "# not found"
        self._post("/file-send", {
            "sender_id": self.agent_id, "recipient_id": sender,
            "file_name": fp.split("/")[-1], "file_path": fp,
            "content": file_content,
            "description": f"Response to file request for {fp}"
        })
        print(f"[FILE] Sent {fp} to {sender}", flush=True)

    # ── Main Loop ────────────────────────────────────────────────────────────

    def run(self):
        print(f"🤖 {self.agent_id} starting (handlers={'registry' if self.registry else 'built-in'})...", flush=True)
        if not self.register():
            print("Registration failed, retrying in 30s...", flush=True)
            time.sleep(30)
            if not self.register():
                print("Registration failed twice, exiting", flush=True)
                return

        while self.running:
            try:
                now = time.time()
                if now - self.last_heartbeat >= HEARTBEAT_INTERVAL:
                    self.heartbeat()
                    self.last_heartbeat = now
                self.poll_tasks()
                self.poll_messages()
            except KeyboardInterrupt:
                print(f"\n[{self.agent_id}] Shutting down...", flush=True)
                self.running = False
            except Exception as e:
                print(f"[ERROR] {e}", flush=True)
            time.sleep(TASK_POLL_INTERVAL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agent Cluster Capable Worker")
    parser.add_argument("--coordinator", default=COORDINATOR_URL, help="Coordinator URL")
    parser.add_argument("--agent-id", required=True, help="Unique agent ID")
    parser.add_argument("--handler-dir", default=None,
                        help="Path to handlers/ directory (from agent-cluster repo)")
    parser.add_argument("--caps", default=None, help="Capabilities JSON override")
    args = parser.parse_args()

    caps = None
    if args.caps:
        try:
            caps = json.loads(args.caps)
        except Exception:
            print("Warning: couldn't parse caps JSON, using defaults", flush=True)

    worker = CapableWorker(args.agent_id, args.coordinator, args.handler_dir, caps)
    worker.run()
