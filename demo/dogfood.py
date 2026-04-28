#!/usr/bin/env python3
"""
Cluster Dogfood Challenge
==========================
The agent cluster runs on itself — 3 specialized workers that maintain
the cluster's own codebase: test runner, roadmap implementer, bug reporter.

This is recursive self-improvement: the system uses itself to improve itself.

Workers:
1. test-runner  — runs pytest on schedule, reports failures to coordinator
2. roadmap-bot  — reads ROADMAP_TO_PRODUCTION.md, generates implementation stubs
3. bug-hound    — monitors the other two, files bug reports as peer messages

All three communicate via the coordinator API, send files, and cross-verify.
"""

import json
import os
import re
import sys
import time
import threading
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

COORD_URL = os.environ.get("COORD_URL", "http://localhost:8080")
PROJECT_ROOT = "/tmp/agent-cluster-extract"
DOGFOOD_DIR = f"{PROJECT_ROOT}/dogfood_results"
HEARTBEAT_INTERVAL = 10  # seconds
TEST_RUN_INTERVAL = 60   # seconds between test runs
POLL_INTERVAL = 5        # seconds between task polls


def api_post(path, data, timeout=5):
    try:
        req = urllib.request.Request(
            f"{COORD_URL}{path}",
            data=json.dumps(data).encode(),
            headers={"Content-Type": "application/json"},
        )
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def api_get(path, timeout=5):
    try:
        resp = urllib.request.urlopen(f"{COORD_URL}{path}", timeout=timeout)
        return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def send_peer_message(sender, recipient, msg_type, payload):
    return api_post("/peer-message", {
        "sender_id": sender,
        "recipient_id": recipient,
        "message_type": msg_type,
        "payload": payload,
    })


def send_file(sender, recipient, filename, content):
    return api_post("/file-send", {
        "sender_id": sender,
        "recipient_id": recipient,
        "message_type": "file_send",
        "payload": {"filename": filename, "content": content},
    })


# ═══════════════════════════════════════════════════════════════════
# Worker 1: test-runner
# ═══════════════════════════════════════════════════════════════════

class TestRunner:
    """Runs the cluster's own test suite and reports results."""

    def __init__(self):
        self.agent_id = "test-runner"
        self.name = "Test Runner"
        self.running = True
        self.last_run = None
        self.run_count = 0
        self.failure_history = []

    def register(self):
        result = api_post("/register", {
            "agent_id": self.agent_id,
            "name": self.name,
            "capabilities": {
                "testing": {"confidence": 0.95, "description": "Run test suites and report failures"},
                "quality_assurance": {"confidence": 0.90, "description": "Code quality verification"},
            },
        })
        print(f"[test-runner] Registered: {result}")
        return "error" not in result

    def heartbeat_loop(self):
        while self.running:
            try:
                api_post("/heartbeat", {"agent_id": self.agent_id})
            except:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def run_tests(self):
        """Run pytest and capture results."""
        self.run_count += 1
        run_id = f"test-run-{self.run_count}"
        print(f"[test-runner] Running test suite (run #{self.run_count})...")

        try:
            result = subprocess.run(
                ["python3", "-m", "pytest", "tests/", "-v", "--tb=short", "-q", "--no-header"],
                capture_output=True, text=True, timeout=120,
                cwd=PROJECT_ROOT,
            )
            output = result.stdout + result.stderr
        except subprocess.TimeoutExpired:
            output = "TIMEOUT: Test suite exceeded 120 seconds"
            result = type("obj", (object,), {"returncode": -1})()
        except Exception as e:
            output = f"ERROR: {e}"
            result = type("obj", (object,), {"returncode": -1})()

        # Parse results
        passed = len(re.findall(r"\bPASSED\b", output))
        failed = len(re.findall(r"\bFAILED\b", output))
        errors = len(re.findall(r"\bERROR\b", output))
        skipped = len(re.findall(r"\bSKIPPED\b", output))

        # Extract failed test names
        failed_tests = re.findall(r"FAILED\s+(.+?)(?:\s*-|\s*$)", output)
        error_tests = re.findall(r"ERROR\s+(.+?)(?:\s*-|\s*$)", output)

        test_report = {
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "return_code": result.returncode,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "skipped": skipped,
            "total": passed + failed + errors + skipped,
            "failed_tests": failed_tests,
            "error_tests": error_tests,
            "output_preview": output[-2000:] if len(output) > 2000 else output,
        }

        self.last_run = test_report
        self.failure_history.append(test_report)

        # Save report
        os.makedirs(DOGFOOD_DIR, exist_ok=True)
        with open(f"{DOGFOOD_DIR}/test_report_{self.run_count}.json", "w") as f:
            json.dump(test_report, f, indent=2, default=str)

        # Send results to coordinator
        send_peer_message(self.agent_id, "coordinator", "status_response", {
            "type": "test_results",
            "run_id": run_id,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "total": test_report["total"],
            "status": "PASS" if failed == 0 and errors == 0 else "FAIL",
        })

        # Notify bug-hound if there are failures
        if failed > 0 or errors > 0:
            send_peer_message(self.agent_id, "bug-hound", "peer_request", {
                "type": "test_failures",
                "run_id": run_id,
                "failed_tests": failed_tests,
                "error_tests": error_tests,
                "output_preview": output[-1000:],
            })

        # Send full report as file to bug-hound
        send_file(self.agent_id, "bug-hound",
                  f"test_report_{self.run_count}.json",
                  json.dumps(test_report, indent=2, default=str))

        print(f"[test-runner] Results: {passed} passed, {failed} failed, {errors} errors")
        return test_report

    def run_loop(self):
        """Main loop: register, heartbeat, run tests periodically."""
        if not self.register():
            print("[test-runner] FATAL: Could not register")
            return

        # Start heartbeat
        hb = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb.start()

        # Initial test run
        self.run_tests()

        # Periodic test runs
        while self.running:
            time.sleep(TEST_RUN_INTERVAL)
            if self.running:
                self.run_tests()


# ═══════════════════════════════════════════════════════════════════
# Worker 2: roadmap-bot
# ═══════════════════════════════════════════════════════════════════

class RoadmapBot:
    """Reads ROADMAP_TO_PRODUCTION.md and generates implementation stubs."""

    def __init__(self):
        self.agent_id = "roadmap-bot"
        self.name = "Roadmap Bot"
        self.running = True
        self.stubs_generated = 0

    def register(self):
        result = api_post("/register", {
            "agent_id": self.agent_id,
            "name": self.name,
            "capabilities": {
                "coding": {"confidence": 0.85, "description": "Generate code stubs and implementations"},
                "planning": {"confidence": 0.90, "description": "Read roadmaps and plan implementations"},
            },
        })
        print(f"[roadmap-bot] Registered: {result}")
        return "error" not in result

    def heartbeat_loop(self):
        while self.running:
            try:
                api_post("/heartbeat", {"agent_id": self.agent_id})
            except:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def read_roadmap(self):
        """Parse ROADMAP_TO_PRODUCTION.md for remaining items."""
        roadmap_path = f"{PROJECT_ROOT}/ROADMAP_TO_PRODUCTION.md"
        if not os.path.exists(roadmap_path):
            print("[roadmap-bot] No ROADMAP_TO_PRODUCTION.md found")
            return []

        with open(roadmap_path, "r") as f:
            content = f.read()

        # Extract "Remaining Work" section
        remaining_match = re.search(r"### Remaining Work\n(.+?)(?=\n---|\n###|\Z)", content, re.DOTALL)
        if not remaining_match:
            return []

        remaining = remaining_match.group(1)
        items = []
        for line in remaining.split("\n"):
            m = re.match(r"\d+\.\s+(.+)", line.strip())
            if m:
                items.append(m.group(1).strip())

        return items

    def generate_stub(self, item_text):
        """Generate a code stub for a roadmap item."""
        self.stubs_generated += 1
        stub_id = f"stub-{self.stubs_generated:03d}"

        # Parse the item into a module/class name
        slug = re.sub(r"[^a-z0-9]+", "_", item_text.lower())[:40].strip("_")

        stub = {
            "stub_id": stub_id,
            "roadmap_item": item_text,
            "module_name": slug,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "code": f'"""\nImplementation stub for: {item_text}\nGenerated by roadmap-bot ({stub_id})\n"""\n\nimport logging\n\nlogger = logging.getLogger(__name__)\n\n\nclass {"".join(w.capitalize() for w in slug.split("_"))}:\n    """TODO: Implement {item_text}."""\n\n    def __init__(self):\n        self.ready = False\n        logger.info(f"Stub initialized: {{self.__class__.__name__}}")\n\n    def execute(self):\n        """Main execution — TODO: implement."""\n        raise NotImplementedError(\n            f"Roadmap item not yet implemented: {item_text}"\n        )\n',
            "tests": f'"""\nTests for: {item_text}\nGenerated by roadmap-bot ({stub_id})\n"""\n\nimport unittest\nfrom {slug} import {"".join(w.capitalize() for w in slug.split("_"))}\n\n\nclass Test{"".join(w.capitalize() for w in slug.split("_"))}(unittest.TestCase):\n    def setUp(self):\n        self.impl = {"".join(w.capitalize() for w in slug.split("_"))}()\n\n    def test_init(self):\n        """Stub initializes without error."""\n        self.assertFalse(self.impl.ready)\n\n    def test_execute_not_implemented(self):\n        """execute() raises NotImplementedError."""\n        with self.assertRaises(NotImplementedError):\n            self.impl.execute()\n\n\nif __name__ == "__main__":\n    unittest.main()\n',
        }

        # Save stub
        os.makedirs(f"{DOGFOOD_DIR}/stubs", exist_ok=True)
        with open(f"{DOGFOOD_DIR}/stubs/{slug}.py", "w") as f:
            f.write(stub["code"])
        with open(f"{DOGFOOD_DIR}/stubs/test_{slug}.py", "w") as f:
            f.write(stub["tests"])

        # Send to coordinator
        send_peer_message(self.agent_id, "coordinator", "status_response", {
            "type": "stub_generated",
            "stub_id": stub_id,
            "roadmap_item": item_text,
            "module": slug,
        })

        # Send file to bug-hound for review
        send_file(self.agent_id, "bug-hound",
                  f"stub_{slug}.py",
                  stub["code"])

        print(f"[roadmap-bot] Generated stub: {slug} for '{item_text[:50]}'")
        return stub

    def process_roadmap(self):
        """Read roadmap and generate stubs for all remaining items."""
        items = self.read_roadmap()
        if not items:
            print("[roadmap-bot] No remaining roadmap items found")
            return []

        print(f"[roadmap-bot] Found {len(items)} remaining roadmap items")
        stubs = []
        for item in items:
            stub = self.generate_stub(item)
            stubs.append(stub)
            time.sleep(1)  # Rate limit

        # Summary report
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "items_found": len(items),
            "stubs_generated": len(stubs),
            "stubs": [{"stub_id": s["stub_id"], "module": s["module_name"], "item": s["roadmap_item"]} for s in stubs],
        }

        os.makedirs(DOGFOOD_DIR, exist_ok=True)
        with open(f"{DOGFOOD_DIR}/roadmap_report.json", "w") as f:
            json.dump(report, f, indent=2, default=str)

        return stubs

    def run_loop(self):
        """Main loop: register, heartbeat, process roadmap once, then poll for tasks."""
        if not self.register():
            print("[roadmap-bot] FATAL: Could not register")
            return

        hb = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb.start()

        # Process roadmap on startup
        self.process_roadmap()

        # Then poll for delegated tasks
        while self.running:
            time.sleep(POLL_INTERVAL)


# ═══════════════════════════════════════════════════════════════════
# Worker 3: bug-hound
# ═══════════════════════════════════════════════════════════════════

class BugHound:
    """Monitors other workers and files bug reports as peer messages."""

    def __init__(self):
        self.agent_id = "bug-hound"
        self.name = "Bug Hound"
        self.running = True
        self.bugs_filed = 0
        self.reports = []

    def register(self):
        result = api_post("/register", {
            "agent_id": self.agent_id,
            "name": self.name,
            "capabilities": {
                "monitoring": {"confidence": 0.90, "description": "Monitor agent health and report issues"},
                "bug_reporting": {"confidence": 0.95, "description": "File and track bug reports"},
                "verification": {"confidence": 0.85, "description": "Cross-verify agent outputs"},
            },
        })
        print(f"[bug-hound] Registered: {result}")
        return "error" not in result

    def heartbeat_loop(self):
        while self.running:
            try:
                api_post("/heartbeat", {"agent_id": self.agent_id})
            except:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def file_bug(self, title, description, severity="medium", source="unknown", details=None):
        """File a bug report."""
        self.bugs_filed += 1
        bug_id = f"bug-{self.bugs_filed:03d}"

        bug = {
            "bug_id": bug_id,
            "title": title,
            "description": description,
            "severity": severity,
            "source": source,
            "details": details or {},
            "filed_at": datetime.now(timezone.utc).isoformat(),
            "status": "open",
        }

        self.reports.append(bug)

        # Save
        os.makedirs(DOGFOOD_DIR, exist_ok=True)
        with open(f"{DOGFOOD_DIR}/bug_{bug_id}.json", "w") as f:
            json.dump(bug, f, indent=2, default=str)

        # Notify coordinator
        send_peer_message(self.agent_id, "coordinator", "error_report", {
            "bug_id": bug_id,
            "title": title,
            "severity": severity,
            "source": source,
        })

        # Notify test-runner
        send_peer_message(self.agent_id, "test-runner", "peer_notify", {
            "type": "bug_filed",
            "bug_id": bug_id,
            "title": title,
            "severity": severity,
        })

        print(f"[bug-hound] Filed {bug_id}: [{severity.upper()}] {title}")
        return bug

    def review_test_results(self):
        """Check latest test results and file bugs for failures."""
        # Get conversation log to find test results
        conv_log = api_get("/conversation-log")
        if not isinstance(conv_log, list):
            return

        for msg in conv_log:
            if msg.get("message_type") == "peer_request" and msg.get("sender_id") == "test-runner":
                payload = msg.get("payload", {})
                if payload.get("type") == "test_failures":
                    failed_tests = payload.get("failed_tests", [])
                    for test_name in failed_tests:
                        # Check if we already filed a bug for this test
                        already_filed = any(
                            test_name in r.get("title", "")
                            for r in self.reports
                        )
                        if not already_filed:
                            self.file_bug(
                                title=f"Test failure: {test_name}",
                                description=f"Test '{test_name}' failed in run {payload.get('run_id', 'unknown')}. See test-runner output for details.",
                                severity="high",
                                source="test-runner",
                                details={"test_name": test_name, "run_id": payload.get("run_id")},
                            )

    def review_stubs(self):
        """Review generated stubs and file issues."""
        stubs_dir = f"{DOGFOOD_DIR}/stubs"
        if not os.path.exists(stubs_dir):
            return

        for fn in os.listdir(stubs_dir):
            if fn.endswith(".py") and not fn.startswith("test_"):
                fp = os.path.join(stubs_dir, fn)
                with open(fp, "r") as f:
                    code = f.read()

                # Check for common issues
                if "NotImplementedError" in code:
                    # This is expected for stubs, but note it
                    pass

                # Try to compile
                try:
                    compile(code, fp, "exec")
                except SyntaxError as e:
                    self.file_bug(
                        title=f"Syntax error in stub: {fn}",
                        description=f"Stub {fn} has syntax error: {e}",
                        severity="high",
                        source="roadmap-bot",
                        details={"file": fn, "error": str(e)},
                    )

    def monitor_loop(self):
        """Periodic monitoring loop."""
        while self.running:
            time.sleep(15)
            if self.running:
                self.review_test_results()
                self.review_stubs()

                # Check agent health
                status = api_get("/status")
                if isinstance(status, dict):
                    agents = status.get("agents", [])
                    for agent in agents:
                        if not agent.get("is_alive") and agent.get("agent_id") in ("test-runner", "roadmap-bot"):
                            self.file_bug(
                                title=f"Agent offline: {agent.get('agent_id')}",
                                description=f"Worker agent {agent.get('agent_id')} is not responding to heartbeats",
                                severity="critical",
                                source="coordinator",
                            )

    def run_loop(self):
        """Main loop: register, heartbeat, monitor."""
        if not self.register():
            print("[bug-hound] FATAL: Could not register")
            return

        hb = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb.start()

        # Start monitoring
        monitor = threading.Thread(target=self.monitor_loop, daemon=True)
        monitor.start()

        # Main loop — file initial health report
        time.sleep(2)
        status = api_get("/status")
        if isinstance(status, dict):
            online = status.get("agents_online", 0)
            total = status.get("agents_total", 0)
            print(f"[bug-hound] Cluster status: {online}/{total} agents online")

        while self.running:
            time.sleep(POLL_INTERVAL)

    def get_summary(self):
        """Get bug report summary."""
        by_severity = defaultdict(list)
        for bug in self.reports:
            by_severity[bug["severity"]].append(bug["title"])

        return {
            "total_bugs": len(self.reports),
            "by_severity": {k: len(v) for k, v in by_severity.items()},
            "bugs": by_severity,
        }


# ═══════════════════════════════════════════════════════════════════
# Main — Run all 3 dogfood workers
# ═══════════════════════════════════════════════════════════════════

def main():
    print("🐕 CLUSTER DOGFOOD CHALLENGE")
    print(f"   Project: {PROJECT_ROOT}")
    print(f"   Coordinator: {COORD_URL}")
    print(f"   Workers: test-runner, roadmap-bot, bug-hound")
    print()

    # Create results directory
    os.makedirs(DOGFOOD_DIR, exist_ok=True)

    # Create worker instances
    test_runner = TestRunner()
    roadmap_bot = RoadmapBot()
    bug_hound = BugHound()

    # Start all workers in threads
    workers = [
        threading.Thread(target=test_runner.run_loop, daemon=True, name="test-runner"),
        threading.Thread(target=roadmap_bot.run_loop, daemon=True, name="roadmap-bot"),
        threading.Thread(target=bug_hound.run_loop, daemon=True, name="bug-hound"),
    ]

    for w in workers:
        w.start()
        time.sleep(1)  # Stagger registration

    print("\n  All 3 dogfood workers running. Press Ctrl+C to stop.\n")

    # Wait for first test run to complete
    time.sleep(15)

    # Print summary
    if test_runner.last_run:
        r = test_runner.last_run
        print(f"\n{'═' * 60}")
        print("🐕 DOGFOOD RESULTS")
        print(f"{'═' * 60}")
        print(f"  Test Runner: {r['passed']} passed, {r['failed']} failed, {r['errors']} errors")
        print(f"  Roadmap Bot: {roadmap_bot.stubs_generated} stubs generated")
        print(f"  Bug Hound:   {bug_hound.bugs_filed} bugs filed")

        if bug_hound.reports:
            print(f"\n  Bug Reports:")
            for bug in bug_hound.reports:
                icon = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(bug["severity"], "⚪")
                print(f"    {icon} [{bug['severity'].upper()}] {bug['title']}")

        print(f"\n  Results saved to: {DOGFOOD_DIR}/")
        print(f"{'═' * 60}")

    # Keep running until interrupted
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n\n  ⏹  Stopping dogfood workers...")
        test_runner.running = False
        roadmap_bot.running = False
        bug_hound.running = False

        # Final summary
        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "test_runs": test_runner.run_count,
            "test_results": test_runner.last_run,
            "stubs_generated": roadmap_bot.stubs_generated,
            "bugs_filed": bug_hound.bugs_filed,
            "bug_summary": bug_hound.get_summary(),
        }
        with open(f"{DOGFOOD_DIR}/dogfood_summary.json", "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print("  ✓ Summary saved. Goodbye!")


if __name__ == "__main__":
    main()
