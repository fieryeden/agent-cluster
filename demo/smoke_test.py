#!/usr/bin/env python3
"""
Agent Cluster — One-Command Smoke Test (Demo)
================================================
`python -m demo` or `python demo/smoke_test.py`

Starts a mock coordinator with 3 agents, runs a realistic business task,
shows delegation + capability discovery + output, prints a summary, cleans up.
30 seconds, zero config, pure stdlib.
"""

import json
import os
import sys
import time
import threading
import random
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone

# ─── Configuration ─────────────────────────────────────────────────
COORD_PORT = 9876
COORD_URL = f"http://localhost:{COORD_PORT}"
TASK_TIMEOUT = 30  # seconds

# ─── Pre-scripted task flow ─────────────────────────────────────────
BUSINESS_TASK = {
    "task_id": "demo-001",
    "title": "Generate a compliance checklist for a real estate syndication",
    "description": (
        "A real estate syndicator needs a compliance checklist for a 506(b) offering. "
        "The checklist must cover: SEC filing requirements, investor accreditation verification, "
        "state blue sky laws, PPM document requirements, and subscription agreement validation."
    ),
    "required_capabilities": ["legal_research", "compliance", "document_generation"],
    "priority": "high",
}

AGENTS = [
    {
        "agent_id": "legal-eagle",
        "name": "Legal Eagle",
        "capabilities": {
            "legal_research": {"confidence": 0.95, "description": "Legal research and analysis"},
            "compliance": {"confidence": 0.92, "description": "Regulatory compliance checking"},
        },
        "role": "Lead researcher — finds regulatory requirements and case law",
    },
    {
        "agent_id": "doc-smith",
        "name": "Doc Smith",
        "capabilities": {
            "document_generation": {"confidence": 0.88, "description": "Document and checklist generation"},
            "compliance": {"confidence": 0.75, "description": "Basic compliance knowledge"},
        },
        "role": "Document producer — assembles the final checklist",
    },
    {
        "agent_id": "verify-vex",
        "name": "Verify Vex",
        "capabilities": {
            "legal_research": {"confidence": 0.70, "description": "Secondary legal research"},
            "compliance": {"confidence": 0.85, "description": "Compliance verification and cross-checking"},
        },
        "role": "Quality assurance — cross-verifies the checklist against requirements",
    },
]

# ─── Mock Coordinator ──────────────────────────────────────────────

class MockCoordinatorHandler(BaseHTTPRequestHandler):
    """Minimal coordinator that handles the demo flow."""

    # State
    agents = {}
    tasks = {}
    messages = []
    conversation_log = []

    def log_message(self, format, *args):
        pass  # Suppress HTTP logging

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if self.path == "/register":
            aid = body.get("agent_id", "unknown")
            self.agents[aid] = {**body, "registered_at": datetime.now(timezone.utc).isoformat(), "is_alive": True}
            self._json(200, {"status": "registered", "agent_id": aid})

        elif self.path == "/heartbeat":
            aid = body.get("agent_id", "unknown")
            if aid in self.agents:
                self.agents[aid]["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
                self.agents[aid]["is_alive"] = True
            self._json(200, {"status": "ok"})

        elif self.path == "/submit-task":
            tid = body.get("task_id", f"task-{len(self.tasks)}")
            self.tasks[tid] = {**body, "status": "submitted", "submitted_at": datetime.now(timezone.utc).isoformat()}
            self._json(200, {"status": "submitted", "task_id": tid})

        elif self.path == "/peer-message":
            msg = {
                "sender_id": body.get("sender_id"),
                "recipient_id": body.get("recipient_id"),
                "message_type": body.get("message_type"),
                "payload": body.get("payload", {}),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.messages.append(msg)
            self.conversation_log.append(msg)
            self._json(200, {"status": "delivered"})

        elif self.path == "/task-delegate":
            msg = {
                "sender_id": body.get("sender_id"),
                "recipient_id": body.get("recipient_id"),
                "message_type": "task_delegate",
                "payload": body.get("payload", {}),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.messages.append(msg)
            self.conversation_log.append(msg)
            self._json(200, {"status": "delegated"})

        elif self.path == "/file-send":
            msg = {
                "sender_id": body.get("sender_id"),
                "recipient_id": body.get("recipient_id"),
                "message_type": "file_send",
                "payload": body.get("payload", {}),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.conversation_log.append(msg)
            self._json(200, {"status": "delivered"})

        elif self.path == "/consensus-vote":
            msg = {
                "sender_id": body.get("sender_id"),
                "message_type": "consensus_vote",
                "payload": body.get("payload", {}),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.conversation_log.append(msg)
            self._json(200, {"status": "recorded"})

        elif self.path == "/capabilities/register":
            aid = body.get("agent_id", "unknown")
            if aid in self.agents:
                self.agents[aid]["capabilities"] = body.get("capabilities", {})
            self._json(200, {"status": "registered"})

        else:
            self._json(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/status":
            alive = [a for a in self.agents.values() if a.get("is_alive")]
            self._json(200, {
                "agents": list(self.agents.values()),
                "tasks": list(self.tasks.values()),
                "message_count": len(self.messages),
                "agents_online": len(alive),
                "agents_total": len(self.agents),
            })
        elif self.path == "/agents":
            self._json(200, list(self.agents.values()))
        elif self.path == "/conversation-log":
            self._json(200, self.conversation_log)
        elif self.path == "/messages":
            self._json(200, self.messages[-20:])
        else:
            self._json(404, {"error": "not found"})

    def _json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())


class MockCoordinator:
    def __init__(self, port):
        self.port = port
        self.server = HTTPServer(("127.0.0.1", port), MockCoordinatorHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.server.shutdown()

    @property
    def handler(self):
        return MockCoordinatorHandler


# ─── Demo Flow ──────────────────────────────────────────────────────

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


def typewriter(text, delay=0.008):
    """Print text character by character for dramatic effect."""
    for char in text:
        sys.stdout.write(char)
        sys.stdout.flush()
        time.sleep(delay)
    print()


def print_header(text):
    width = 60
    print()
    print("═" * width)
    print(f"  {text}")
    print("═" * width)
    print()


def print_step(step_num, title, detail=""):
    print(f"\n┌─ Step {step_num}: {title}")
    if detail:
        for line in detail.split("\n"):
            print(f"│  {line}")
    print(f"└─")


def print_agent_action(agent_id, action, detail=""):
    icon = {"legal-eagle": "⚖️", "doc-smith": "📄", "verify-vex": "🔍"}.get(agent_id, "🤖")
    name = next((a["name"] for a in AGENTS if a["agent_id"] == agent_id), agent_id)
    print(f"  {icon} [{name}] {action}")
    if detail:
        for line in detail.split("\n")[:3]:
            print(f"     {line}")


def run_demo():
    """Main demo flow."""
    print()
    typewriter("🚀 Agent Cluster — One-Command Smoke Test", 0.02)
    typewriter("   30 seconds. Zero config. Watch it work.", 0.015)
    print()

    # ─── Step 1: Start coordinator ─────────────────────────────
    print_header("STARTING COORDINATOR")
    coord = MockCoordinator(COORD_PORT)
    coord.start()
    time.sleep(0.3)
    print(f"  ✓ Mock coordinator running on port {COORD_PORT}")

    # ─── Step 2: Register 3 agents ─────────────────────────────
    print_header("REGISTERING AGENTS")
    for agent in AGENTS:
        result = api_post("/register", {
            "agent_id": agent["agent_id"],
            "name": agent["name"],
            "capabilities": agent["capabilities"],
        })
        if "error" not in result:
            caps = ", ".join(agent["capabilities"].keys())
            print_agent_action(agent["agent_id"], f"registered — capabilities: {caps}")
            print(f"     Role: {agent['role']}")
        time.sleep(0.3)

    status = api_get("/status")
    print(f"\n  ✓ {status.get('agents_online', 0)} agents online, ready for tasks")

    # ─── Step 3: Submit business task ──────────────────────────
    print_header("SUBMITTING BUSINESS TASK")
    task = BUSINESS_TASK
    result = api_post("/submit-task", task)
    print(f"  Task: {task['title']}")
    print(f"  Required capabilities: {', '.join(task['required_capabilities'])}")
    print(f"  Priority: {task['priority']}")
    print(f"  ✓ Task submitted (ID: {task['task_id']})")

    # ─── Step 4: Capability discovery ──────────────────────────
    print_header("CAPABILITY DISCOVERY")
    print("  Coordinator queries agents for matching capabilities...")

    time.sleep(0.5)
    print()
    for agent in AGENTS:
        caps = agent["capabilities"]
        matching = [c for c in task["required_capabilities"] if c in caps]
        if matching:
            print_agent_action(agent["agent_id"], f"has matching capabilities: {', '.join(matching)}")
            for cap in matching:
                conf = caps[cap]["confidence"]
                bar = "█" * int(conf * 20) + "░" * (20 - int(conf * 20))
                print(f"     {cap}: [{bar}] {conf:.0%}")

    time.sleep(0.5)
    print(f"\n  ✓ Best agent for 'legal_research': legal-eagle (95% confidence)")
    print(f"  ✓ Best agent for 'document_generation': doc-smith (88% confidence)")
    print(f"  ✓ Best agent for 'compliance' verification: verify-vex (85% confidence)")

    # ─── Step 5: Task delegation ───────────────────────────────
    print_header("TASK DELEGATION")
    print("  Coordinator splits the task and delegates to agents...")

    # Delegate to legal-eagle
    time.sleep(0.3)
    api_post("/task-delegate", {
        "sender_id": "coordinator",
        "recipient_id": "legal-eagle",
        "payload": {
            "task": "Research 506(b) compliance requirements",
            "subtasks": [
                "SEC filing requirements (Form D, Reg D)",
                "Investor accreditation verification (accredited investor definition)",
                "State blue sky law requirements (top 5 states)",
            ],
        },
    })
    print_agent_action("legal-eagle", "received delegation", "Research 506(b) compliance requirements\n3 subtasks assigned")

    # Delegate to doc-smith
    time.sleep(0.3)
    api_post("/task-delegate", {
        "sender_id": "coordinator",
        "recipient_id": "doc-smith",
        "payload": {
            "task": "Prepare checklist document structure",
            "subtasks": [
                "Create checklist template with sections",
                "Prepare PPM document requirements section",
                "Set up subscription agreement validation section",
            ],
        },
    })
    print_agent_action("doc-smith", "received delegation", "Prepare checklist document structure\n3 subtasks assigned")

    # Delegate to verify-vex
    time.sleep(0.3)
    api_post("/task-delegate", {
        "sender_id": "coordinator",
        "recipient_id": "verify-vex",
        "payload": {
            "task": "Prepare verification criteria",
            "subtasks": [
                "Define compliance verification checkpoints",
                "Cross-reference requirements against regulations",
            ],
        },
    })
    print_agent_action("verify-vex", "received delegation", "Prepare verification criteria\n2 subtasks assigned")

    # ─── Step 6: Agent collaboration ───────────────────────────
    print_header("AGENT COLLABORATION")
    print("  Agents communicate, share findings, and coordinate...")

    time.sleep(0.5)
    # legal-eagle sends research to doc-smith
    api_post("/peer-message", {
        "sender_id": "legal-eagle",
        "recipient_id": "doc-smith",
        "message_type": "peer_request",
        "payload": {
            "content": "SEC Form D filing is required within 15 days of first sale. Reg D Rule 506(b) allows up to 35 non-accredited investors if the issuer provides required disclosures.",
            "sections": ["SEC Filing", "Investor Limits", "Disclosure Requirements"],
        },
    })
    print_agent_action("legal-eagle", "sent research findings to doc-smith", "SEC Form D requirements, Reg D Rule 506(b) provisions")

    time.sleep(0.4)
    # doc-smith requests additional info
    api_post("/peer-message", {
        "sender_id": "doc-smith",
        "recipient_id": "legal-eagle",
        "message_type": "peer_request",
        "payload": {
            "content": "Need clarification on blue sky law filing thresholds — which states require notice filing vs. registration?",
            "request": "blue_sky_details",
        },
    })
    print_agent_action("doc-smith", "requested blue sky law details from legal-eagle")

    time.sleep(0.4)
    # legal-eagle responds
    api_post("/peer-message", {
        "sender_id": "legal-eagle",
        "recipient_id": "doc-smith",
        "message_type": "peer_response",
        "payload": {
            "content": "Most states require notice filing under Rule 506(b) (Form D + fee). CA, NY, TX, FL, IL all require notice filing. No full registration needed for 506(b) exempt offerings.",
            "states": ["California", "New York", "Texas", "Florida", "Illinois"],
        },
    })
    print_agent_action("legal-eagle", "responded with blue sky law details", "5 states require notice filing, no full registration")

    # ─── Step 7: File transfer ─────────────────────────────────
    print_header("FILE TRANSFER")
    print("  Agents share working documents...")

    time.sleep(0.4)
    # doc-smith sends draft checklist to verify-vex
    checklist_draft = {
        "title": "506(b) Real Estate Syndication Compliance Checklist",
        "sections": [
            {"name": "SEC Filing Requirements", "items": [
                "☐ File Form D within 15 days of first sale",
                "☐ Verify Reg D Rule 506(b) exemption eligibility",
                "☐ Ensure no general solicitation",
            ]},
            {"name": "Investor Accreditation", "items": [
                "☐ Verify accredited investor status (income >$200K or net worth >$1M)",
                "☐ Collect investor questionnaires",
                "☐ Limit non-accredited investors to 35",
            ]},
            {"name": "State Blue Sky Laws", "items": [
                "☐ File notice in CA, NY, TX, FL, IL",
                "☐ Pay state filing fees",
                "☐ Verify state-specific exemptions",
            ]},
            {"name": "PPM Document Requirements", "items": [
                "☐ Include risk factors disclosure",
                "☐ Include property description and financials",
                "☐ Include sponsor background and track record",
                "☐ Include subscription procedures",
            ]},
            {"name": "Subscription Agreement Validation", "items": [
                "☐ Verify investor signatures",
                "☐ Confirm investment amount",
                "☐ Validate suitability questionnaire",
                "☐ Check for required representations and warranties",
            ]},
        ],
    }

    api_post("/file-send", {
        "sender_id": "doc-smith",
        "recipient_id": "verify-vex",
        "message_type": "file_send",
        "payload": {
            "filename": "compliance_checklist_draft.md",
            "content": json.dumps(checklist_draft, indent=2),
            "sections_count": len(checklist_draft["sections"]),
            "items_count": sum(len(s["items"]) for s in checklist_draft["sections"]),
        },
    })
    print_agent_action("doc-smith", f"sent draft checklist to verify-vex", f"{len(checklist_draft['sections'])} sections, {sum(len(s['items']) for s in checklist_draft['sections'])} checklist items")

    # ─── Step 8: Cross-verification ────────────────────────────
    print_header("CROSS-VERIFICATION")
    print("  verify-vex reviews the checklist against requirements...")

    time.sleep(0.5)
    # verify-vex sends verification results
    verification = {
        "status": "approved_with_notes",
        "findings": [
            {"item": "Form D filing deadline", "verdict": "✅ Correct — 15 days"},
            {"item": "Non-accredited investor limit", "verdict": "✅ Correct — 35 max"},
            {"item": "Blue sky law states", "verdict": "⚠️ Note: add NV and AZ (common for RE syndicators)"},
            {"item": "PPM requirements", "verdict": "✅ Complete"},
            {"item": "Subscription agreement", "verdict": "⚠️ Note: add ERISA compliance check"},
        ],
        "score": "4/5 items fully verified, 2 improvement notes",
    }

    api_post("/peer-message", {
        "sender_id": "verify-vex",
        "recipient_id": "doc-smith",
        "message_type": "peer_response",
        "payload": verification,
    })
    print_agent_action("verify-vex", "completed verification", f"Verdict: {verification['status']}\n{verification['score']}")

    time.sleep(0.3)
    for finding in verification["findings"]:
        icon = "✅" if "✅" in finding["verdict"] else "⚠️"
        print(f"     {icon} {finding['item']}: {finding['verdict']}")

    # ─── Step 9: Consensus vote ────────────────────────────────
    print_header("CONSENSUS VOTE")
    print("  All agents vote to approve the final checklist...")

    votes = {
        "legal-eagle": {"vote": "approve", "confidence": 0.92, "comment": "Accurate legal requirements, minor state additions noted"},
        "doc-smith": {"vote": "approve", "confidence": 0.95, "comment": "All sections complete, improvements will be incorporated"},
        "verify-vex": {"vote": "approve", "confidence": 0.88, "comment": "Verified against regulations, 2 improvement notes to address"},
    }

    for aid, vote in votes.items():
        api_post("/consensus-vote", {
            "sender_id": aid,
            "message_type": "consensus_vote",
            "payload": vote,
        })
        print_agent_action(aid, f"voted: {vote['vote']} (confidence: {vote['confidence']:.0%})", vote["comment"])
        time.sleep(0.3)

    print(f"\n  ✓ Consensus reached: 3/3 approve — checklist finalized")

    # ─── Step 10: Final output ─────────────────────────────────
    print_header("FINAL DELIVERABLE")

    final_checklist = checklist_draft.copy()
    # Add the improvement notes
    final_checklist["sections"][2]["items"].append("☐ File notice in NV and AZ if applicable")
    final_checklist["sections"][4]["items"].append("☐ Verify ERISA compliance for pension fund investors")
    final_checklist["verified"] = True
    final_checklist["verified_by"] = ["legal-eagle", "verify-vex"]
    final_checklist["consensus"] = "approved (3/3)"
    final_checklist["generated_at"] = datetime.now(timezone.utc).isoformat()

    total_items = sum(len(s["items"]) for s in final_checklist["sections"])
    print(f"  📋 {final_checklist['title']}")
    print(f"  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    for section in final_checklist["sections"]:
        print(f"\n  ▸ {section['name']}")
        for item in section["items"]:
            print(f"    {item}")
    print(f"\n  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"  ✓ {total_items} checklist items across {len(final_checklist['sections'])} sections")
    print(f"  ✓ Verified by: {', '.join(final_checklist['verified_by'])}")
    print(f"  ✓ Consensus: {final_checklist['consensus']}")

    # ─── Summary ───────────────────────────────────────────────
    print_header("SESSION SUMMARY")

    # Get conversation log
    conv_log = api_get("/conversation-log")

    print(f"  ⏱  Duration: ~30 seconds")
    print(f"  🤖 Agents: 3 registered, 3 online")
    print(f"  📨 Messages exchanged: {len(conv_log) if isinstance(conv_log, list) else 'N/A'}")
    print(f"  📋 Tasks completed: 1")
    print(f"  📎 Files transferred: 1")
    print(f"  ✅ Verification: passed")
    print(f"  🗳  Consensus: approved (3/3)")
    print()
    print("  This demo used a mock coordinator with pre-scripted agent responses.")
    print("  The real cluster runs on TCP/WebSocket with live agent processes.")
    print("  Try: python -m dashboard.cli serve --port 8080 --shared-dir /tmp/cluster")
    print()

    # ─── Cleanup ───────────────────────────────────────────────
    coord.stop()

    return final_checklist


# ─── Entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        run_demo()
    except KeyboardInterrupt:
        print("\n\n  ⏹  Demo interrupted. Cleaning up...")
    except Exception as e:
        print(f"\n  ❌ Demo error: {e}")
        import traceback
        traceback.print_exc()
