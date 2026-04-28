#!/usr/bin/env python3
"""
Task Decomposer

Breaks high-level goals into dependency-ordered subtasks
that map to the cluster's available capabilities.
"""

import json
import os
import re
import uuid
import yaml
import requests as req_lib
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path


class SubtaskStatus(Enum):
    PENDING = "pending"
    READY = "ready"       # all deps satisfied
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Subtask:
    """A single decomposed subtask."""
    subtask_id: str
    goal_id: str
    capability: str          # research, web, data, ai, legal, file
    action: str              # search, fetch, analyze_csv, summarize, etc.
    description: str
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)  # subtask_ids
    priority: int = 0
    status: SubtaskStatus = SubtaskStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    assigned_to: Optional[str] = None
    task_id: Optional[str] = None  # coordinator task_id once assigned
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None

    def is_ready(self, completed_ids: set) -> bool:
        """Check if all dependencies are satisfied."""
        return all(dep in completed_ids for dep in self.dependencies)

    def to_dict(self) -> Dict:
        return {
            "subtask_id": self.subtask_id,
            "goal_id": self.goal_id,
            "capability": self.capability,
            "action": self.action,
            "description": self.description,
            "params": self.params,
            "dependencies": self.dependencies,
            "priority": self.priority,
            "status": self.status.value,
            "result": self.result,
            "assigned_to": self.assigned_to,
            "task_id": self.task_id,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }


@dataclass
class Goal:
    """A high-level goal to be decomposed and executed."""
    goal_id: str
    description: str
    subtasks: List[Subtask] = field(default_factory=list)
    status: str = "pending"  # pending, running, completed, failed
    result: Optional[Dict[str, Any]] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "goal_id": self.goal_id,
            "description": self.description,
            "status": self.status,
            "subtask_count": len(self.subtasks),
            "completed_subtasks": sum(1 for s in self.subtasks if s.status == SubtaskStatus.COMPLETED),
            "failed_subtasks": sum(1 for s in self.subtasks if s.status == SubtaskStatus.FAILED),
            "result": self.result,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }


# --- Decomposition Templates ---
# Maps goal patterns to subtask sequences

RESEARCH_REPORT_TEMPLATE = [
    {"capability": "research", "action": "search", "params_key": "query",
     "description": "Search for information on the topic"},
    {"capability": "web", "action": "fetch", "params_key": "url",
     "description": "Fetch top search results for details",
     "depends_on": 0},  # index of previous subtask
    {"capability": "ai", "action": "summarize", "params_key": "text",
     "description": "Synthesize findings into a coherent summary",
     "depends_on": 1},
]

DATA_ANALYSIS_TEMPLATE = [
    {"capability": "data", "action": "analyze_csv", "params_key": "data",
     "description": "Analyze the dataset"},
    {"capability": "data", "action": "generate_chart", "params_key": "data",
     "description": "Generate visualization charts",
     "depends_on": 0},
    {"capability": "ai", "action": "summarize", "params_key": "text",
     "description": "Write analysis narrative",
     "depends_on": 0},
]

DUE_DILIGENCE_TEMPLATE = [
    {"capability": "research", "action": "search", "params_key": "query",
     "description": "Research the company/topic"},
    {"capability": "web", "action": "fetch", "params_key": "url",
     "description": "Fetch key documents",
     "depends_on": 0},
    {"capability": "data", "action": "analyze_csv", "params_key": "data",
     "description": "Analyze financial/operational data"},
    {"capability": "legal", "action": "extract_clauses", "params_key": "text",
     "description": "Review legal documents for key clauses"},
    {"capability": "ai", "action": "summarize", "params_key": "text",
     "description": "Synthesize due diligence report",
     "depends_on": [0, 1, 2, 3]},
]

COMPETITIVE_ANALYSIS_TEMPLATE = [
    {"capability": "research", "action": "search", "params_key": "query",
     "description": "Search for competitor information"},
    {"capability": "web", "action": "fetch", "params_key": "url",
     "description": "Fetch competitor websites and reports",
     "depends_on": 0},
    {"capability": "data", "action": "analyze_csv", "params_key": "data",
     "description": "Analyze competitive metrics"},
    {"capability": "ai", "action": "summarize", "params_key": "text",
     "description": "Generate competitive analysis report",
     "depends_on": [0, 1, 2]},
]


class TaskDecomposer:
    """
    Decomposes high-level goals into dependency-ordered subtasks.

    Strategies:
    1. Template matching — recognizes common goal patterns
    2. LLM-powered decomposition — uses orchestrator's own AI for complex goals
    3. Capability inference — maps goal descriptions to required capabilities
    4. Dependency resolution — orders subtasks respecting data flow
    """

    # Goal pattern -> template mapping
    PATTERNS = [
        (r"(research|investigate|study|explore|find out|look into)\s+", RESEARCH_REPORT_TEMPLATE),
        (r"(analyze|analysis|examine|review)\s+.*(data|dataset|csv|numbers|metrics)", DATA_ANALYSIS_TEMPLATE),
        (r"(due.?diligence|legal.?review|contract.?review|compliance)", DUE_DILIGENCE_TEMPLATE),
        (r"(competitive|competitor|market.?analysis|landscape)", COMPETITIVE_ANALYSIS_TEMPLATE),
        (r"(report|white.?paper|brief|overview)\s+", RESEARCH_REPORT_TEMPLATE),
        (r"(summarize|digest|synopsize|condense)\s+", [
            {"capability": "ai", "action": "summarize", "params_key": "text",
             "description": "Summarize the provided text"},
        ]),
        (r"(fetch|scrape|download|get)\s+", [
            {"capability": "web", "action": "fetch", "params_key": "url",
             "description": "Fetch content from URL"},
        ]),
        (r"(legal|contract|agreement|clause|compliance)", [
            {"capability": "legal", "action": "extract_clauses", "params_key": "text",
             "description": "Extract legal clauses"},
            {"capability": "legal", "action": "compliance_check", "params_key": "text",
             "description": "Run compliance checks",
             "depends_on": 0},
        ]),
    ]

    def __init__(self, ai_config: Dict[str, Any] = None):
        self.goals: Dict[str, Goal] = {}
        # Orchestrator's OWN AI config — never shared with workers
        self.ai_config = ai_config if ai_config is not None else self._load_orchestrator_config()

    @staticmethod
    def _load_orchestrator_config() -> Dict[str, Any]:
        """Load orchestration layer's own AI config (separate from workers)."""
        # Try orchestration/config.yaml first, then fall back
        config_paths = [
            Path(__file__).parent / "config.yaml",
            Path("orchestration/config.yaml"),
            Path("config.yaml"),
        ]
        for cp in config_paths:
            if cp.exists():
                with open(cp) as f:
                    data = yaml.safe_load(f) or {}
                ai = data.get("ai", {})
                if ai.get("api_key"):
                    return ai
        # Auto-discover from OpenClaw config
        try:
            oc_path = Path(os.path.expanduser("~/.openclaw/openclaw.json"))
            if oc_path.exists():
                with open(oc_path) as f:
                    oc = json.load(f)
                providers = oc.get("providers", {})
                for pid, pdata in providers.items():
                    if "nvidia" in pid.lower():
                        auth = pdata.get("auth", {})
                        key = auth.get("apiKey", "")
                        if key:
                            return {
                                "api_key": key,
                                "base_url": pdata.get("baseURL", "https://integrate.api.nvidia.com/v1"),
                                "model": pdata.get("modelId", "z-ai/glm5"),
                            }
        except Exception:
            pass
        return {}

    def _llm_decompose(self, description: str, params: Dict[str, Any] = None) -> Optional[List[Dict]]:
        """Use the orchestrator's own AI to decompose a complex goal into subtasks.

        Returns a list of subtask dicts, or None if LLM is unavailable.
        The AI config here is the ORCHESTRATOR's — workers use their own.
        """
        if not self.ai_config.get("api_key"):
            return None

        available_caps = ["research", "web", "data", "ai", "legal", "file"]
        prompt = f"""Break down this goal into 2-5 subtasks using these capabilities: {', '.join(available_caps)}.

Goal: {description}
{f'Parameters: {json.dumps(params)}' if params else ''}

Respond with ONLY a JSON array. Each element must have:
- "capability": one of {available_caps}
- "action": specific action (e.g., "search", "analyze_csv", "summarize", "fetch", "extract_clauses")
- "description": what this subtask does
- "depends_on": list of 0-based indices of subtasks it depends on (empty [] if none)

Example: [{{"capability": "research", "action": "search", "description": "Research the topic", "depends_on": []}}]

JSON array only, no other text:"""

        try:
            resp = req_lib.post(
                f"{self.ai_config.get('base_url', 'https://integrate.api.nvidia.com/v1')}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.ai_config['api_key']}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.ai_config.get("model", "z-ai/glm5"),
                    "messages": [
                        {"role": "system", "content": "You are a task decomposition expert. Output only valid JSON arrays."},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 1000,
                    "temperature": 0.3,
                },
                timeout=90,
            )
            if resp.status_code == 429:
                return None  # Rate limited, fall back to template
            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"].strip()
            # Extract JSON from response (may have markdown fences)
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            subtasks_raw = json.loads(text)
            if isinstance(subtasks_raw, list) and len(subtasks_raw) > 0:
                return subtasks_raw
        except Exception as e:
            print(f"[DECOMPOSER] LLM decomp failed: {e}")
        return None


    def decompose(self, description: str, params: Dict[str, Any] = None) -> Goal:
        """
        Decompose a high-level goal into subtasks.

        Args:
            description: Natural language goal description
            params: Optional parameters (query, url, data, text, etc.)

        Returns:
            Goal with subtasks ready for execution
        """
        params = params or {}
        goal_id = f"goal-{uuid.uuid4().hex[:8]}"
        goal = Goal(goal_id=goal_id, description=description)

        # Try LLM decomposition first (uses orchestrator's own AI, not workers')
        llm_result = self._llm_decompose(description, params)
        if llm_result:
            subtasks = self._build_from_llm(goal_id, llm_result, description, params)
        else:
            # Fall back to template matching
            template = self._match_template(description)
            if template:
                subtasks = self._build_from_template(goal_id, template, description, params)
            else:
                subtasks = self._build_from_inference(goal_id, description, params)

        goal.subtasks = subtasks
        self.goals[goal_id] = goal
        return goal

    def _build_from_llm(
        self, goal_id: str, llm_subtasks: List[Dict], description: str, params: Dict[str, Any],
    ) -> List[Subtask]:
        """Build subtasks from LLM-generated decomposition."""
        subtasks = []
        for i, step in enumerate(llm_subtasks):
            subtask_id = f"st-{goal_id}-{i:02d}"
            deps = []
            for dep_idx in step.get("depends_on", []):
                if isinstance(dep_idx, int) and dep_idx < i:
                    dep_id = f"st-{goal_id}-{dep_idx:02d}"
                    deps.append(dep_id)
            step_params = {k: v for k, v in params.items()}  # copy base params
            step_params["action"] = step.get("action", "process")
            # Don't pass any API keys — workers use their own config
            subtasks.append(Subtask(
                subtask_id=subtask_id,
                goal_id=goal_id,
                capability=step.get("capability", "research"),
                action=step.get("action", "process"),
                description=step.get("description", f"Step {i+1}: {description}"),
                params=step_params,
                dependencies=deps,
                priority=len(llm_subtasks) - i,
            ))
        return subtasks

    def _match_template(self, description: str) -> Optional[List[Dict]]:
        """Match description to a decomposition template."""
        desc_lower = description.lower()
        for pattern, template in self.PATTERNS:
            if re.search(pattern, desc_lower):
                return template
        return None

    def _build_from_template(
        self,
        goal_id: str,
        template: List[Dict],
        description: str,
        params: Dict[str, Any],
    ) -> List[Subtask]:
        """Build subtasks from a matched template."""
        subtasks = []
        id_map = {}  # template index -> subtask_id

        for i, step in enumerate(template):
            subtask_id = f"st-{goal_id}-{i:02d}"

            # Resolve dependencies
            deps = []
            dep_spec = step.get("depends_on")
            if dep_spec is not None:
                if isinstance(dep_spec, int):
                    if dep_spec in id_map:
                        deps.append(id_map[dep_spec])
                elif isinstance(dep_spec, list):
                    for d in dep_spec:
                        if d in id_map:
                            deps.append(id_map[d])

            # Build params — use goal params or infer from description
            step_params = dict(params)
            if step.get("params_key") == "query" and "query" not in step_params:
                step_params["query"] = description
                step_params["action"] = step["action"]
            elif step.get("params_key") == "text" and "text" not in step_params:
                # Will be filled from dependency results at runtime
                step_params["action"] = step["action"]
                step_params["_needs_upstream"] = True
            elif step.get("params_key") == "url" and "url" not in step_params:
                step_params["action"] = step["action"]
                step_params["_needs_upstream"] = True
            elif step.get("params_key") == "data" and "data" not in step_params:
                step_params["action"] = step["action"]
                step_params["_needs_upstream"] = True
            else:
                step_params["action"] = step["action"]

            subtask = Subtask(
                subtask_id=subtask_id,
                goal_id=goal_id,
                capability=step["capability"],
                action=step["action"],
                description=step["description"],
                params=step_params,
                dependencies=deps,
                priority=len(template) - i,  # earlier = higher priority
            )
            subtasks.append(subtask)
            id_map[i] = subtask_id

        return subtasks

    def _build_from_inference(
        self,
        goal_id: str,
        description: str,
        params: Dict[str, Any],
    ) -> List[Subtask]:
        """Infer subtasks from description when no template matches."""
        subtasks = []
        desc_lower = description.lower()

        # Default: research → synthesize pipeline
        needs_research = any(w in desc_lower for w in [
            "find", "search", "lookup", "what is", "who", "how does", "explain",
            "tell me", "investigate", "learn about", "compare",
        ])
        needs_data = any(w in desc_lower for w in [
            "data", "csv", "numbers", "statistics", "metrics", "analyze", "chart", "graph",
        ])
        needs_legal = any(w in desc_lower for w in [
            "legal", "contract", "agreement", "clause", "compliance", "regulation",
        ])
        needs_web = any(w in desc_lower for w in [
            "website", "url", "page", "scrape", "fetch", "download",
        ])
        needs_file = any(w in desc_lower for w in [
            "file", "document", "read", "write", "search file", "directory",
        ])

        idx = 0
        id_map = {}
        deps = []

        if needs_research or not (needs_data or needs_legal or needs_web or needs_file):
            st_id = f"st-{goal_id}-{idx:02d}"
            subtasks.append(Subtask(
                subtask_id=st_id, goal_id=goal_id,
                capability="research", action="search",
                description=f"Research: {description}",
                params={"query": description, "action": "search", **params},
                priority=10,
            ))
            id_map["research"] = st_id
            deps.append(st_id)
            idx += 1

        if needs_web:
            st_id = f"st-{goal_id}-{idx:02d}"
            subtasks.append(Subtask(
                subtask_id=st_id, goal_id=goal_id,
                capability="web", action="fetch",
                description=f"Fetch web content for: {description}",
                params={"action": "fetch", "_needs_upstream": True, **params},
                dependencies=list(deps),
                priority=8,
            ))
            id_map["web"] = st_id
            deps.append(st_id)
            idx += 1

        if needs_data:
            st_id = f"st-{goal_id}-{idx:02d}"
            subtasks.append(Subtask(
                subtask_id=st_id, goal_id=goal_id,
                capability="data", action="analyze_csv",
                description=f"Analyze data for: {description}",
                params={"action": "analyze_csv", **params},
                priority=8,
            ))
            id_map["data"] = st_id
            deps.append(st_id)
            idx += 1

        if needs_legal:
            st_id = f"st-{goal_id}-{idx:02d}"
            subtasks.append(Subtask(
                subtask_id=st_id, goal_id=goal_id,
                capability="legal", action="extract_clauses",
                description=f"Legal review for: {description}",
                params={"action": "extract_clauses", **params},
                priority=8,
            ))
            id_map["legal"] = st_id
            deps.append(st_id)
            idx += 1

        if needs_file:
            st_id = f"st-{goal_id}-{idx:02d}"
            subtasks.append(Subtask(
                subtask_id=st_id, goal_id=goal_id,
                capability="file", action="read",
                description=f"File operation for: {description}",
                params={"action": "read", **params},
                priority=8,
            ))
            id_map["file"] = st_id
            deps.append(st_id)
            idx += 1

        # Final synthesis step (depends on all above)
        if len(subtasks) > 1:
            st_id = f"st-{goal_id}-{idx:02d}"
            subtasks.append(Subtask(
                subtask_id=st_id, goal_id=goal_id,
                capability="ai", action="summarize",
                description=f"Synthesize findings for: {description}",
                params={"action": "summarize", "_needs_upstream": True},
                dependencies=deps,
                priority=5,
            ))

        # Fallback: single research task
        if not subtasks:
            st_id = f"st-{goal_id}-00"
            subtasks.append(Subtask(
                subtask_id=st_id, goal_id=goal_id,
                capability="research", action="search",
                description=f"Research: {description}",
                params={"query": description, "action": "search", **params},
                priority=10,
            ))

        return subtasks

    def get_ready_subtasks(self, goal_id: str) -> List[Subtask]:
        """Get subtasks whose dependencies are all completed."""
        goal = self.goals.get(goal_id)
        if not goal:
            return []

        completed_ids = {
            s.subtask_id for s in goal.subtasks
            if s.status == SubtaskStatus.COMPLETED
        }

        ready = []
        for subtask in goal.subtasks:
            if subtask.status == SubtaskStatus.PENDING and subtask.is_ready(completed_ids):
                subtask.status = SubtaskStatus.READY
                ready.append(subtask)

        return ready

    def update_subtask_result(self, subtask_id: str, result: Dict[str, Any]):
        """Update a subtask with its result and feed downstream params."""
        for goal in self.goals.values():
            for subtask in goal.subtasks:
                if subtask.subtask_id == subtask_id:
                    subtask.status = SubtaskStatus.COMPLETED
                    subtask.result = result
                    subtask.completed_at = datetime.now(timezone.utc).isoformat()

                    # Feed results to dependent subtasks
                    self._propagate_results(goal, subtask)
                    return True
        return False

    def fail_subtask(self, subtask_id: str, error: str):
        """Mark a subtask as failed."""
        for goal in self.goals.values():
            for subtask in goal.subtasks:
                if subtask.subtask_id == subtask_id:
                    subtask.status = SubtaskStatus.FAILED
                    subtask.result = {"error": error}
                    subtask.completed_at = datetime.now(timezone.utc).isoformat()
                    return True
        return False

    def _propagate_results(self, goal: Goal, completed: Subtask):
        """Propagate completed subtask results to dependent subtasks."""
        output_text = self._extract_text_result(completed)

        for subtask in goal.subtasks:
            if completed.subtask_id in subtask.dependencies:
                if subtask.params.get("_needs_upstream"):
                    # Feed upstream results into this subtask's params
                    if subtask.capability == "ai" and "text" not in subtask.params:
                        subtask.params["text"] = output_text
                    elif subtask.capability == "web" and "url" not in subtask.params:
                        # Try to extract first URL from research results
                        urls = self._extract_urls(completed)
                        if urls:
                            subtask.params["url"] = urls[0]
                    elif subtask.capability == "data" and "data" not in subtask.params:
                        subtask.params["data"] = output_text

    def _extract_text_result(self, subtask: Subtask) -> str:
        """Extract meaningful text from a subtask result."""
        if not subtask.result:
            return ""
        output = subtask.result.get("output", subtask.result)
        if isinstance(output, str):
            return output
        if isinstance(output, dict):
            # Try common fields
            for key in ["summary", "content", "text", "findings", "result", "analysis"]:
                if key in output:
                    val = output[key]
                    if isinstance(val, str):
                        return val
                    if isinstance(val, list):
                        return "\n".join(str(v) for v in val)
            return json.dumps(output, indent=2, default=str)
        return str(output)

    def _extract_urls(self, subtask: Subtask) -> List[str]:
        """Extract URLs from a subtask result (e.g., search results)."""
        urls = []
        if not subtask.result:
            return urls
        output = subtask.result.get("output", subtask.result)
        if isinstance(output, dict):
            for finding in output.get("findings", output.get("results", [])):
                if isinstance(finding, dict) and finding.get("url"):
                    urls.append(finding["url"])
                elif isinstance(finding, str) and finding.startswith("http"):
                    urls.append(finding)
        return urls

    def goal_status(self, goal_id: str) -> Optional[Dict]:
        """Get detailed goal status."""
        goal = self.goals.get(goal_id)
        if not goal:
            return None
        return goal.to_dict()
