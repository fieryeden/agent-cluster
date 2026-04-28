#!/usr/bin/env python3
"""
Goal Orchestrator

End-to-end orchestration: takes a high-level goal, decomposes it
into subtasks, dispatches to the coordinator, monitors execution,
and aggregates results into a final deliverable.
"""

import json
import time
import uuid
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path

from orchestration.decomposer import (
    TaskDecomposer, Goal, Subtask, SubtaskStatus,
)


class GoalOrchestrator:
    """
    Full pipeline orchestrator.

    Flow:
    1. Accept high-level goal (natural language)
    2. Decompose into dependency-ordered subtasks
    3. Dispatch ready subtasks to coordinator
    4. Poll for completion
    5. Feed upstream results to downstream subtasks
    6. Aggregate all results into final deliverable
    """

    def __init__(
        self,
        coordinator_url: str = "http://localhost:8080",
        poll_interval: float = 3.0,
        timeout: float = 300.0,
        ai_config: Dict[str, Any] = None,
    ):
        self.coordinator_url = coordinator_url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout
        # Decomposer gets orchestrator's own AI config (separate from workers)
        self.decomposer = TaskDecomposer(ai_config=ai_config)
        self.session = requests.Session()

    # --- Public API ---

    def execute(self, description: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute a high-level goal end-to-end.

        Args:
            description: Natural language goal (e.g., "Research transformer architecture")
            params: Optional parameters (query, url, data, text)

        Returns:
            Final deliverable with all subtask results aggregated
        """
        print(f"\n{'='*60}")
        print(f"GOAL: {description}")
        print(f"{'='*60}")

        # 1. Decompose
        goal = self.decomposer.decompose(description, params)
        print(f"\n📋 Decomposed into {len(goal.subtasks)} subtasks:")
        for st in goal.subtasks:
            dep_str = f" (after {st.dependencies})" if st.dependencies else ""
            print(f"   [{st.capability:8s}] {st.action:15s} → {st.description[:50]}{dep_str}")

        # 2. Execute subtasks respecting dependencies
        goal.status = "running"
        start_time = time.time()

        try:
            self._run_subtasks(goal, start_time)
        except TimeoutError:
            goal.status = "failed"
            goal.result = {"error": f"Goal timed out after {self.timeout}s"}
        except Exception as e:
            goal.status = "failed"
            goal.result = {"error": str(e)}

        # 3. Aggregate results
        if goal.status != "failed":
            goal.result = self._aggregate_results(goal)
            completed = sum(1 for s in goal.subtasks if s.status == SubtaskStatus.COMPLETED)
            total = len(goal.subtasks)
            goal.status = "completed" if completed == total else "partial"

        goal.completed_at = datetime.now(timezone.utc).isoformat()

        # Print summary
        print(f"\n{'='*60}")
        print(f"RESULT: {goal.status.upper()}")
        print(f"Subtasks: {sum(1 for s in goal.subtasks if s.status == SubtaskStatus.COMPLETED)}/{len(goal.subtasks)} completed")
        elapsed = time.time() - start_time
        print(f"Time: {elapsed:.1f}s")
        if goal.result:
            summary = goal.result.get("summary", goal.result.get("error", ""))
            if summary:
                print(f"Summary: {str(summary)[:200]}")
        print(f"{'='*60}\n")

        return goal.to_dict()

    def execute_async(self, description: str, params: Dict[str, Any] = None) -> str:
        """
        Start goal execution in background. Returns goal_id for polling.

        Args:
            description: Natural language goal
            params: Optional parameters

        Returns:
            goal_id for status polling
        """
        import threading

        goal = self.decomposer.depose(description, params)
        goal_id = goal.goal_id

        def _run():
            goal.status = "running"
            start_time = time.time()
            try:
                self._run_subtasks(goal, start_time)
                goal.result = self._aggregate_results(goal)
                completed = sum(1 for s in goal.subtasks if s.status == SubtaskStatus.COMPLETED)
                goal.status = "completed" if completed == len(goal.subtasks) else "partial"
            except Exception as e:
                goal.status = "failed"
                goal.result = {"error": str(e)}
            goal.completed_at = datetime.now(timezone.utc).isoformat()

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        return goal_id

    def get_goal_status(self, goal_id: str) -> Optional[Dict]:
        """Get current status of a goal."""
        return self.decomposer.goal_status(goal_id)

    # --- Internal ---

    def _run_subtasks(self, goal: Goal, start_time: float):
        """Run all subtasks respecting dependencies and timeout."""
        dispatched: Dict[str, str] = {}  # subtask_id -> coordinator task_id
        completed_ids: set = set()

        while True:
            # Check timeout
            if time.time() - start_time > self.timeout:
                raise TimeoutError(f"Timeout after {self.timeout}s")

            # Check if all done
            all_done = all(
                s.status in (SubtaskStatus.COMPLETED, SubtaskStatus.FAILED, SubtaskStatus.SKIPPED)
                for s in goal.subtasks
            )
            if all_done:
                break

            # Dispatch ready subtasks
            ready = self.decomposer.get_ready_subtasks(goal.goal_id)
            for subtask in ready:
                if subtask.subtask_id in dispatched:
                    continue
                task_id = self._dispatch_subtask(subtask)
                if task_id:
                    dispatched[subtask.subtask_id] = task_id
                    subtask.task_id = task_id
                    subtask.status = SubtaskStatus.RUNNING
                    print(f"   ▶ Dispatched {subtask.subtask_id} [{subtask.capability}] → {task_id}")

            # Poll completed tasks from coordinator
            self._poll_results(goal, dispatched, completed_ids)

            if not ready and not all_done:
                time.sleep(self.poll_interval)

    def _dispatch_subtask(self, subtask: Subtask) -> Optional[str]:
        """Dispatch a subtask to the coordinator."""
        try:
            resp = self.session.post(
                f"{self.coordinator_url}/assign",
                json={
                    "capability": subtask.capability,
                    "task_data": subtask.params,
                    "description": subtask.description,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("task_id")
        except Exception as e:
            print(f"   ✗ Failed to dispatch {subtask.subtask_id}: {e}")
            self.decomposer.fail_subtask(subtask.subtask_id, str(e))
            return None

    def _poll_results(self, goal: Goal, dispatched: Dict[str, str], completed_ids: set):
        """Poll coordinator for completed tasks and update subtasks."""
        try:
            resp = self.session.get(
                f"{self.coordinator_url}/tasks/completed",
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return

        for task in data.get("tasks", []):
            coord_task_id = task.get("task_id")
            if not coord_task_id:
                continue

            # Find which subtask this belongs to
            subtask_id = None
            for sid, tid in dispatched.items():
                if tid == coord_task_id:
                    subtask_id = sid
                    break

            if not subtask_id or subtask_id in completed_ids:
                continue

            result = task.get("result", {})
            status = result.get("status", "")

            if status == "completed":
                self.decomposer.update_subtask_result(subtask_id, result)
                completed_ids.add(subtask_id)
                subtask_obj = self._find_subtask(goal, subtask_id)
                if subtask_obj:
                    subtask_obj.assigned_to = task.get("completed_by", "unknown")
                    print(f"   ✓ Completed {subtask_id} [{subtask_obj.capability}] by {subtask_obj.assigned_to}")
            elif status == "failed":
                error = result.get("output", {}).get("error", "Unknown error")
                self.decomposer.fail_subtask(subtask_id, error)
                completed_ids.add(subtask_id)
                print(f"   ✗ Failed {subtask_id}: {error}")

    def _find_subtask(self, goal: Goal, subtask_id: str) -> Optional[Subtask]:
        """Find a subtask by ID."""
        for s in goal.subtasks:
            if s.subtask_id == subtask_id:
                return s
        return None

    def _aggregate_results(self, goal: Goal) -> Dict[str, Any]:
        """
        Aggregate all subtask results into a final deliverable.
        """
        completed = [s for s in goal.subtasks if s.status == SubtaskStatus.COMPLETED]
        failed = [s for s in goal.subtasks if s.status == SubtaskStatus.FAILED]

        # Collect outputs by capability
        by_capability: Dict[str, List[Dict]] = {}
        for subtask in completed:
            cap = subtask.capability
            if cap not in by_capability:
                by_capability[cap] = []
            by_capability[cap].append({
                "subtask_id": subtask.subtask_id,
                "action": subtask.action,
                "description": subtask.description,
                "result": subtask.result,
            })

        # Build final summary
        summary_parts = []
        for cap, results in by_capability.items():
            for r in results:
                output = r["result"]
                if isinstance(output, dict):
                    out = output.get("output", output)
                    if isinstance(out, dict):
                        # Extract the most useful text from each capability
                        if cap == "research":
                            findings = out.get("findings", [])
                            if findings:
                                for f in findings[:3]:
                                    title = f.get("title", "")
                                    snippet = f.get("snippet", "")
                                    summary_parts.append(f"🔍 {title}: {snippet}")
                            research_summary = out.get("summary", "")
                            if research_summary:
                                summary_parts.append(f"📝 Research Summary: {research_summary}")

                        elif cap == "web":
                            content = out.get("content", "")
                            title = out.get("title", "")
                            if title:
                                summary_parts.append(f"🌐 {title}")
                            if content:
                                summary_parts.append(content[:500])

                        elif cap == "data":
                            analysis = out.get("analysis", {})
                            summary_stats = analysis.get("summary", {})
                            if summary_stats:
                                summary_parts.append(f"📊 Data Analysis: {json.dumps(summary_stats, default=str)[:300]}")

                        elif cap == "ai":
                            content = out.get("content", "")
                            if content:
                                summary_parts.append(f"🤖 AI Analysis: {content}")

                        elif cap == "legal":
                            clauses = out.get("clauses", [])
                            clause_types = out.get("clause_types", [])
                            if clause_types:
                                summary_parts.append(f"⚖️ Legal Clauses Found: {', '.join(clause_types)}")
                            risk = out.get("risk_level", "")
                            if risk:
                                summary_parts.append(f"   Risk Level: {risk}")

                        elif cap == "file":
                            content = out.get("content", "")
                            if content:
                                summary_parts.append(f"📁 File Content: {str(content)[:300]}")

                        else:
                            summary_parts.append(f"[{cap}] {json.dumps(out, default=str)[:200]}")
                    else:
                        summary_parts.append(f"[{cap}] {str(out)[:200]}")

        full_summary = "\n\n".join(summary_parts) if summary_parts else "No results collected."

        return {
            "goal_id": goal.goal_id,
            "description": goal.description,
            "status": goal.status,
            "subtask_count": len(goal.subtasks),
            "completed_count": len(completed),
            "failed_count": len(failed),
            "summary": full_summary,
            "by_capability": by_capability,
            "failed_subtasks": [
                {"subtask_id": s.subtask_id, "capability": s.capability, "error": str(s.result)}
                for s in failed
            ],
        }


# --- CLI ---

def main():
    """CLI for the Goal Orchestrator."""
    import argparse

    parser = argparse.ArgumentParser(description="Agent Cluster Goal Orchestrator")
    parser.add_argument("--coordinator", default="http://localhost:8080",
                        help="Coordinator URL (default: http://localhost:8080)")
    parser.add_argument("--poll-interval", type=float, default=3.0,
                        help="Poll interval in seconds (default: 3)")
    parser.add_argument("--timeout", type=float, default=300.0,
                        help="Overall timeout in seconds (default: 300)")
    parser.add_argument("--params", type=str, default=None,
                        help="JSON params to pass (e.g. '{\"query\": \"...\"}')")
    parser.add_argument("goal", nargs="?", help="Goal description (or omit for interactive)")

    args = parser.parse_args()

    params = {}
    if args.params:
        try:
            params = json.loads(args.params)
        except json.JSONDecodeError as e:
            print(f"Error parsing --params: {e}")
            return

    orch = GoalOrchestrator(
        coordinator_url=args.coordinator,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )

    if args.goal:
        result = orch.execute(args.goal, params)
        print(json.dumps(result, indent=2, default=str))
    else:
        # Interactive mode
        print("=== Agent Cluster Goal Orchestrator ===")
        print(f"Coordinator: {args.coordinator}")
        print("Type a goal and press Enter. 'quit' to exit.\n")

        while True:
            try:
                goal = input("🎯 Goal> ").strip()
                if goal.lower() in ("quit", "exit", "q"):
                    break
                if not goal:
                    continue
                result = orch.execute(goal, params)
                print(f"\n✅ Goal {result['goal_id']}: {result['status']}")
                if result.get("summary"):
                    print(f"\n{result['summary'][:500]}")
                print()
            except KeyboardInterrupt:
                break
            except EOFError:
                break


if __name__ == "__main__":
    main()
