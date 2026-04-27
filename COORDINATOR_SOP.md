# Coordinator SOP: How to Split Up Any Research Project for the Agent Cluster

## Overview
This SOP defines how to decompose any research/investor platform project into tasks
that the agent cluster can execute. It's reusable across any domain.

## Phase Structure

Every project follows 5 phases. Never skip a phase.

### Phase 1: RESEARCH (Research/Web capability)
- **Purpose:** Gather raw facts, data, news, regulations
- **Task type:** `research` or `web` capability
- **Best agents:** worker-foxtrot (research:1.0), worker-bravo (research:0.9)
- **Task format:**
  ```json
  {
    "capability": "research",
    "agent_id": "worker-foxtrot",
    "task_data": {
      "query": "specific search query",
      "max_results": 10
    }
  }
  ```
- **Batch size:** 12-16 tasks per project, 2-3 queries per agent
- **Stagger timing:** Don't send all at once (Brave API rate limits). Send in groups of 3-4.
- **Wait condition:** All Phase 1 tasks complete before Phase 2

### Phase 2: ANALYSIS (AI capability)
- **Purpose:** Synthesize research into structured outputs
- **Task type:** `ai` capability with `action: "generate"`
- **Best agents:** eden3 (85%), worker-echo (83%), worker-hotel (75%)
- **Task format:**
  ```json
  {
    "capability": "ai",
    "agent_id": "eden3",
    "task_data": {
      "prompt": "Detailed prompt with research context embedded...",
      "action": "generate"
    }
  }
  ```
- **Critical rules:**
  - Use ONLY `action: "generate"` (NOT `generate_html`, `generate_content`)
  - Keep prompts under 4000 chars (API truncation risk)
  - Include research context IN the prompt (workers can't read files)
  - Specify output format explicitly (markdown, Python, HTML)
  - For code: say "Output raw Python only, no markdown fences, under 100 lines"
- **Batch size:** 6-10 tasks, spread across top agents
- **Wait condition:** All Phase 2 tasks complete before Phase 3

### Phase 3: CROSS-VERIFICATION (AI capability)
- **Purpose:** Workers check OTHER workers' outputs for errors
- **Task type:** `ai` capability with `action: "generate"`
- **Rule:** Verifier agent must be DIFFERENT from the original author
- **Task format:**
  ```json
  {
    "capability": "ai",
    "agent_id": "worker-delta",
    "task_data": {
      "prompt": "Review this content produced by [agent]. Check for: 1) Factual accuracy 2) Completeness 3) Clarity 4) Actionability 5) Red flags. Content: [truncated to 3000 chars]...",
      "action": "generate"
    }
  }
  ```
- **Batch size:** Equal to Phase 2 output count
- **Wait condition:** All Phase 3 tasks complete before Phase 4

### Phase 4: BUILD (AI capability)
- **Purpose:** Generate deliverables (database, API, website, documents)
- **Task type:** `ai` capability with `action: "generate"`
- **Typical deliverables per project:**
  1. Database schema + seed script (Python/SQL)
  2. REST API server (Python/FastAPI)
  3. Frontend website (single HTML file)
  4. Investor playbook (Markdown)
  5. Marketing package (Markdown)
  6. FAQ/knowledge base (Markdown)
  7. Integration test script (Python)
- **Code generation rules:**
  - Say "Output raw [language] only, no markdown fences"
  - Specify line limit (under 100-150 lines)
  - For Python: specify "End with print('OK')" for validation
  - For HTML: specify "Complete single-file HTML with embedded CSS/JS"
- **Batch size:** 6-8 build tasks
- **Wait condition:** All Phase 4 tasks complete before Phase 5

### Phase 5: VALIDATION & ASSEMBLY
- **Purpose:** Review build outputs, fix issues, assemble final package
- **Task type:** `ai` capability + manual verification
- **Steps:**
  1. Run integration tests via cluster
  2. Assign code review tasks to different agents
  3. Strip markdown code fences from code files
  4. Fix syntax errors (re-assign failed builds)
  5. Assemble final deliverable package
- **Re-assignment rule:** If a build task fails, re-assign to the NEXT most reliable agent
  - Priority order: eden3 → worker-echo → worker-hotel → eden4 → worker-alpha

## Agent Reliability Rankings (as of 2026-04-22)

### AI Tasks (generate/summarize/classify)
1. eden3 — 85% success
2. worker-echo — 83% success
3. worker-hotel — 75% success
4. eden4 — 67% success
5. worker-alpha — 62% success
6. worker-charlie — 57% success
7. worker-juliet — 57% success

### Research Tasks (web search)
1. worker-foxtrot — research:1.0
2. worker-bravo — research:0.9
3. worker-hotel — research:0.6 (backup)

### Web/File Tasks
1. worker-delta — web:1.0, file:0.9
2. worker-india — web:1.0, data:0.9
3. worker-golf — data:1.0, file:1.0

## Task Distribution Strategy

### For Research-Heavy Projects (like litigation financing):
- Phase 1: 12-16 research tasks across foxtrot/bravo/hotel
- Phase 2: 8-10 analysis tasks across eden3/echo/hotel
- Phase 3: 8-10 verification tasks (different agents than Phase 2)
- Phase 4: 6-8 build tasks across eden3/echo/hotel
- Phase 5: 3-4 validation tasks

### For Build-Heavy Projects (like platform development):
- Phase 1: 4-6 research tasks (lighter)
- Phase 2: 4-6 analysis tasks
- Phase 3: 4-6 verification tasks
- Phase 4: 10-12 build tasks (heavier)
- Phase 5: 4-6 validation tasks

## Common Pitfalls & Fixes

1. **Brave API rate limiting** → Stagger research tasks, 3-4 at a time
2. **NVIDIA API 502s** → Retry with different agent, wait 30s between submissions
3. **Markdown code fences** → Strip ` ```python ` and ` ``` ` from output
4. **Truncated output** → Keep prompts short, specify line limits
5. **Wrong action type** → Only use `generate`, `summarize`, `classify`
6. **Workers can't read files** → Embed ALL context in the prompt
7. **Relative file paths** → Use absolute paths in generated code

## Pipeline Template

```python
# Standard pipeline structure
PHASES = {
    1: {"name": "Research", "capability": "research", "agents": ["worker-foxtrot", "worker-bravo"]},
    2: {"name": "Analysis", "capability": "ai", "agents": ["eden3", "worker-echo", "worker-hotel"]},
    3: {"name": "Verification", "capability": "ai", "agents": ["worker-delta", "worker-golf", "worker-charlie"]},
    4: {"name": "Build", "capability": "ai", "agents": ["eden3", "worker-echo", "worker-hotel"]},
    5: {"name": "Validation", "capability": "ai", "agents": ["worker-alpha", "eden4", "worker-juliet"]},
}
```

## Monitoring

- Poll `/status` every 5 seconds during active phases
- Check `/tasks/completed` after each phase completes
- Save results to `/tmp/{project_name}/pipeline_results.json`
- Track: task_id, agent, content length, errors
