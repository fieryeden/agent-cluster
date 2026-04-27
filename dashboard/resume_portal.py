#!/usr/bin/env python3
"""
Agent Resume Portal

Transforms the cluster into a browsable labor market.
Each agent has a public "resume" showing:
- Specialization and capabilities
- Task completion history and success rate
- Client ratings and reviews
- Compute rate and availability

This enables businesses to browse and "hire" agents based on track record.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))


@dataclass
class AgentReview:
    """A client review for an agent."""
    reviewer_id: str
    rating: int  # 1-5 stars
    comment: str
    task_type: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        return d


@dataclass
class AgentResume:
    """
    Public resume for an agent.
    
    This is what clients see when browsing the labor market.
    """
    agent_id: str
    specialization: str  # e.g., "Transaction categorization"
    version: str = "v1.0"
    
    # Task history
    tasks_completed: int = 0
    tasks_failed: int = 0
    success_rate: float = 0.0
    
    # Client ratings
    reviews: List[AgentReview] = field(default_factory=list)
    avg_rating: float = 0.0
    total_ratings: int = 0
    
    # Capabilities
    capabilities: List[str] = field(default_factory=list)
    learning: List[str] = field(default_factory=list)  # Currently learning
    
    # Availability
    uptime_percentage: float = 99.0
    last_active: Optional[datetime] = None
    
    # Pricing
    compute_rate_per_hour: float = 0.12  # $0.12/hr default
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['last_active'] = self.last_active.isoformat() if self.last_active else None
        d['created_at'] = self.created_at.isoformat()
        d['reviews'] = [r.to_dict() if isinstance(r, AgentReview) else r for r in self.reviews]
        return d
    
    def add_review(self, rating: int, comment: str, reviewer_id: str, task_type: str):
        """Add a new review and recalculate average."""
        review = AgentReview(
            reviewer_id=reviewer_id,
            rating=rating,
            comment=comment,
            task_type=task_type
        )
        self.reviews.append(review)
        self._recalculate_rating()
    
    def _recalculate_rating(self):
        """Recalculate average rating from all reviews."""
        if self.reviews:
            self.total_ratings = len(self.reviews)
            self.avg_rating = sum(r.rating for r in self.reviews) / self.total_ratings
    
    def record_task_completion(self, success: bool, duration_seconds: float = 0):
        """Record a task completion and update success rate."""
        if success:
            self.tasks_completed += 1
        else:
            self.tasks_failed += 1
        
        total = self.tasks_completed + self.tasks_failed
        if total > 0:
            self.success_rate = self.tasks_completed / total
        
        self.last_active = datetime.now()


class AgentResumePortal:
    """
    Portal for browsing and managing agent resumes.
    
    Features:
    - Browse agents by specialization, rating, capability
    - View detailed resume for each agent
    - Add reviews and ratings
    - Track agent performance over time
    - Generate public resume pages (HTML)
    """
    
    # Pre-defined specializations for capability mapping
    SPECIALIZATIONS = {
        'file': 'File Operations Specialist',
        'web': 'Web Scraping & API Worker',
        'data': 'Data Processing Analyst',
        'ai': 'AI/ML Assistant',
        'system': 'System Administrator',
        'communication': 'Communication Coordinator',
        'database': 'Database Manager',
        'cloud': 'Cloud Operations Worker',
        'integration': 'Integration Specialist',
    }
    
    def __init__(self, data_dir: Optional[Path] = None):
        self.resumes: Dict[str, AgentResume] = {}
        self.data_dir = Path(data_dir) if data_dir else Path('/tmp/agent_resumes')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing resumes
        self._load_resumes()
    
    def _load_resumes(self):
        """Load resumes from disk."""
        resume_file = self.data_dir / 'resumes.json'
        if resume_file.exists():
            try:
                data = json.loads(resume_file.read_text())
                for agent_id, resume_data in data.items():
                    resume_data['created_at'] = datetime.fromisoformat(resume_data['created_at'])
                    if resume_data.get('last_active'):
                        resume_data['last_active'] = datetime.fromisoformat(resume_data['last_active'])
                    if 'reviews' in resume_data:
                        resume_data['reviews'] = [
                            AgentReview(
                                reviewer_id=r['reviewer_id'],
                                rating=r['rating'],
                                comment=r['comment'],
                                task_type=r['task_type'],
                                timestamp=datetime.fromisoformat(r['timestamp'])
                            ) for r in resume_data['reviews']
                        ]
                    self.resumes[agent_id] = AgentResume(**resume_data)
            except Exception as e:
                print(f"Warning: Could not load resumes: {e}")
    
    def _save_resumes(self):
        """Save resumes to disk."""
        resume_file = self.data_dir / 'resumes.json'
        data = {agent_id: resume.to_dict() for agent_id, resume in self.resumes.items()}
        # Convert datetime objects for JSON
        for resume_data in data.values():
            resume_data['created_at'] = resume_data['created_at']
            if resume_data.get('last_active'):
                resume_data['last_active'] = resume_data['last_active']
        resume_file.write_text(json.dumps(data, indent=2, default=str))
    
    def get_or_create_resume(self, agent_id: str, capabilities: List[str] = None) -> AgentResume:
        """Get existing resume or create new one for agent."""
        if agent_id not in self.resumes:
            # Determine specialization from capabilities
            specialization = 'General Worker'
            if capabilities:
                for cap in capabilities:
                    if cap in self.SPECIALIZATIONS:
                        specialization = self.SPECIALIZATIONS[cap]
                        break
            
            self.resumes[agent_id] = AgentResume(
                agent_id=agent_id,
                specialization=specialization,
                capabilities=capabilities or [],
            )
            self._save_resumes()
        
        return self.resumes[agent_id]
    
    def browse_agents(
        self,
        specialization: Optional[str] = None,
        min_rating: float = 0.0,
        capability: Optional[str] = None,
        sort_by: str = 'rating',  # rating, tasks, success_rate
        limit: int = 20
    ) -> List[Dict]:
        """
        Browse available agents with filters.
        
        Args:
            specialization: Filter by specialization
            min_rating: Minimum average rating
            capability: Must have this capability
            sort_by: Sort order (rating, tasks, success_rate)
            limit: Max results
        
        Returns:
            List of agent resume summaries
        """
        results = []
        
        for resume in self.resumes.values():
            # Apply filters
            if specialization and resume.specialization != specialization:
                continue
            if min_rating > 0 and resume.avg_rating < min_rating:
                continue
            if capability and capability not in resume.capabilities:
                continue
            
            # Create summary
            summary = {
                'agent_id': resume.agent_id,
                'specialization': resume.specialization,
                'version': resume.version,
                'tasks_completed': resume.tasks_completed,
                'success_rate': resume.success_rate,
                'avg_rating': resume.avg_rating,
                'total_ratings': resume.total_ratings,
                'capabilities': resume.capabilities,
                'uptime': resume.uptime_percentage,
                'compute_rate': resume.compute_rate_per_hour,
                'available': resume.last_active and 
                    (datetime.now() - resume.last_active) < timedelta(minutes=5)
            }
            results.append(summary)
        
        # Sort
        sort_keys = {
            'rating': lambda x: x['avg_rating'],
            'tasks': lambda x: x['tasks_completed'],
            'success_rate': lambda x: x['success_rate'],
        }
        if sort_by in sort_keys:
            results.sort(key=sort_keys[sort_by], reverse=True)
        
        return results[:limit]
    
    def get_resume(self, agent_id: str) -> Optional[Dict]:
        """Get full resume for an agent."""
        if agent_id in self.resumes:
            return self.resumes[agent_id].to_dict()
        return None
    
    def add_review(
        self,
        agent_id: str,
        rating: int,
        comment: str,
        reviewer_id: str,
        task_type: str
    ) -> bool:
        """Add a review for an agent."""
        if agent_id not in self.resumes:
            return False
        
        if not 1 <= rating <= 5:
            raise ValueError("Rating must be 1-5")
        
        self.resumes[agent_id].add_review(rating, comment, reviewer_id, task_type)
        self._save_resumes()
        return True
    
    def record_task(self, agent_id: str, success: bool, duration_seconds: float = 0):
        """Record a task completion for an agent."""
        resume = self.get_or_create_resume(agent_id)
        resume.record_task_completion(success, duration_seconds)
        self._save_resumes()
    
    def update_capabilities(self, agent_id: str, capabilities: List[str]):
        """Update agent capabilities."""
        resume = self.get_or_create_resume(agent_id, capabilities)
        resume.capabilities = capabilities
        
        # Update specialization if needed
        for cap in capabilities:
            if cap in self.SPECIALIZATIONS:
                resume.specialization = self.SPECIALIZATIONS[cap]
                break
        
        self._save_resumes()
    
    def set_learning(self, agent_id: str, learning: List[str]):
        """Set what the agent is currently learning."""
        if agent_id in self.resumes:
            self.resumes[agent_id].learning = learning
            self._save_resumes()
    
    def generate_resume_html(self, agent_id: str) -> str:
        """Generate HTML resume page for an agent."""
        resume = self.resumes.get(agent_id)
        if not resume:
            return "<h1>Agent not found</h1>"
        
        stars = "★" * int(resume.avg_rating) + "☆" * (5 - int(resume.avg_rating))
        
        reviews_html = ""
        for review in resume.reviews[-5:]:  # Last 5 reviews
            review_stars = "★" * review.rating + "☆" * (5 - review.rating)
            reviews_html += f"""
            <div class="review">
                <div class="review-header">
                    <span class="review-stars">{review_stars}</span>
                    <span class="review-task">{review.task_type}</span>
                </div>
                <p class="review-comment">{review.comment}</p>
                <span class="review-date">{review.timestamp.strftime('%Y-%m-%d')}</span>
            </div>
            """
        
        capabilities_html = ", ".join(resume.capabilities) if resume.capabilities else "None"
        learning_html = ", ".join(resume.learning) if resume.learning else "None"
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <title>{resume.agent_id} - Agent Resume</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
        .resume-card {{ border: 1px solid #e0e0e0; border-radius: 12px; padding: 24px; background: #fff; }}
        .agent-header {{ display: flex; justify-content: space-between; align-items: center; }}
        .agent-id {{ font-size: 24px; font-weight: 600; }}
        .agent-version {{ color: #666; font-size: 14px; }}
        .specialization {{ color: #2196F3; font-size: 16px; margin-top: 4px; }}
        .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-top: 24px; }}
        .stat {{ text-align: center; padding: 16px; background: #f5f5f5; border-radius: 8px; }}
        .stat-value {{ font-size: 24px; font-weight: 600; }}
        .stat-label {{ font-size: 12px; color: #666; margin-top: 4px; }}
        .rating-section {{ margin-top: 24px; }}
        .rating-stars {{ font-size: 24px; color: #FFC107; }}
        .rating-count {{ color: #666; font-size: 14px; }}
        .capabilities {{ margin-top: 24px; }}
        .cap-list {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }}
        .cap-tag {{ background: #E3F2FD; color: #1565C0; padding: 6px 12px; border-radius: 16px; font-size: 13px; }}
        .learning {{ background: #FFF3E0; color: #E65100; }}
        .reviews {{ margin-top: 24px; }}
        .review {{ border-top: 1px solid #e0e0e0; padding: 16px 0; }}
        .review-header {{ display: flex; gap: 12px; align-items: center; }}
        .review-stars {{ color: #FFC107; }}
        .review-task {{ background: #f5f5f5; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        .review-comment {{ margin: 8px 0; }}
        .review-date {{ color: #999; font-size: 12px; }}
        .hire-btn {{ background: #4CAF50; color: white; border: none; padding: 12px 32px; font-size: 16px; border-radius: 8px; cursor: pointer; margin-top: 24px; }}
        .hire-btn:hover {{ background: #388E3C; }}
    </style>
</head>
<body>
    <div class="resume-card">
        <div class="agent-header">
            <div>
                <div class="agent-id">{resume.agent_id}</div>
                <div class="agent-version">{resume.version}</div>
                <div class="specialization">{resume.specialization}</div>
            </div>
            <div class="rating-section">
                <span class="rating-stars">{stars}</span>
                <span class="rating-count">({resume.total_ratings} reviews)</span>
            </div>
        </div>
        
        <div class="stats">
            <div class="stat">
                <div class="stat-value">{resume.tasks_completed}</div>
                <div class="stat-label">Tasks Completed</div>
            </div>
            <div class="stat">
                <div class="stat-value">{resume.success_rate:.1%}</div>
                <div class="stat-label">Success Rate</div>
            </div>
            <div class="stat">
                <div class="stat-value">{resume.uptime_percentage:.1f}%</div>
                <div class="stat-label">Uptime</div>
            </div>
            <div class="stat">
                <div class="stat-value">${resume.compute_rate_per_hour:.2f}/hr</div>
                <div class="stat-label">Compute Rate</div>
            </div>
        </div>
        
        <div class="capabilities">
            <h3>Capabilities</h3>
            <div class="cap-list">
                {"".join(f'<span class="cap-tag">{cap}</span>' for cap in resume.capabilities)}
            </div>
        </div>
        
        {f'''<div class="capabilities">
            <h3>Currently Learning</h3>
            <div class="cap-list">
                {"".join(f'<span class="cap-tag learning">{cap}</span>' for cap in resume.learning)}
            </div>
        </div>''' if resume.learning else ''}
        
        <div class="reviews">
            <h3>Recent Reviews</h3>
            {reviews_html if reviews_html else '<p>No reviews yet</p>'}
        </div>
        
        <button class="hire-btn" onclick="hireAgent()">Hire This Agent</button>
    </div>
    
    <script>
        function hireAgent() {{
            alert('Agent hire request sent! The agent will be assigned to your next task.');
        }}
    </script>
</body>
</html>
        """
    
    def generate_marketplace_html(self, title: str = "Agent Labor Market") -> str:
        """Generate HTML page showing all available agents."""
        agents = self.browse_agents(sort_by='rating', limit=50)
        
        agent_cards = ""
        for agent in agents:
            stars = "★" * int(agent['avg_rating']) + "☆" * (5 - int(agent['avg_rating']))
            status = "🟢 Available" if agent['available'] else "🔴 Offline"
            agent_cards += f"""
            <div class="agent-card" onclick="window.location.href='/resume/{agent['agent_id']}'">
                <div class="agent-header">
                    <div>
                        <div class="agent-id">{agent['agent_id']}</div>
                        <div class="specialization">{agent['specialization']}</div>
                    </div>
                    <div class="status">{status}</div>
                </div>
                <div class="agent-stats">
                    <span>✅ {agent['tasks_completed']} tasks</span>
                    <span>📊 {agent['success_rate']:.0%} success</span>
                    <span>{stars} ({agent['total_ratings']})</span>
                </div>
                <div class="agent-rate">${agent['compute_rate']:.2f}/hr</div>
            </div>
            """
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        .filters {{ background: white; padding: 16px; border-radius: 8px; margin-bottom: 20px; display: flex; gap: 16px; }}
        .filter {{ padding: 8px 16px; border: 1px solid #ddd; border-radius: 4px; }}
        .agents {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 16px; }}
        .agent-card {{ background: white; border-radius: 12px; padding: 20px; cursor: pointer; transition: transform 0.2s; }}
        .agent-card:hover {{ transform: translateY(-4px); box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
        .agent-id {{ font-size: 18px; font-weight: 600; }}
        .specialization {{ color: #2196F3; font-size: 14px; margin-top: 4px; }}
        .status {{ font-size: 12px; }}
        .agent-stats {{ margin-top: 12px; display: flex; gap: 12px; font-size: 13px; color: #666; }}
        .agent-rate {{ margin-top: 8px; font-weight: 600; color: #4CAF50; }}
    </style>
</head>
<body>
    <h1>🤖 {title}</h1>
    <p>Browse and hire AI agents based on their track record</p>
    
    <div class="filters">
        <select class="filter" onchange="filterBySpecialization(this.value)">
            <option value="">All Specializations</option>
            <option value="File Operations Specialist">File Operations</option>
            <option value="Web Scraping & API Worker">Web & API</option>
            <option value="Data Processing Analyst">Data Processing</option>
            <option value="AI/ML Assistant">AI/ML</option>
        </select>
        <select class="filter" onchange="filterByRating(this.value)">
            <option value="0">Any Rating</option>
            <option value="4">4+ Stars</option>
            <option value="4.5">4.5+ Stars</option>
        </select>
    </div>
    
    <div class="agents">
        {agent_cards}
    </div>
    
    <script>
        function filterBySpecialization(spec) {{
            // Filter logic would go here
            console.log('Filter by:', spec);
        }}
        function filterByRating(min) {{
            console.log('Min rating:', min);
        }}
    </script>
</body>
</html>
        """


# API Integration for Dashboard
class ResumePortalAPI:
    """REST API endpoints for the resume portal."""
    
    def __init__(self, portal: AgentResumePortal):
        self.portal = portal
    
    def get_routes(self) -> Dict[str, callable]:
        """Return API routes to integrate with dashboard."""
        return {
            '/api/resumes': self._browse_resumes,
            '/api/resumes/:id': self._get_resume,
            '/api/resumes/:id/review': self._add_review,
            '/api/resumes/:id/task': self._record_task,
            '/api/marketplace': self._get_marketplace,
            '/api/marketplace/:id': self._get_resume_html,
        }
    
    def _browse_resumes(self, params: Dict = None) -> Dict:
        """GET /api/resumes - Browse all agents."""
        params = params or {}
        agents = self.portal.browse_agents(
            specialization=params.get('specialization'),
            min_rating=float(params.get('min_rating', 0)),
            capability=params.get('capability'),
            sort_by=params.get('sort_by', 'rating'),
            limit=int(params.get('limit', 50))
        )
        return {'agents': agents, 'total': len(agents)}
    
    def _get_resume(self, agent_id: str, params: Dict = None) -> Dict:
        """GET /api/resumes/:id - Get full resume."""
        resume = self.portal.get_resume(agent_id)
        if resume:
            return resume
        return {'error': 'Agent not found'}, 404
    
    def _add_review(self, agent_id: str, data: Dict) -> Dict:
        """POST /api/resumes/:id/review - Add review."""
        try:
            self.portal.add_review(
                agent_id=agent_id,
                rating=data['rating'],
                comment=data['comment'],
                reviewer_id=data['reviewer_id'],
                task_type=data['task_type']
            )
            return {'success': True}
        except Exception as e:
            return {'error': str(e)}, 400
    
    def _record_task(self, agent_id: str, data: Dict) -> Dict:
        """POST /api/resumes/:id/task - Record task completion."""
        self.portal.record_task(
            agent_id=agent_id,
            success=data.get('success', True),
            duration_seconds=data.get('duration', 0)
        )
        return {'success': True}
    
    def _get_marketplace(self, params: Dict = None) -> str:
        """GET /api/marketplace - Get marketplace HTML."""
        return self.portal.generate_marketplace_html()
    
    def _get_resume_html(self, agent_id: str, params: Dict = None) -> str:
        """GET /api/marketplace/:id - Get resume HTML."""
        return self.portal.generate_resume_html(agent_id)


if __name__ == '__main__':
    # Demo
    portal = AgentResumePortal()
    
    # Create some demo agents
    demo_agents = [
        ('bookkeeper-v2.3', ['data', 'file'], 'Transaction categorization'),
        ('web-scraper-01', ['web', 'data'], 'Web scraping & extraction'),
        ('ai-assistant-04', ['ai', 'communication'], 'AI-powered summarization'),
    ]
    
    for agent_id, caps, spec in demo_agents:
        resume = portal.get_or_create_resume(agent_id, caps)
        resume.specialization = spec
        
        # Add some fake task completions
        for _ in range(10):
            resume.record_task_completion(success=True)
        
        # Add a review
        portal.add_review(agent_id, 5, "Excellent work, very reliable!", "client-001", "data_processing")
    
    # Generate marketplace
    print(portal.generate_marketplace_html())
