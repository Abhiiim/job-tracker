import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from sqlmodel import Field, Relationship, SQLModel, Session, create_engine, select

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "tracker.db"
DATA_DIR.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False},
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Resume(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    filename: str
    file_path: str
    parsed_text: str
    page_count: int = 0
    word_count: int = 0
    uploaded_at: datetime = Field(default_factory=utcnow)
    is_active: bool = True


class Company(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    location: str = "Remote"
    career_url: Optional[str] = None
    portal_type: str = "Custom"
    is_on_dashboard: bool = True
    is_monitored_by_agent: bool = False
    last_scraped_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=utcnow)
    jobs: List["Job"] = Relationship(back_populates="company", cascade_delete=True)


class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    company_id: int = Field(foreign_key="company.id", index=True)
    title: str
    url: str = Field(unique=True)
    job_type: str = "Full-time"
    summary: str = ""
    matching_score: float = 0.0
    matching_feedback: str = ""
    experience_required: str = "0 years"
    experience_numeric: int = 0
    skills_json: str = "[]"
    date_posted: datetime = Field(default_factory=utcnow)
    is_new: bool = True
    notified: bool = False
    company: Company = Relationship(back_populates="jobs")


class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str


class AgentRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    status: str = "completed"
    jobs_scraped: int = 0
    jobs_matched: int = 0
    started_at: datetime = Field(default_factory=utcnow)
    completed_at: Optional[datetime] = None
    log_text: str = ""


def _seed(session: Session) -> None:
    if not session.get(Setting, "default_roles"):
        session.add(Setting(key="default_roles", value=json.dumps([
            "backend engineering", "distributed systems", "AI platform", "FastAPI lead"
        ])))
    if not session.get(Setting, "ai_model"):
        session.add(Setting(key="ai_model", value="gemini/gemini-1.5-flash"))
    if not session.get(Setting, "alert_threshold"):
        session.add(Setting(key="alert_threshold", value="8.0"))

    existing = session.exec(select(Company)).first()
    if existing:
        session.commit()
        return

    company_specs = [
        ("Stripe", "San Francisco / Dublin", "https://stripe.com/jobs/search", "Custom API", True, True),
        ("Anthropic", "San Francisco / Remote US", "https://boards.greenhouse.io/anthropic", "Greenhouse", True, True),
        ("Perplexity AI", "San Francisco, CA", "https://jobs.lever.co/perplexity-ai", "Lever", True, True),
        ("Datadog", "New York / Paris", "https://careers.datadoghq.com", "Greenhouse", True, False),
        ("Scale AI", "San Francisco, CA", "https://boards.greenhouse.io/scaleai", "Greenhouse", True, True),
        ("Brex", "Remote / NYC", "https://www.brex.com/careers", "Lever", False, False),
        ("Coinbase", "Remote", "https://www.coinbase.com/careers", "Greenhouse", True, True),
    ]
    companies: dict[str, Company] = {}
    for spec in company_specs:
        company = Company(
            name=spec[0], location=spec[1], career_url=spec[2], portal_type=spec[3],
            is_on_dashboard=spec[4], is_monitored_by_agent=spec[5],
            last_scraped_at=utcnow() - timedelta(minutes=42),
        )
        session.add(company)
        session.flush()
        companies[company.name] = company

    jobs = [
        ("Stripe", "Senior Backend Engineer (Distributed Systems)", "Remote", 9.4, 6, ["Kafka", "PostgreSQL", "Python", "Distributed Systems"], "Design high-throughput payment event streams using Kafka, PostgreSQL, and Python microservices.", "Direct overlap with backend systems, streaming, and reliability experience."),
        ("Perplexity AI", "Staff AI Infrastructure Engineer", "Hybrid", 9.1, 5, ["LangGraph", "FastAPI", "RAG Pipelines", "Vector DB"], "Build low-latency RAG pipelines and model-routing services using FastAPI and LangGraph.", "Strong alignment with RAG architecture and asynchronous Python expertise."),
        ("Coinbase", "Lead Platform Engineer (Real-time Systems)", "Remote", 8.7, 5, ["WebSockets", "Kafka", "Python"], "Own real-time developer infrastructure and event-driven platform services.", "Excellent platform match with minor gaps in domain-specific crypto experience."),
        ("Datadog", "Software Engineer — Distributed Storage", "Hybrid", 8.3, 4, ["Python", "PostgreSQL", "Observability"], "Improve distributed storage systems used by a global observability platform.", "Solid systems fit; storage engine depth should be highlighted in the application."),
        ("Brex", "Senior Financial Ledger Engineer", "Remote", 7.8, 5, ["Python", "PostgreSQL", "Ledger"], "Build reliable financial ledger services and reconciliation workflows.", "Backend fundamentals match, though the role values deeper financial-domain experience."),
    ]
    for index, item in enumerate(jobs):
        company = companies[item[0]]
        session.add(Job(
            company_id=company.id, title=item[1], url=f"{company.career_url}#matchpulse-{index}",
            job_type=item[2], matching_score=item[3], experience_numeric=item[4],
            experience_required=f"{max(item[4]-2, 1)}–{item[4]} years", skills_json=json.dumps(item[5]),
            summary=item[6], matching_feedback=item[7], date_posted=utcnow() - timedelta(hours=14 + index * 9),
        ))
    session.add(AgentRun(
        status="completed", jobs_scraped=32, jobs_matched=5, completed_at=utcnow() - timedelta(minutes=42),
        log_text="[08:00:01] Scheduled crawl started\n[08:00:15] Stripe: 4 role candidates found\n[08:00:28] Perplexity AI: 2 role candidates found\n[08:00:45] LiteLLM batch evaluation completed\n[08:01:02] 3 high-affinity roles identified\n[08:01:07] Agent sleeping until next cycle",
    ))
    session.commit()


def init_db() -> None:
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        _seed(session)
