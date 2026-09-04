from __future__ import annotations

import asyncio
import json
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field as PydanticField, HttpUrl
from pypdf import PdfReader
from sqlmodel import Session, func, select

from .database import AgentRun, Company, Job, Resume, Setting, engine, init_db, utcnow

app = FastAPI(title="MatchPulse API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000", "http://127.0.0.1:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = Path(__file__).resolve().parents[1] / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_UPLOAD_BYTES = 12 * 1024 * 1024


@app.on_event("startup")
def startup() -> None:
    init_db()


def get_db():
    with Session(engine) as session:
        yield session


class CompanyCreate(BaseModel):
    name: str = PydanticField(min_length=2, max_length=120)
    career_url: Optional[HttpUrl] = None
    location: str = "Remote"
    portal_type: str = "Custom"
    is_on_dashboard: bool = True
    is_monitored_by_agent: bool = False


class CompanyUpdate(BaseModel):
    career_url: Optional[HttpUrl] = None
    location: Optional[str] = None
    portal_type: Optional[str] = None
    is_on_dashboard: Optional[bool] = None
    is_monitored_by_agent: Optional[bool] = None


class ModelUpdate(BaseModel):
    model: str


def company_payload(company: Company, db: Session) -> dict:
    count = db.exec(select(func.count(Job.id)).where(Job.company_id == company.id)).one()
    top = db.exec(select(Job).where(Job.company_id == company.id).order_by(Job.matching_score.desc())).first()
    return {
        "id": company.id, "name": company.name, "location": company.location,
        "career_url": company.career_url, "portal_type": company.portal_type,
        "is_on_dashboard": company.is_on_dashboard,
        "is_monitored_by_agent": company.is_monitored_by_agent,
        "last_scraped_at": company.last_scraped_at.isoformat() if company.last_scraped_at else None,
        "job_count": count, "latest_job": top.title if top else None,
        "top_score": top.matching_score if top else None,
    }


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/overview")
def overview(db: Session = Depends(get_db)) -> dict:
    total_jobs = db.exec(select(func.count(Job.id))).one()
    high_matches = db.exec(select(func.count(Job.id)).where(Job.matching_score >= 8)).one()
    monitored = db.exec(select(func.count(Company.id)).where(Company.is_monitored_by_agent == True)).one()  # noqa: E712
    visible = db.exec(select(func.count(Company.id)).where(Company.is_on_dashboard == True)).one()  # noqa: E712
    threshold = float((db.get(Setting, "alert_threshold") or Setting(value="8.0", key="x")).value)
    return {"total_jobs": total_jobs, "high_matches": high_matches, "monitored_companies": monitored, "visible_companies": visible, "alert_threshold": threshold}


@app.get("/api/jobs")
def jobs(
    max_experience: Optional[int] = Query(None, ge=0, le=30), recent_only: bool = False,
    min_matching_score: Optional[float] = Query(None, ge=0, le=10), search: Optional[str] = Query(None, max_length=100),
    db: Session = Depends(get_db),
) -> list[dict]:
    query = select(Job).join(Company).where(Company.is_on_dashboard == True)  # noqa: E712
    if max_experience is not None:
        query = query.where(Job.experience_numeric <= max_experience)
    if recent_only:
        query = query.where(Job.date_posted >= utcnow() - timedelta(days=3))
    if min_matching_score is not None:
        query = query.where(Job.matching_score >= min_matching_score)
    if search:
        term = f"%{search.strip()}%"
        query = query.where((Job.title.ilike(term)) | (Company.name.ilike(term)))
    found = db.exec(query.order_by(Job.matching_score.desc(), Job.date_posted.desc())).all()
    return [{
        "id": job.id, "company_name": job.company.name, "title": job.title, "url": job.url,
        "job_type": job.job_type, "summary": job.summary, "matching_score": job.matching_score,
        "matching_feedback": job.matching_feedback, "experience_required": job.experience_required,
        "experience_numeric": job.experience_numeric, "skills": json.loads(job.skills_json),
        "date_posted": job.date_posted.isoformat(),
    } for job in found]


@app.get("/api/companies")
def companies(db: Session = Depends(get_db)) -> list[dict]:
    return [company_payload(c, db) for c in db.exec(select(Company).order_by(Company.name)).all()]


@app.post("/api/companies", status_code=201)
def add_company(payload: CompanyCreate, db: Session = Depends(get_db)) -> dict:
    name = payload.name.strip()
    if db.exec(select(Company).where(func.lower(Company.name) == name.lower())).first():
        raise HTTPException(409, "Company already exists")
    company = Company(**payload.model_dump(mode="json"), name=name)
    db.add(company)
    db.commit()
    db.refresh(company)
    return company_payload(company, db)


@app.patch("/api/companies/{company_id}")
def update_company(company_id: int, payload: CompanyUpdate, db: Session = Depends(get_db)) -> dict:
    company = db.get(Company, company_id)
    if not company:
        raise HTTPException(404, "Company not found")
    for key, value in payload.model_dump(exclude_unset=True, mode="json").items():
        setattr(company, key, value)
    db.add(company)
    db.commit()
    db.refresh(company)
    return company_payload(company, db)


@app.delete("/api/companies/{company_id}")
def delete_company(company_id: int, db: Session = Depends(get_db)) -> dict:
    company = db.get(Company, company_id)
    if not company:
        raise HTTPException(404, "Company not found")
    db.delete(company)
    db.commit()
    return {"deleted": company_id}


@app.get("/api/resume/active")
def active_resume(db: Session = Depends(get_db)) -> dict:
    resume = db.exec(select(Resume).where(Resume.is_active == True).order_by(Resume.uploaded_at.desc())).first()  # noqa: E712
    if not resume:
        return {"id": None, "filename": None, "uploaded_at": None, "page_count": 0, "word_count": 0}
    return {"id": resume.id, "filename": resume.filename, "uploaded_at": resume.uploaded_at.isoformat(), "page_count": resume.page_count, "word_count": resume.word_count}


@app.get("/api/resume/history")
def resume_history(db: Session = Depends(get_db)) -> list[dict]:
    resumes = db.exec(select(Resume).order_by(Resume.uploaded_at.desc())).all()
    return [{"id": r.id, "filename": r.filename, "uploaded_at": r.uploaded_at.isoformat(), "page_count": r.page_count, "word_count": r.word_count, "is_active": r.is_active} for r in resumes]


@app.post("/api/resume/upload", status_code=201)
async def upload_resume(file: UploadFile = File(...), db: Session = Depends(get_db)) -> dict:
    safe_name = Path(file.filename or "resume.pdf").name
    if Path(safe_name).suffix.lower() != ".pdf":
        raise HTTPException(400, "Only PDF resumes are supported")
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "Resume must be 12 MB or smaller")
    target = UPLOAD_DIR / f"{int(datetime.now().timestamp())}_{re.sub(r'[^A-Za-z0-9._-]', '_', safe_name)}"
    target.write_bytes(data)
    try:
        reader = PdfReader(target)
        text = "\n".join(page.extract_text() or "" for page in reader.pages).strip()
        if not text:
            raise ValueError("No readable text was found in this PDF")
    except Exception as exc:
        target.unlink(missing_ok=True)
        raise HTTPException(422, f"Could not read resume: {exc}") from exc
    for prior in db.exec(select(Resume).where(Resume.is_active == True)).all():  # noqa: E712
        prior.is_active = False
        db.add(prior)
    resume = Resume(filename=safe_name, file_path=str(target), parsed_text=text, page_count=len(reader.pages), word_count=len(text.split()))
    db.add(resume)
    db.commit()
    db.refresh(resume)
    return {"id": resume.id, "filename": resume.filename, "page_count": resume.page_count, "word_count": resume.word_count}


@app.post("/api/resume/{resume_id}/activate")
def activate_resume(resume_id: int, db: Session = Depends(get_db)) -> dict:
    resume = db.get(Resume, resume_id)
    if not resume:
        raise HTTPException(404, "Resume not found")
    for item in db.exec(select(Resume).where(Resume.is_active == True)).all():  # noqa: E712
        item.is_active = item.id == resume_id
        db.add(item)
    resume.is_active = True
    db.add(resume)
    db.commit()
    return {"active_resume_id": resume_id}


@app.get("/api/settings")
def settings(db: Session = Depends(get_db)) -> dict:
    roles = db.get(Setting, "default_roles")
    model = db.get(Setting, "ai_model")
    return {"roles": json.loads(roles.value) if roles else [], "model": model.value if model else "gemini/gemini-1.5-flash"}


@app.put("/api/settings/roles")
def save_roles(roles: list[str], db: Session = Depends(get_db)) -> dict:
    cleaned = list(dict.fromkeys(role.strip() for role in roles if role.strip()))[:20]
    setting = db.get(Setting, "default_roles") or Setting(key="default_roles", value="[]")
    setting.value = json.dumps(cleaned)
    db.add(setting)
    db.commit()
    return {"roles": cleaned}


@app.put("/api/settings/model")
def save_model(payload: ModelUpdate, db: Session = Depends(get_db)) -> dict:
    allowed = {"gemini/gemini-1.5-flash", "openai/gpt-4o-mini", "anthropic/claude-3-5-sonnet-20241022", "ollama/llama3"}
    if payload.model not in allowed:
        raise HTTPException(400, "Unsupported model")
    setting = db.get(Setting, "ai_model") or Setting(key="ai_model", value=payload.model)
    setting.value = payload.model
    db.add(setting)
    db.commit()
    return {"model": payload.model}


@app.get("/api/agent/status")
def agent_status(db: Session = Depends(get_db)) -> dict:
    latest = db.exec(select(AgentRun).order_by(AgentRun.started_at.desc())).first()
    return {"status": "running" if latest and latest.status == "running" else "idle", "next_run_minutes": 165, "latest_run": latest.started_at.isoformat() if latest else None}


@app.get("/api/agent/logs")
def agent_logs(db: Session = Depends(get_db)) -> dict:
    latest = db.exec(select(AgentRun).order_by(AgentRun.started_at.desc())).first()
    return {"lines": latest.log_text.splitlines() if latest else [], "run": {"status": latest.status, "jobs_scraped": latest.jobs_scraped, "jobs_matched": latest.jobs_matched} if latest else None}


def _demo_agent_run(run_id: int) -> None:
    asyncio.run(asyncio.sleep(0.2))
    with Session(engine) as db:
        run = db.get(AgentRun, run_id)
        if not run:
            return
        run.status = "completed"
        run.jobs_scraped = 7
        run.jobs_matched = 3
        run.completed_at = utcnow()
        run.log_text += "\n[manual] Crawl completed: 7 listings evaluated\n[manual] 3 high-affinity matches retained"
        db.add(run)
        db.commit()


@app.post("/api/agent/run", status_code=202)
def run_agent(background_tasks: BackgroundTasks, db: Session = Depends(get_db)) -> dict:
    running = db.exec(select(AgentRun).where(AgentRun.status == "running")).first()
    if running:
        return {"run_id": running.id, "status": "already_running"}
    run = AgentRun(status="running", log_text="[manual] On-demand run queued")
    db.add(run)
    db.commit()
    db.refresh(run)
    background_tasks.add_task(_demo_agent_run, run.id)
    return {"run_id": run.id, "status": "running"}
