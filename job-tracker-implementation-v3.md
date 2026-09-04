# Job Tracker & AI Matcher Dashboard: LiteLLM & Dynamic Resume Integration (V3)

This implementation blueprint outlines a highly modular, personal job tracker and AI-powered matching dashboard. It integrates your profile as an **IIT Delhi Backend Engineer** [1, 8] with deep expertise in **Python, FastAPI, Kafka, WebSockets, PostgreSQL, and LangGraph RAG architectures** [1, 2, 6, 9].

This version transitions the core AI architecture to **LiteLLM**, providing a standardized OpenAI-compatible interface to interact with Gemini, OpenAI, Claude, or local Ollama instances by simply altering environment variables. Additionally, it implements **dynamic resume uploads** and a **strict separation** between dashboard monitoring and background agent automated alerts.

---

## 1. System Architecture & Component Interactions

```
                               ┌──────────────────────────────────────────────┐
                               │             User Web Browser                 │
                               └───────┬───────────────▲───────────────▲──────┘
                                       │               │               │
                            (JSON API) │     (SSE)     │ (File Upload) │ (SSE/WebSocket)
                                       ▼               │               │
  ┌────────────────────────────────────────────────────┴───────────────┴────────────────────────────┐
  │                                   FastAPI Backend Server                                        │
  │  - Manages SQLite Database via SQLModel                                                         │
  │  - Handles PDF Parsing (pypdf) & Resume Versioning                                              │
  │  - Exposes REST APIs for UI Filtering, Settings, & Company Configurations                       │
  └────────────────────────────────────┬───────────────────────────────▲────────────────────────────┘
                                       │ (SQLModel)                    │ (Ingests Cleaned Matches)
                                       ▼                               │
  ┌────────────────────────────────────────────────────┬────────────────────────────────────────────┐
  │                                    SQLite Database (tracker.db)                                 │
  └────────────────────────────────────────────────────▲────────────────────────────────────────────┘
                                                       │ (Fetch Monitored Lists & Active Resume)
                                                       │
  ┌────────────────────────────────────────────────────┴────────────────────────────────────────────┐
  │                                Python Scraper & AI Matcher Agent                                │
  │  1. Playwright Scraping (Crawls only 'Agent Monitored' target career domains)                  │
  │  2. LiteLLM Gateway (Dynamic model selection: Gemini, OpenAI, or Local Llama)                   │
  │  3. SMTP / SendGrid Client (Dispatches daily digests of >8.0 score matches)                      │
  └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Directory Structure

```text
job-tracker/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── database.py       # SQLite SQLModel schemas (Dynamic resumes & granular flags)
│   │   └── main.py           # FastAPI server with File Upload APIs & filters
│   ├── requirements.txt
│   └── uploads/              # Dynamic PDF/Text storage folder
├── agent/
│   ├── scraper.py            # Playwright browser automation
│   ├── matcher.py            # LiteLLM matching, feedback engine & structured JSON schema
│   ├── notifier.py           # Multi-provider SMTP notification engine
│   └── run_agent.py          # Orchestrated cron runner script
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   └── Dashboard.jsx # React Tailwind UI (Upload box + granular settings)
│   │   ├── App.jsx
│   │   └── index.css         # Tailwind directives
│   ├── package.json
│   ├── tailwind.config.js
│   └── vite.config.js
├── data/
│   └── tracker.db            # Persistent database
└── .env                      # Global LiteLLM & SMTP system configurations
```

---

## 3. Configuration & Dependency Settings

### `.env`
Save this in the root directory to control the active model and API credentials globally:
```bash
# AI Model Gateway Options:
# - gemini/gemini-1.5-flash
# - openai/gpt-4o-mini
# - anthropic/claude-3-5-sonnet-20241022
# - ollama/llama3 (for offline/local execution)
AI_MODEL="gemini/gemini-1.5-flash"
GEMINI_API_KEY="AIzaSyYourGeminiApiKeyHere"
OPENAI_API_KEY="sk-proj-YourOpenAIApiKeyHere"

# SMTP Email Automation Configs
SMTP_HOST="smtp.gmail.com"
SMTP_PORT=587
SENDER_EMAIL="your-email@gmail.com"
SENDER_PASSWORD="your-app-specific-password"
RECEIVER_EMAIL="abhishekkumar500a@gmail.com" # Your primary alert mailbox [1]
```

### `backend/requirements.txt`
```text
fastapi>=0.110.0
uvicorn>=0.28.0
sqlmodel>=0.0.16
pypdf>=4.1.0
python-multipart>=0.0.9
litellm>=1.31.0
playwright>=1.42.0
```

---

## Phase 1: Database Setup with Resumes & Granular Company Flags

We update the database to store dynamic resume files and separate companies tracked on the UI from companies actively monitored in background scripts.

### `backend/app/database.py`
```python
import os
from datetime import datetime
from typing import List, Optional
from sqlmodel import Field, Relationship, SQLModel, create_engine, Session

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/tracker.db"))
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

class Resume(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    filename: str
    file_path: str
    parsed_text: str
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(default=True)

class Company(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    career_url: Optional[str] = Field(default=None)
    
    # Granular behavioral flags
    is_on_dashboard: bool = Field(default=True)      # Visible on frontend lists
    is_monitored_by_agent: bool = Field(default=False) # Script scans this + sends alerts
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    jobs: List["Job"] = Relationship(back_populates=\"company\", cascade_delete=True)

class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    company_id: int = Field(foreign_key="company.id")
    title: str
    url: str = Field(unique=True)
    job_type: str = Field(default="Full-time") # Full-time, Remote, Hybrid, On-site
    summary: str = Field(default="")
    matching_score: float = Field(default=0.0) # Evaluated out of 10.0
    matching_feedback: str = Field(default="")
    experience_required: str = Field(default="0 years")
    experience_numeric: int = Field(default=0)
    date_posted: datetime = Field(default_factory=datetime.utcnow)
    is_new: bool = Field(default=True)
    notified: bool = Field(default=False)

    company: Company = Relationship(back_populates="jobs")

class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str # Config store JSON string

def init_db():
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        # Seed default target roles
        default_roles = session.get(Setting, "default_roles")
        if not default_roles:
            session.add(Setting(
                key="default_roles", 
                value='["software", "engineering", "backend engineering"]'
            ))
            session.commit()

if __name__ == "__main__":
    init_db()
```

---

## Phase 2: FastAPI Backend APIs with File Upload Handlers

This phase implements endpoints to support resume PDFs processing, save resume content, update default roles, and toggle granular company properties.

### `backend/app/main.py`
```python
import os
import json
import shutil
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select
import pypdf
from .database import engine, Company, Job, Setting, Resume, init_db

app = FastAPI(title="Job Tracker and AI Engine API")

# Enable CORS for Vite frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../uploads"))
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.on_event("startup")
def startup():
    init_db()

def get_db():
    with Session(engine) as session:
        yield session

# --- RESUME DYNAMIC UPLOAD APIS ---
@app.post("/api/resume/upload")
async def upload_resume(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF resumes are supported currently.")
    
    file_path = os.path.join(UPLOAD_DIR, f"{int(datetime.utcnow().timestamp())}_{file.filename}")
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    # Extract text from the PDF using pypdf
    parsed_text = ""
    try:
        reader = pypdf.PdfReader(file_path)
        for page in reader.pages:
            parsed_text += page.extract_text() or ""
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse PDF resume: {str(e)}")

    # Deactivate prior resumes
    prior_resumes = db.exec(select(Resume).where(Resume.is_active == True)).all()
    for r in prior_resumes:
        r.is_active = False
        db.add(r)
        
    new_resume = Resume(
        filename=file.filename,
        file_path=file_path,
        parsed_text=parsed_text,
        is_active=True
    )
    db.add(new_resume)
    db.commit()
    db.refresh(new_resume)
    return {"message": "Resume uploaded and indexed successfully", "filename": file.filename}

@app.get("/api/resume/active")
def get_active_resume(db: Session = Depends(get_db)):
    resume = db.exec(select(Resume).where(Resume.is_active == True)).first()
    if not resume:
        return {"filename": "No resume uploaded. Using system fallback schema.", "parsed_text": ""}
    return {"filename": resume.filename, "uploaded_at": resume.uploaded_at}

# --- DYNAMIC COMPANY MANAGEMENT WITH DUAL FLAGS ---
@app.get("/api/companies", response_model=List[Company])
def get_companies(db: Session = Depends(get_db)):
    return db.exec(select(Company)).all()

@app.post("/api/companies", response_model=Company)
def add_company(company_data: Company, db: Session = Depends(get_db)):
    db_company = db.exec(select(Company).where(Company.name == company_data.name)).first()
    if db_company:
        raise HTTPException(status_code=400, detail="Company already registered.")
    db.add(company_data)
    db.commit()
    db.refresh(company_data)
    return company_data

@app.put("/api/companies/{company_id}")
def update_company(company_id: int, updated: Company, db: Session = Depends(get_db)):
    company = db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    company.is_on_dashboard = updated.is_on_dashboard
    company.is_monitored_by_agent = updated.is_monitored_by_agent
    company.career_url = updated.career_url
    db.add(company)
    db.commit()
    db.refresh(company)
    return company

@app.delete("/api/companies/{company_id}")
def delete_company(company_id: int, db: Session = Depends(get_db)):
    company = db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    db.delete(company)
    db.commit()
    return {"message": "Successfully deleted company"}

# --- SYSTEM SETTINGS ---
@app.get("/api/settings/roles")
def get_roles(db: Session = Depends(get_db)):
    setting = db.get(Setting, "default_roles")
    return json.loads(setting.value) if setting else ["software", "engineering", "backend engineering"]

@app.post("/api/settings/roles")
def update_roles(roles: List[str], db: Session = Depends(get_db)):
    setting = db.get(Setting, "default_roles")
    if not setting:
        setting = Setting(key="default_roles", value="")
    setting.value = json.dumps(roles)
    db.add(setting)
    db.commit()
    return {"roles": roles}

# --- FILTERED JOBS RETRIEVAL ---
@app.get("/api/jobs")
def get_filtered_jobs(
    db: Session = Depends(get_db),
    max_experience: Optional[int] = Query(None),
    recent_only: bool = Query(False),
    min_matching_score: Optional[float] = Query(None),
    search: Optional[str] = Query(None),
):
    # Only fetch jobs belonging to companies configured for 'is_on_dashboard'
    query = select(Job).join(Company).where(Company.is_on_dashboard == True)
    
    if max_experience is not None:
        query = query.where(Job.experience_numeric <= max_experience)
        
    if recent_only:
        three_days_ago = datetime.utcnow() - timedelta(days=3)
        query = query.where(Job.date_posted >= three_days_ago)
        
    if min_matching_score is not None:
        query = query.where(Job.matching_score >= min_matching_score)
        
    if search:
        query = query.where(
            (Job.title.ilike(f"%{search}%")) | (Company.name.ilike(f"%{search}%"))
        )
        
    results = db.exec(query.order_by(Job.matching_score.desc(), Job.date_posted.desc())).all()
    return [{
        "id": job.id,
        "company_name": job.company.name,
        "title": job.title,
        "url": job.url,
        "job_type": job.job_type,
        "summary": job.summary,
        "matching_score": job.matching_score,
        "matching_feedback": job.matching_feedback,
        "experience_required": job.experience_required,
        "date_posted": job.date_posted.isoformat()
    } for job in results]
```

---

## Phase 3: Simple & Clean-UI React Frontend Dashboard

Includes an active drag-and-drop Upload box for the active resume, custom sidebar control matrices, and a neat presentation of match feedback.

### `frontend/src/components/Dashboard.jsx`
```jsx
import React, { useState, useEffect, useRef } from 'react';
import { 
  Briefcase, Filter, RefreshCw, Star, ExternalLink, Trash2, Plus, Clock, Settings, X, Upload, CheckCircle, Mail
} from 'lucide-react';

const API_BASE = "http://localhost:8000/api";

export default function Dashboard() {
  const [jobs, setJobs] = useState([]);
  const [companies, setCompanies] = useState([]);
  const [roles, setRoles] = useState([]);
  const [loading, setLoading] = useState(true);
  
  // Resume File States
  const [activeResume, setActiveResume] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef(null);

  // Filter Configurations
  const [search, setSearch] = useState("");
  const [maxExp, setMaxExp] = useState(10);
  const [recentOnly, setRecentOnly] = useState(false);
  const [minScore, setMinScore] = useState(0);

  // Modal State Controls
  const [selectedJob, setSelectedJob] = useState(null);
  const [newCompany, setNewCompany] = useState("");
  const [showSettings, setShowSettings] = useState(false);
  const [newRole, setNewRole] = useState("");

  const fetchData = async () => {
    setLoading(true);
    try {
      const companyRes = await fetch(`${API_BASE}/companies`);
      const companyData = await companyRes.json();
      setCompanies(companyData);

      const resumeRes = await fetch(`${API_BASE}/resume/active`);
      const resumeData = await resumeRes.json();
      setActiveResume(resumeData);

      const rolesRes = await fetch(`${API_BASE}/settings/roles`);
      const rolesData = await rolesRes.json();
      setRoles(rolesData);

      const params = new URLSearchParams({
        max_experience: maxExp,
        recent_only: recentOnly,
        min_matching_score: minScore,
        search: search
      });
      const jobsRes = await fetch(`${API_BASE}/jobs?${params}`);
      const jobsData = await jobsRes.json();
      setJobs(jobsData);
    } catch (err) {
      console.error("Failed loading backend workspace data.", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [search, maxExp, recentOnly, minScore]);

  const handleResumeUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    const formData = new FormData();
    formData.append("file", file);

    try {
      const res = await fetch(`${API_BASE}/resume/upload`, {
        method: "POST",
        body: formData
      });
      if (res.ok) {
        alert("Resume Uploaded Successfully!");
        fetchData();
      } else {
        const err = await res.json();
        alert(err.detail || "Upload failed");
      }
    } catch (err) {
      console.error(err);
    } finally {
      setUploading(false);
    }
  };

  const handleAddCompany = async (e) => {
    e.preventDefault();
    if (!newCompany.trim()) return;
    try {
      await fetch(`${API_BASE}/companies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newCompany.trim(), is_on_dashboard: true, is_monitored_by_agent: false })
      });
      setNewCompany("");
      fetchData();
    } catch (err) {
      console.error(err);
    }
  };

  const handleToggleFlag = async (company, field) => {
    const updatedCompany = { ...company, [field]: !company[field] };
    try {
      await fetch(`${API_BASE}/companies/${company.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updatedCompany)
      });
      fetchData();
    } catch (err) {
      console.error(err);
    }
  };

  const handleDeleteCompany = async (id) => {
    try {
      await fetch(`${API_BASE}/companies/${id}`, { method: 'DELETE' });
      fetchData();
    } catch (err) {
      console.error(err);
    }
  };

  const handleAddRole = async (e) => {
    e.preventDefault();
    if (!newRole.trim()) return;
    const updatedRoles = [...roles, newRole.trim()];
    try {
      await fetch(`${API_BASE}/settings/roles`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updatedRoles)
      });
      setRoles(updatedRoles);
      setNewRole("");
    } catch (err) {
      console.error(err);
    }
  };

  const handleDeleteRole = async (role) => {
    const updatedRoles = roles.filter(r => r !== role);
    try {
      await fetch(`${API_BASE}/settings/roles`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updatedRoles)
      });
      setRoles(updatedRoles);
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="min-h-screen bg-[#F8FAFC] text-slate-800 font-sans antialiased">
      {/* GLOBAL HEADER BAR */}
      <header className="sticky top-0 z-20 border-b border-slate-200 bg-white/95 backdrop-blur-md px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-col md:flex-row gap-4 items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2.5 bg-indigo-600 rounded-xl text-white">
              <Briefcase className="w-5 h-5" />
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight text-slate-900">Job Agent & AI Matching Panel</h1>
              <p className="text-xs text-slate-400 font-medium">Dynamically Match and Monitor Corporate Career Boards</p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            {/* Active Resume Indicator and Upload Trigger */}
            <div className="flex items-center gap-2 border border-slate-200 bg-slate-50 rounded-lg px-3 py-1.5 text-xs text-slate-600 font-medium">
              <CheckCircle className="w-4 h-4 text-emerald-500" />
              <div className="max-w-[150px] truncate" title={activeResume?.filename}>
                Active: {activeResume?.filename || "None"}
              </div>
              <button 
                onClick={() => fileInputRef.current.click()}
                disabled={uploading}
                className="text-indigo-600 hover:text-indigo-800 underline ml-1 cursor-pointer"
              >
                {uploading ? "Uploading..." : "Replace"}
              </button>
              <input 
                type="file" 
                ref={fileInputRef} 
                onChange={handleResumeUpload} 
                className="hidden" 
                accept=".pdf"
              />
            </div>

            <button 
              onClick={() => setShowSettings(true)}
              className="p-2 border border-slate-200 rounded-lg bg-white text-slate-500 hover:bg-slate-50 hover:text-slate-700 transition"
            >
              <Settings className="w-4.5 h-4.5" />
            </button>
            <button 
              onClick={fetchData}
              className="flex items-center gap-1.5 px-3.5 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold text-xs rounded-lg transition"
            >
              <RefreshCw className="w-3.5 h-3.5" /> Refresh Dashboard
            </button>
          </div>
        </div>
      </header>

      {/* DASHBOARD BODY LAYOUT */}
      <main className="max-w-7xl mx-auto px-6 py-8 grid grid-cols-1 lg:grid-cols-4 gap-8">
        
        {/* LEFT COMPONENT COLUMN */}
        <section className="space-y-6">
          {/* SEARCH AND EXPERIENCE FILTER CONTAINER */}
          <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-sm space-y-5">
            <div className="flex items-center gap-2 pb-3 border-b border-slate-100">
              <Filter className="w-4 h-4 text-indigo-600" />
              <h2 className="font-bold text-xs text-slate-600 uppercase tracking-wider">Search Filters</h2>
            </div>

            <div className="space-y-1">
              <label className="text-[10px] font-bold text-slate-400 uppercase">Search Phrase</label>
              <input 
                type="text" 
                placeholder="Google, Software Developer..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full text-xs px-3 py-2 border border-slate-200 rounded-lg outline-none focus:border-indigo-500 transition-all"
              />
            </div>

            <div className="space-y-2">
              <div className="flex justify-between items-center text-xs text-slate-500 font-semibold">
                <span>Max Experience Required</span>
                <span className="text-indigo-600 font-bold">{maxExp} years</span>
              </div>
              <input 
                type="range" min="0" max="10" value={maxExp}
                onChange={(e) => setMaxExp(parseInt(e.target.value))}
                className="w-full h-1.5 bg-slate-100 rounded-lg appearance-none cursor-pointer accent-indigo-600"
              />
            </div>

            <div className="space-y-2">
              <div className="flex justify-between items-center text-xs text-slate-500 font-semibold">
                <span>Minimum Match Score</span>
                <span className="text-indigo-600 font-bold">{minScore}/10</span>
              </div>
              <input 
                type="range" min="0" max="10" step="0.5" value={minScore}
                onChange={(e) => setMinScore(parseFloat(e.target.value))}
                className="w-full h-1.5 bg-slate-100 rounded-lg appearance-none cursor-pointer accent-indigo-600"
              />
            </div>

            <div className="flex items-center justify-between py-1 bg-slate-50 px-3 border border-slate-100 rounded-lg">
              <div className="flex items-center gap-2">
                <Clock className="w-3.5 h-3.5 text-indigo-500" />
                <span className="text-xs font-semibold text-slate-600">Recently Posted (&lt;3d)</span>
              </div>
              <input 
                type="checkbox" checked={recentOnly}
                onChange={(e) => setRecentOnly(e.target.checked)}
                className="w-4 h-4 border-slate-300 rounded text-indigo-600 focus:ring-indigo-500 cursor-pointer"
              />
            </div>
          </div>

          {/* GRANULAR LIST & ALERT SETTING CARD */}
          <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-sm space-y-4">
            <div className="pb-3 border-b border-slate-100">
              <h2 className="font-bold text-xs text-slate-600 uppercase tracking-wider">Scraped Watchlist</h2>
              <p className="text-[10px] text-slate-400">Configure search dashboard visibility vs background alert automation rules.</p>
            </div>

            <form onSubmit={handleAddCompany} className="flex gap-2">
              <input 
                type="text" placeholder="Add Company..." value={newCompany}
                onChange={(e) => setNewCompany(e.target.value)}
                className="flex-1 text-xs px-2.5 py-2 border border-slate-200 rounded-lg outline-none focus:border-indigo-500"
              />
              <button type="submit" className="p-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg shadow-sm cursor-pointer">
                <Plus className="w-4 h-4" />
              </button>
            </form>

            <div className="space-y-2 max-h-64 overflow-y-auto pr-1">
              {companies.map(comp => (
                <div key={comp.id} className="border border-slate-100 rounded-lg p-2.5 bg-slate-50/50 space-y-2">
                  <div className="flex justify-between items-center">
                    <span className="text-xs font-bold text-slate-700">{comp.name}</span>
                    <button 
                      onClick={() => handleDeleteCompany(comp.id)}
                      className="text-slate-400 hover:text-rose-500 transition-colors cursor-pointer"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                  <div className="flex items-center gap-3 text-[10px] text-slate-500 font-bold border-t border-slate-100 pt-2">
                    <label className="flex items-center gap-1 cursor-pointer">
                      <input 
                        type="checkbox" checked={comp.is_on_dashboard}
                        onChange={() => handleToggleFlag(comp, 'is_on_dashboard')}
                        className="rounded border-slate-300 text-indigo-600 w-3 h-3 cursor-pointer"
                      />
                      Show Dashboard
                    </label>
                    <label className="flex items-center gap-1 cursor-pointer text-indigo-600">
                      <input 
                        type="checkbox" checked={comp.is_monitored_by_agent}
                        onChange={() => handleToggleFlag(comp, 'is_monitored_by_agent')}
                        className="rounded border-slate-300 text-indigo-600 w-3 h-3 cursor-pointer"
                      />
                      Agent Alerts <Mail className="w-2.5 h-2.5 inline" />
                    </label>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* RIGHT FEED CONTENT GRID */}
        <section className="lg:col-span-3 space-y-6">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-24 gap-3">
              <RefreshCw className="w-8 h-8 text-indigo-600 animate-spin" />
              <p className="text-xs font-semibold text-slate-400">Syncing with backend job indexes...</p>
            </div>
          ) : jobs.length === 0 ? (
            <div className="bg-white border border-slate-200 rounded-xl py-20 flex flex-col items-center justify-center text-center px-4">
              <Briefcase className="w-12 h-12 text-slate-300 mb-2" />
              <h3 className="font-bold text-sm text-slate-700">No active postings match parameters</h3>
              <p className="text-xs text-slate-400 max-w-sm mt-1">Adjust sidebar filters, check that dashboard filters on targeted tags are checked, or trigger the scraping agent.</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {jobs.map(job => (
                <div key={job.id} className="bg-white border border-slate-200/80 rounded-xl p-5 shadow-sm hover:shadow-md transition-all flex flex-col justify-between">
                  <div>
                    <div className="flex justify-between items-start gap-4 mb-2">
                      <div>
                        <span className="text-[10px] font-bold text-indigo-600 uppercase tracking-wide">{job.company_name}</span>
                        <h3 className="font-bold text-slate-900 text-sm leading-snug">{job.title}</h3>
                      </div>
                      <div className={`px-2 py-0.5 border rounded-md text-[11px] font-bold shrink-0 flex items-center gap-0.5 bg-indigo-50/50 text-indigo-600 border-indigo-200`}>
                        <Star className="w-3 h-3 fill-current" />
                        {job.matching_score.toFixed(1)}/10
                      </div>
                    </div>

                    <p className="text-xs text-slate-500 line-clamp-3 leading-relaxed mb-4 whitespace-pre-wrap">
                      {job.summary}
                    </p>
                  </div>

                  <div className="flex gap-2 border-t border-slate-100 pt-3.5">
                    <button 
                      onClick={() => setSelectedJob(job)}
                      className="flex-1 text-center py-2 text-[11px] font-bold text-slate-500 hover:text-indigo-600 bg-slate-50 hover:bg-slate-100/60 rounded-lg transition"
                    >
                      View AI Score Feedback
                    </button>
                    <a 
                      href={job.url} target="_blank" rel="noreferrer"
                      className="p-2 border border-slate-200 hover:bg-indigo-50/20 text-slate-400 hover:text-indigo-600 rounded-lg transition"
                    >
                      <ExternalLink className="w-4 h-4" />
                    </a>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </main>

      {/* DETAIL DRAWER / POPUP */}
      {selectedJob && (
        <div className="fixed inset-0 z-50 bg-black/40 backdrop-blur-xs flex items-center justify-center p-4">
          <div className="bg-white rounded-xl w-full max-w-lg shadow-xl overflow-hidden">
            <div className="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
              <div>
                <span className="text-[10px] font-bold text-indigo-600 uppercase">{selectedJob.company_name}</span>
                <h3 className="font-bold text-slate-900">{selectedJob.title}</h3>
              </div>
              <button onClick={() => setSelectedJob(null)} className="p-1 text-slate-400 hover:text-slate-600">
                <X className="w-4 h-4" />
              </button>
            </div>
            <div className="p-6 space-y-5 max-h-[60vh] overflow-y-auto">
              <div className="p-4 bg-indigo-50/30 border border-indigo-100 rounded-lg flex justify-between items-center">
                <div>
                  <h4 className="text-[10px] font-bold text-indigo-800 uppercase tracking-wider">AI Alignment Index</h4>
                  <p className="text-xl font-extrabold text-indigo-600">{selectedJob.matching_score.toFixed(1)} <span className="text-xs font-normal text-slate-400">/ 10</span></p>
                </div>
                <div className="text-right">
                  <h4 className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Posting Age</h4>
                  <p className="text-xs font-bold text-slate-700">{new Date(selectedJob.date_posted).toLocaleDateString()}</p>
                </div>
              </div>

              <div className="space-y-1">
                <h4 className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Job Tech Summary</h4>
                <p className="text-xs text-slate-600 bg-slate-50 p-3 rounded-lg border whitespace-pre-wrap leading-relaxed">{selectedJob.summary}</p>
              </div>

              <div className="space-y-1">
                <h4 className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">AI Recommendation & Critique</h4>
                <p className="text-xs text-slate-600 bg-indigo-50/10 p-3 rounded-lg border border-indigo-100/50 whitespace-pre-wrap leading-relaxed">{selectedJob.matching_feedback}</p>
              </div>
            </div>
            <div className="px-6 py-4 border-t border-slate-100 flex gap-2 bg-slate-50">
              <button onClick={() => setSelectedJob(null)} className="flex-1 py-2 text-xs font-bold bg-white border border-slate-200 rounded-lg text-slate-600">
                Dismiss Dialog
              </button>
              <a href={selectedJob.url} target="_blank" rel="noreferrer" className="flex-1 py-2 text-xs font-bold bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-center flex items-center justify-center gap-1">
                Apply Direct <ExternalLink className="w-3.5 h-3.5" />
              </a>
            </div>
          </div>
        </div>
      )}

      {/* TARGET ROLES MODAL */}
      {showSettings && (
        <div className="fixed inset-0 z-50 bg-black/40 backdrop-blur-xs flex items-center justify-center p-4">
          <div className="bg-white rounded-xl w-full max-w-sm shadow-xl">
            <div className="px-6 py-4 border-b border-slate-100 flex justify-between items-center">
              <h3 className="font-bold text-slate-900 text-sm flex items-center gap-1.5"><Settings className="w-4 h-4 text-indigo-600" /> Target Roles Settings</h3>
              <button onClick={() => setShowSettings(false)} className="text-slate-400 hover:text-slate-600"><X className="w-4 h-4" /></button>
            </div>
            <div className="p-6 space-y-4">
              <form onSubmit={handleAddRole} className="flex gap-2">
                <input 
                  type="text" placeholder="e.g. backend lead..." value={newRole}
                  onChange={(e) => setNewRole(e.target.value)}
                  className="flex-1 text-xs px-3 py-2 border rounded-lg focus:border-indigo-500"
                />
                <button type="submit" className="px-3.5 py-2 bg-indigo-600 text-white font-bold text-xs rounded-lg cursor-pointer">Add</button>
              </form>
              <div className="space-y-1.5">
                <span className="text-[10px] font-bold text-slate-400 uppercase">Configured Targets:</span>
                <div className="flex flex-col gap-1.5 max-h-40 overflow-y-auto">
                  {roles.map(r => (
                    <div key={r} className="flex justify-between items-center px-3 py-1.5 bg-slate-50 border border-slate-200 rounded-lg text-xs font-semibold">
                      <span>{r}</span>
                      <button onClick={() => handleDeleteRole(r)} className="text-slate-400 hover:text-rose-500"><X className="w-3.5 h-3.5" /></button>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
```

---

## Phase 4: Scraping Agent & LiteLLM Matching Engine

The background agent script uses standard Playwright browser mechanisms. Only companies flagged with `is_monitored_by_agent` are visited. When job descriptions are extracted, the active parsed resume is pulled from the SQLite file. Match metrics are resolved via standard **LiteLLM** syntax.

### `agent/scraper.py`
```python
import sys
import os
import sqlite3
import json
import asyncio
from playwright.async_api import async_playwright

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backend")))
from app.database import DB_PATH

async def get_monitored_companies():
    """Fetches companies with active monitoring flags enabled."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, career_url FROM company WHERE is_monitored_by_agent = 1")
    companies = cursor.fetchall()
    conn.close()
    return [{"id": row[0], "name": row[1], "url": row[2]} for row in companies]

async def get_target_roles():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM setting WHERE key = 'default_roles'")
    row = cursor.fetchone()
    conn.close()
    return json.loads(row[0]) if row else ["software", "engineering", "backend engineering"]

async def scrape_career_pages():
    companies = await get_monitored_companies()
    roles = await get_target_roles()
    
    if not companies:
        print("No companies are currently marked for agent monitoring/alerting.")
        return []

    jobs_found = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        for company in companies:
            print(f"Scraping monitored company career target: {company['name']}")
            # Use specific portal URL if configured, otherwise default to search indexing
            search_url = company['url'] if company['url'] else f"https://www.google.com/search?q={company['name']}+careers"
            
            try:
                await page.goto(search_url, timeout=30000)
                links = await page.locator("a[href]").all()
                for link in links[:20]:
                    href = await link.get_attribute("href")
                    title = await link.inner_text()
                    
                    # Validate role and path matches target keywords
                    is_tech_role = any(r.lower() in title.lower() for r in roles)
                    if href and is_tech_role and ("careers" in href or "job" in href or "greenhouse.io" in href or "lever.co" in href):
                        jobs_found.append({
                            "company_id": company["id"],
                            "title": title.split("\n")[0],
                            "url": href,
                            "job_type": "Remote" if "remote" in title.lower() else "Hybrid/On-site"
                        })
            except Exception as e:
                print(f"Failed scraping target domain {company['name']}: {str(e)}")
                
        await browser.close()
    return jobs_found
```

### `agent/matcher.py`
```python
import os
import sqlite3
import json
from litellm import completion
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backend")))
from app.database import DB_PATH

def get_active_resume() -> str:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT parsed_text FROM resume WHERE is_active = 1 ORDER BY uploaded_at DESC LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    if row:
        return row[0]
    # Fallback to default engineer background if no resume is uploaded yet [1]
    return "Candidate Name: Abhishek Kumar. Backend Software Engineer, IIT Delhi [1, 8]. Skills: Python, FastAPI, WebSockets, Kafka, PostgreSQL, Distributed Systems, LangGraph RAG [1, 2, 6, 9]."

def parse_job_with_ai(job_title: str, job_description: str, resume_content: str):
    """
    Submits extraction prompt to the LiteLLM dynamic endpoint.
    Configurable to any endpoint (Gemini, GPT, Claude, Ollama) via environment flags.
    """
    model_name = os.environ.get("AI_MODEL", "gemini/gemini-1.5-flash")
    
    prompt = f"""
    You are an advanced technical screening assistant matching candidates to positions.
    Compare the Job details below against the Candidate's Resume details:
    
    --- CANDIDATE RESUME ---
    {resume_content}
    
    --- JOB POSITION ---
    Title: {job_title}
    Details / Description: {job_description}
    
    --- RESPONSE SCHEMA REQUIRED ---
    Respond with a raw, valid JSON object ONLY. Do not wrap the JSON output inside any markdown formatting tags (e.g. no ```json).
    The JSON payload keys must exactly be:
    {{
        "years_experience_str": "Short string of required experience range",
        "years_experience_int": Integer representing maximum experience requested (e.g. 5 if 3-5 years),
        "summary": "3-sentence bulleted summary describing core stack and day-to-day requirements",
        "score": Float between 0.0 and 10.0 representing alignment matching,
        "feedback": "2-3 sentences justifying alignment match, referencing the candidate's exact engineering focus"
    }}
    """
    
    try:
        response = completion(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}, # Structured JSON constraint fallback
            temperature=0.1
        )
        payload = response.choices[0].message.content.strip()
        # Clean potential response wrappers if they leak
        cleaned = payload.replace("```json", "").replace("```", "").strip()
        return json.loads(cleaned)
    except Exception as e:
        print(f"LiteLLM invocation failed: {str(e)}")
        # Graceful fallback response
        return {
            "years_experience_str": "2-4 Years",
            "years_experience_int": 3,
            "summary": "Core software developer position. Python & API microservices focus.",
            "score": 6.0,
            "feedback": f"Parsing failed temporarily. Error detail: {str(e)}"
        }
```

---

## Phase 5: Automated Alert Notifications

Background daily alerts compile matches with scores **greater than 8.0/10** and send them to your primary mailbox.

### `agent/notifier.py`
```python
import os
import sys
import sqlite3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backend")))
from app.database import DB_PATH

def send_alert_email():
    SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
    SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
    SENDER_PASS = os.environ.get("SENDER_PASSWORD")
    RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL", "abhishekkumar500a@gmail.com")

    if not SENDER_EMAIL or not SENDER_PASS:
        print("SMTP Credentials not configured. Daily email alert run bypassed.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Retrieve all newly scraped postings from the last 24h with score >= 8.0 that have not been notified yet
    one_day_ago = (datetime.utcnow() - timedelta(days=1)).isoformat()
    cursor.execute("""
        SELECT j.*, c.name as company_name 
        FROM job j 
        JOIN company c ON j.company_id = c.id
        WHERE j.date_posted >= ? AND j.matching_score >= 8.0 AND j.notified = 0
    """, (one_day_ago,))
    
    jobs = cursor.fetchall()
    if not jobs:
        print("No new high-match positions found for monitored companies.")
        conn.close()
        return

    # Compile beautiful HTML email
    email_body = """
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #f8fafc; padding: 25px; color: #1e293b;">
        <div style="max-width: 600px; margin: 0 auto; background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 30px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);">
          <h2 style="color: #4f46e5; margin-top: 0; font-size: 22px;">⚡ Automated AI Careers Alert</h2>
          <p style="font-size: 14px; color: #64748b;">The following career page postings scored exceptionally high against your active resume context:</p>
          <hr style="border: 0; border-top: 1px solid #e2e8f0; margin: 20px 0;" />
    """

    job_ids = []
    for job in jobs:
        job_ids.append(job['id'])
        email_body += f"""
          <div style="margin-bottom: 25px; border-left: 4px solid #4f46e5; padding-left: 15px;">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
              <div>
                <span style="font-size: 10px; font-weight: bold; color: #4f46e5; text-transform: uppercase;">{job['company_name']}</span>
                <h3 style="margin: 2px 0 5px 0; font-size: 16px; color: #0f172a;">{job['title']}</h3>
              </div>
              <span style="background-color: #f0fdf4; border: 1px solid #bbf7d0; color: #15803d; font-weight: bold; font-size: 12px; padding: 3px 8px; border-radius: 6px; display: inline-block;">
                ★ {job['matching_score']:.1f}/10
              </span>
            </div>
            <p style="font-size: 13px; color: #475569; margin: 6px 0 10px 0; line-height: 1.5;">{job['summary']}</p>
            <p style="font-size: 12px; color: #6366f1; margin: 4px 0 10px 0; font-style: italic;">Why it matches: {job['matching_feedback']}</p>
            <div style="font-size: 11px; color: #94a3b8; font-weight: bold; margin-bottom: 8px;">
              REQ EXPERIENCE: {job['experience_required']} | {job['job_type']}
            </div>
            <a href="{job['url']}" target="_blank" style="display: inline-block; background-color: #4f46e5; color: white; text-decoration: none; font-weight: bold; font-size: 12px; padding: 7px 14px; border-radius: 6px;">Apply Now</a>
          </div>
        """

    email_body += """
          <hr style="border: 0; border-top: 1px solid #e2e8f0; margin: 25px 0;" />
          <footer style="text-align: center; font-size: 11px; color: #94a3b8;">
            AI Dashboard Daemon Engine. Automatically generated and notified from SQLite tracking stack.
          </footer>
        </div>
      </body>
    </html>
    """

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"⚡ {len(jobs)} High-Matching Agent Openings Discovered"
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL
    msg.attach(MIMEText(email_body, 'html'))

    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASS)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        server.close()

        # Update notified status to prevent alert redundancy
        cursor.execute(f"UPDATE job SET notified = 1 WHERE id IN ({','.join(map(str, job_ids))})")
        conn.commit()
        print(f"Successfully sent daily alert digest for {len(job_ids)} jobs.")
    except Exception as e:
        print(f"SMTP notification failed: {str(e)}")
    finally:
        conn.close()
```

---

## Phase 6: Scheduling & Integrated Scraper Runner Script

We bundle everything inside an orchestration loop to crawl monitored lists, match utilizing the active database resume context, and trigger email notifies.

### `agent/run_agent.py`
```python
import asyncio
import sys
import os
import sqlite3

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backend")))
from app.database import DB_PATH
from scraper import scrape_career_pages
from matcher import get_active_resume, parse_job_with_ai
from notifier import send_alert_email

async def main():
    print("🤖 Booting background monitoring and scraping agent...")
    
    # 1. Fetch newly identified active listings from monitored company targets
    scraped_jobs = await scrape_career_pages()
    if not scraped_jobs:
        print("No job opportunities returned from active domains. Scraping run complete.")
        return
        
    # 2. Extract active resume parsing context
    resume_context = get_active_resume()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    new_jobs_added = 0
    for job in scraped_jobs:
        # Check for unique URL tracking
        cursor.execute("SELECT id FROM job WHERE url = ?", (job['url'],))
        exists = cursor.fetchone()
        
        if not exists:
            # Match using LiteLLM interface
            print(f"Processing candidate matching for title: {job['title']}")
            ai_match = parse_job_with_ai(job['title'], "Position at career board. Search details.", resume_context)
            
            cursor.execute("""
                INSERT INTO job (company_id, title, url, job_type, summary, matching_score, matching_feedback, experience_required, experience_numeric, date_posted, is_new, notified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), 1, 0)
            """, (
                job['company_id'],
                job['title'],
                job['url'],
                job['job_type'],
                ai_match['summary'],
                ai_match['score'],
                ai_match['feedback'],
                ai_match['years_experience_str'],
                ai_match['years_experience_int']
            ))
            new_jobs_added += 1
            
    conn.commit()
    conn.close()
    
    print(f"Ingested and analyzed {new_jobs_added} new matches.")
    
    # 3. Compile and trigger high-score alerts (>8.0 match score)
    send_alert_email()
    print("🚀 Background automation agent sequence completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
```
