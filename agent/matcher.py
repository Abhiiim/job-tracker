from __future__ import annotations

import json
import os
from typing import TypedDict

from litellm import completion
from sqlmodel import Session, select

from backend.app.database import Resume, Setting, engine


class MatchResult(TypedDict):
    years_experience_str: str
    years_experience_int: int
    summary: str
    score: float
    feedback: str
    skills: list[str]


def active_resume_text() -> str:
    with Session(engine) as session:
        resume = session.exec(select(Resume).where(Resume.is_active == True).order_by(Resume.uploaded_at.desc())).first()  # noqa: E712
        return resume.parsed_text if resume else "Backend engineer skilled in Python, FastAPI, Kafka, PostgreSQL, WebSockets, distributed systems, and RAG."


def selected_model() -> str:
    with Session(engine) as session:
        setting = session.get(Setting, "ai_model")
        return os.getenv("AI_MODEL") or (setting.value if setting else "gemini/gemini-1.5-flash")


def _validated(payload: dict) -> MatchResult:
    return {
        "years_experience_str": str(payload.get("years_experience_str", "Not specified"))[:80],
        "years_experience_int": max(0, min(int(payload.get("years_experience_int", 0)), 30)),
        "summary": str(payload.get("summary", ""))[:1200],
        "score": max(0.0, min(float(payload.get("score", 0)), 10.0)),
        "feedback": str(payload.get("feedback", ""))[:1200],
        "skills": [str(item)[:60] for item in payload.get("skills", [])[:10]],
    }


def match_job(title: str, description: str, resume: str) -> MatchResult:
    prompt = f"""Compare this resume and job. Return JSON only with keys years_experience_str,
years_experience_int, summary, score, feedback, and skills. Score must be 0-10.

RESUME:\n{resume[:16000]}\n\nJOB TITLE: {title}\nJOB DESCRIPTION:\n{description[:10000]}"""
    try:
        response = completion(
            model=selected_model(), messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}, temperature=0.1,
        )
        content = response.choices[0].message.content or "{}"
        return _validated(json.loads(content.replace("```json", "").replace("```", "").strip()))
    except Exception as exc:
        print(f"LiteLLM unavailable: {exc}")
        return {
            "years_experience_str": "Not specified", "years_experience_int": 0,
            "summary": description[:500] or f"Open role: {title}", "score": 0.0,
            "feedback": "AI scoring is unavailable. Configure the selected model credentials and run the agent again.",
            "skills": [],
        }
