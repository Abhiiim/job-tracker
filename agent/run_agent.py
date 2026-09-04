from __future__ import annotations

import asyncio
import json

from sqlmodel import Session, select

from backend.app.database import AgentRun, Company, Job, engine, init_db, utcnow
from agent.matcher import active_resume_text, match_job
from agent.notifier import send_alert_email
from agent.scraper import scrape_career_pages


async def main() -> None:
    init_db()
    run = AgentRun(status="running", log_text="Agent run started")
    with Session(engine) as session:
        session.add(run)
        session.commit()
        session.refresh(run)
        run_id = run.id

    scraped = await scrape_career_pages()
    resume = active_resume_text()
    added = 0
    with Session(engine) as session:
        for item in scraped:
            if session.exec(select(Job).where(Job.url == item["url"])).first():
                continue
            result = match_job(item["title"], item["description"], resume)
            session.add(Job(
                company_id=item["company_id"], title=item["title"], url=item["url"], job_type=item["job_type"],
                summary=result["summary"], matching_score=result["score"], matching_feedback=result["feedback"],
                experience_required=result["years_experience_str"], experience_numeric=result["years_experience_int"],
                skills_json=json.dumps(result["skills"]),
            ))
            company = session.get(Company, item["company_id"])
            if company:
                company.last_scraped_at = utcnow()
                session.add(company)
            added += 1
        run = session.get(AgentRun, run_id)
        if run:
            run.status, run.jobs_scraped, run.jobs_matched, run.completed_at = "completed", len(scraped), added, utcnow()
            run.log_text += f"\n{len(scraped)} listings found\n{added} new roles matched"
            session.add(run)
        session.commit()
    send_alert_email()


if __name__ == "__main__":
    asyncio.run(main())
