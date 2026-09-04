from __future__ import annotations

import html
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sqlmodel import Session, select

from backend.app.database import Company, Job, engine, utcnow


def send_alert_email() -> int:
    sender = os.getenv("SENDER_EMAIL")
    password = os.getenv("SENDER_PASSWORD")
    receiver = os.getenv("RECEIVER_EMAIL")
    if not sender or not password or not receiver:
        print("SMTP credentials are not configured; digest skipped.")
        return 0

    with Session(engine) as session:
        jobs = session.exec(select(Job).where(Job.matching_score >= 8, Job.notified == False)).all()  # noqa: E712
        if not jobs:
            return 0
        cards = []
        for job in jobs:
            company = session.get(Company, job.company_id)
            cards.append(
                f'<section style="border-left:4px solid #4f46e5;padding:12px 16px;margin:16px 0">'
                f'<small>{html.escape(company.name if company else "Company")}</small>'
                f'<h3>{html.escape(job.title)}</h3><strong>{job.matching_score:.1f}/10</strong>'
                f'<p>{html.escape(job.summary)}</p><a href="{html.escape(job.url, quote=True)}">View role</a></section>'
            )
        message = MIMEMultipart("alternative")
        message["Subject"] = f"MatchPulse: {len(jobs)} high-affinity roles"
        message["From"], message["To"] = sender, receiver
        message.attach(MIMEText(f'<html><body style="font-family:Arial"><h2>MatchPulse daily digest</h2>{"".join(cards)}</body></html>', "html"))
        with smtplib.SMTP(os.getenv("SMTP_HOST", "smtp.gmail.com"), int(os.getenv("SMTP_PORT", "587"))) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, receiver, message.as_string())
        for job in jobs:
            job.notified = True
            session.add(job)
        session.commit()
        print(f"Digest sent at {utcnow().isoformat()}")
        return len(jobs)
