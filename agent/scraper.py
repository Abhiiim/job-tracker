from __future__ import annotations

import json
from typing import Any

from playwright.async_api import async_playwright
from sqlmodel import Session, select

from backend.app.database import Company, Setting, engine


def monitored_targets() -> tuple[list[Company], list[str]]:
    with Session(engine) as session:
        companies = session.exec(select(Company).where(Company.is_monitored_by_agent == True)).all()  # noqa: E712
        setting = session.get(Setting, "default_roles")
        roles = json.loads(setting.value) if setting else ["backend engineering"]
        session.expunge_all()
        return companies, roles


async def scrape_career_pages() -> list[dict[str, Any]]:
    companies, roles = monitored_targets()
    found: list[dict[str, Any]] = []
    if not companies:
        return found

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(user_agent="MatchPulse/1.0 (personal job tracker)")
        for company in companies:
            if not company.career_url:
                continue
            try:
                await page.goto(company.career_url, wait_until="domcontentloaded", timeout=30_000)
                links = page.locator("a[href]")
                for index in range(min(await links.count(), 80)):
                    link = links.nth(index)
                    title = (await link.inner_text()).strip().split("\n")[0][:180]
                    href = await link.get_attribute("href")
                    if not href or not title or not any(role.lower() in title.lower() for role in roles):
                        continue
                    absolute_url = await link.evaluate("element => element.href")
                    found.append({
                        "company_id": company.id,
                        "title": title,
                        "url": absolute_url,
                        "job_type": "Remote" if "remote" in title.lower() else "Hybrid/On-site",
                        "description": f"{title} at {company.name}",
                    })
            except Exception as exc:
                print(f"{company.name}: scrape skipped ({exc})")
        await browser.close()

    unique: dict[str, dict[str, Any]] = {job["url"]: job for job in found}
    return list(unique.values())
