"""Transactional email — magic links and purchase receipts.

MAIL_PROVIDER controls how: console (default — just logs it, fine for
launch), resend (HTTPS API), or smtp.
"""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage

from .config import get_settings

log = logging.getLogger("credits.mailer")


async def send_email(to: str, subject: str, html: str, text: str) -> None:
    s = get_settings()
    if s.mail_provider == "resend" and s.resend_api_key:
        await _send_resend(to, subject, html, text)
    elif s.mail_provider == "smtp" and s.smtp_host:
        from starlette.concurrency import run_in_threadpool
        await run_in_threadpool(_send_smtp, to, subject, html, text)
    else:
        log.warning("MAIL[console] to=%s subject=%s\n%s", to, subject, text)


async def _send_resend(to: str, subject: str, html: str, text: str) -> None:
    import httpx

    s = get_settings()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {s.resend_api_key}"},
            json={"from": f"{s.mail_from_name} <{s.mail_from}>", "to": [to],
                 "subject": subject, "html": html, "text": text},
        )
    if resp.status_code >= 300:
        log.error("resend failed %s %s", resp.status_code, resp.text[:400])
        raise RuntimeError("email_send_failed")


def _send_smtp(to: str, subject: str, html: str, text: str) -> None:
    s = get_settings()
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{s.mail_from_name} <{s.mail_from}>"
    msg["To"] = to
    msg.set_content(text)
    msg.add_alternative(html, subtype="html")
    with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=20) as server:
        if s.smtp_starttls:
            server.starttls()
        if s.smtp_user:
            server.login(s.smtp_user, s.smtp_password)
        server.send_message(msg)


_WRAP = """<!doctype html><html><body style="margin:0;background:#0b0b0c;padding:32px 16px;
font-family:ui-sans-serif,system-ui,sans-serif;color:#e8e8ea">
<table width="100%"><tr><td align="center">
<table width="480" style="background:#151517;border:1px solid #26262a;border-radius:14px;padding:32px">
<tr><td style="font-size:13px;letter-spacing:.14em;text-transform:uppercase;color:#8b8b93;padding-bottom:20px">
AudioForges</td></tr>
{body}
<tr><td style="padding-top:28px;border-top:1px solid #26262a;color:#6c6c75;font-size:12px;line-height:1.6">
audioforges.com. If you didn't request this, ignore this email.
</td></tr></table></td></tr></table></body></html>"""


def _button(url: str, label: str) -> str:
    return (f'<a href="{url}" style="display:inline-block;background:#e8e8ea;color:#0b0b0c;'
           f'text-decoration:none;font-weight:600;font-size:15px;padding:12px 22px;border-radius:8px">{label}</a>')


def magic_link_email(link: str, minutes: int) -> tuple[str, str, str]:
    body = (f'<tr><td style="font-size:20px;font-weight:600;padding-bottom:10px">Sign in to AudioForges</td></tr>'
           f'<tr><td style="font-size:14px;color:#b6b6bd;padding-bottom:22px">This link expires in {minutes} '
           f'minutes and works once.</td></tr><tr><td style="padding-bottom:22px">{_button(link, "Sign in")}</td></tr>'
           f'<tr><td style="font-size:12px;color:#6c6c75;word-break:break-all">{link}</td></tr>')
    text = f"Sign in to AudioForges:\n{link}\n\nExpires in {minutes} minutes."
    return ("Sign in to AudioForges", _WRAP.format(body=body), text)


def receipt_email(credits: int, balance: int, link: str) -> tuple[str, str, str]:
    body = (f'<tr><td style="font-size:20px;font-weight:600;padding-bottom:10px">{credits} credits added</td></tr>'
           f'<tr><td style="font-size:14px;color:#b6b6bd;padding-bottom:22px">Balance: {balance} credits. '
           f'Credits never expire.<br>Use this link to reach them on any device.</td></tr>'
           f'<tr><td style="padding-bottom:22px">{_button(link, "Open my account")}</td></tr>'
           f'<tr><td style="font-size:12px;color:#6c6c75;word-break:break-all">{link}</td></tr>')
    text = f"{credits} AudioForges credits added. Balance: {balance}. Access on any device: {link}"
    return (f"{credits} AudioForges credits added", _WRAP.format(body=body), text)