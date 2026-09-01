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


# --- branding ---------------------------------------------------------------
#
# PNG, NOT THE SVG. Gmail, Outlook and Apple Mail all strip or fail to render
# SVG, so /icon.svg would leave a broken image in most inboxes. Absolute URL
# because an email has no origin to resolve a relative path against.
#
# Width and height are set as ATTRIBUTES as well as CSS: Outlook's Word
# renderer ignores the CSS and falls back to the image's intrinsic size, which
# on a 512px source is a logo the width of the card.
#
# background is the CARD colour, not none. The source is a rounded dark tile
# with transparent corners, and Outlook composites transparency onto white —
# which would put a white square around the mark on a dark card.

LOGO_URL = "https://www.audioforges.com/images/logo.png"

BG = "#0b0b0c"
CARD = "#151517"
BORDER = "#26262a"
TEXT = "#e8e8ea"
MUTED = "#b6b6bd"
SUBTLE = "#6c6c75"
AMBER = "#f59e0b"
INK = "#0b0b0c"

FONT = ("-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,"
        "sans-serif")
MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace"


def _wrap(body: str, preheader: str) -> str:
    """Card layout shared by every email.

    The preheader is the grey line an inbox shows beside the subject. Left
    unset, clients scrape the first visible text — which here is the word
    AUDIOFORGES, so every email previewed identically. Hidden in the body and
    padded so nothing after it leaks into the preview.
    """
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark light">
<meta name="supported-color-schemes" content="dark light">
<title>AudioForges</title>
</head>
<body style="margin:0;padding:0;background:{BG};">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;height:0;width:0">
{preheader}&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;&#847;&zwnj;&nbsp;
</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
       style="background:{BG};padding:40px 16px">
<tr><td align="center">

<table role="presentation" width="480" cellpadding="0" cellspacing="0" border="0"
       style="width:480px;max-width:100%;background:{CARD};border:1px solid {BORDER};border-radius:16px">

<tr><td style="padding:28px 32px 0">
<table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
<td style="padding-right:11px;line-height:0;vertical-align:middle">
<img src="{LOGO_URL}" width="32" height="32" alt=""
     style="display:block;width:32px;height:32px;border:0;border-radius:8px;background:{CARD}">
</td>
<td style="font-family:{MONO};font-size:14px;font-weight:600;letter-spacing:.02em;color:{TEXT};vertical-align:middle">
AudioForges
</td>
</tr></table>
</td></tr>

<tr><td style="padding:0 32px">
<div style="height:1px;background:{BORDER};margin:24px 0 28px"></div>
</td></tr>

{body}

<tr><td style="padding:0 32px 30px">
<div style="height:1px;background:{BORDER};margin:30px 0 20px"></div>
<p style="margin:0;font-family:{FONT};font-size:12px;line-height:1.7;color:{SUBTLE}">
<a href="https://www.audioforges.com" style="color:{SUBTLE};text-decoration:none">audioforges.com</a>
&nbsp;·&nbsp; Free audio tools, no sign-up.
</p>
<p style="margin:8px 0 0;font-family:{FONT};font-size:12px;line-height:1.7;color:{SUBTLE}">
If you didn't request this, you can ignore this email — nothing will happen.
</p>
</td></tr>

</table>
</td></tr></table>
</body></html>"""


def _button(url: str, label: str) -> str:
    """Amber on ink, matching the primary button on the site.

    mso- properties are Outlook-only and stop its renderer collapsing the
    padding into a text link.
    """
    return (
        f'<a href="{url}" style="display:inline-block;background:{AMBER};color:{INK};'
        f'font-family:{FONT};font-size:15px;font-weight:600;line-height:1;'
        f'text-decoration:none;padding:14px 26px;border-radius:10px;'
        f'mso-padding-alt:14px 26px;mso-line-height-rule:exactly">{label}</a>'
    )


def _heading(text: str) -> str:
    return (f'<tr><td style="padding:0 32px;font-family:{FONT};font-size:21px;'
            f'font-weight:700;letter-spacing:-.01em;line-height:1.3;color:{TEXT}">'
            f'{text}</td></tr>')


def _lede(text: str) -> str:
    return (f'<tr><td style="padding:10px 32px 0;font-family:{FONT};font-size:15px;'
            f'line-height:1.65;color:{MUTED}">{text}</td></tr>')


def _cta(url: str, label: str) -> str:
    return f'<tr><td style="padding:26px 32px 0">{_button(url, label)}</td></tr>'


def _raw_link(url: str) -> str:
    """The URL in plain text under the button.

    Some clients rewrite or strip anchor hrefs, and some people paste rather
    than click. Monospace so it reads as a value rather than prose.
    """
    return (f'<tr><td style="padding:22px 32px 0;font-family:{MONO};font-size:11px;'
            f'line-height:1.6;color:{SUBTLE};word-break:break-all">'
            f'<a href="{url}" style="color:{SUBTLE};text-decoration:none">{url}</a>'
            f'</td></tr>')


def magic_link_email(link: str, minutes: int) -> tuple[str, str, str]:
    body = (
        _heading("Sign in to AudioForges")
        + _lede(f"This link expires in {minutes} minutes and works once. "
                f"Open it on the device you want your credits on.")
        + _cta(link, "Sign in")
        + _raw_link(link)
    )
    text = (
        "Sign in to AudioForges\n\n"
        f"{link}\n\n"
        f"This link expires in {minutes} minutes and works once. "
        "Open it on the device you want your credits on.\n\n"
        "If you didn't request this, ignore this email.\n"
        "audioforges.com"
    )
    return ("Sign in to AudioForges",
            _wrap(body, f"Your sign-in link — expires in {minutes} minutes."),
            text)


def receipt_email(credits: int, balance: int, link: str) -> tuple[str, str, str]:
    word = "credit" if credits == 1 else "credits"
    bal_word = "credit" if balance == 1 else "credits"

    # The balance as a figure rather than a sentence. It is the one thing
    # someone opens a receipt to check, and a number set large is read before
    # any prose around it.
    balance_block = (
        f'<tr><td style="padding:24px 32px 0">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"'
        f' style="background:{BG};border:1px solid {BORDER};border-radius:12px">'
        f'<tr><td style="padding:18px 20px">'
        f'<p style="margin:0;font-family:{MONO};font-size:10px;letter-spacing:.16em;'
        f'text-transform:uppercase;color:{SUBTLE}">Balance</p>'
        f'<p style="margin:6px 0 0;font-family:{MONO};font-size:28px;font-weight:700;'
        f'line-height:1;color:{AMBER}">{balance}</p>'
        f'<p style="margin:6px 0 0;font-family:{FONT};font-size:13px;color:{MUTED}">'
        f'{bal_word} · never expire</p>'
        f'</td></tr></table></td></tr>'
    )

    body = (
        _heading(f"{credits} {word} added")
        + _lede("Thanks for supporting AudioForges. Your credits work on every "
                "GPU-backed tool on the site, and there is nothing recurring to "
                "cancel.")
        + balance_block
        + _cta(link, "Open my account")
        + _lede("Use that link to reach your credits on any device — phone, "
                "laptop, or a browser you haven't used before.")
        + _raw_link(link)
    )
    text = (
        f"{credits} AudioForges {word} added\n\n"
        f"Balance: {balance} {bal_word}. Credits never expire.\n\n"
        f"Reach them on any device:\n{link}\n\n"
        "audioforges.com"
    )
    return (f"{credits} AudioForges {word} added",
            _wrap(body, f"Balance: {balance} {bal_word}. Credits never expire."),
            text)