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


def unsubscribe_headers(url: str | None) -> dict | None:
    """One-click unsubscribe headers (RFC 8058) for non-transactional mail."""
    if not url:
        return None
    return {"List-Unsubscribe": f"<{url}>", "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"}


async def send_email(to: str, subject: str, html: str, text: str, headers: dict | None = None) -> None:
    s = get_settings()
    if s.mail_provider == "resend" and s.resend_api_key:
        await _send_resend(to, subject, html, text, headers)
    elif s.mail_provider == "smtp" and s.smtp_host:
        from starlette.concurrency import run_in_threadpool
        await run_in_threadpool(_send_smtp, to, subject, html, text, headers)
    else:
        log.warning("MAIL[console] to=%s subject=%s\n%s", to, subject, text)


async def _send_resend(to: str, subject: str, html: str, text: str, headers: dict | None = None) -> None:
    import httpx

    s = get_settings()
    body = {"from": f"{s.mail_from_name} <{s.mail_from}>", "to": [to],
            "subject": subject, "html": html, "text": text}
    if headers:
        body["headers"] = headers
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {s.resend_api_key}"},
            json=body,
        )
    if resp.status_code >= 300:
        log.error("resend failed %s %s", resp.status_code, resp.text[:400])
        raise RuntimeError("email_send_failed")


def _send_smtp(to: str, subject: str, html: str, text: str, headers: dict | None = None) -> None:
    s = get_settings()
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{s.mail_from_name} <{s.mail_from}>"
    msg["To"] = to
    for name, value in (headers or {}).items():
        msg[name] = value
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
If you didn't expect this email, you can safely ignore it.
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
            _wrap(body, f"Your sign-in link. It expires in {minutes} minutes."),
            text)


def pass_credit_life(rollover_months: int) -> str:
    if rollover_months <= 0:
        return "Unused Pass credits expire at your next renewal."
    unit = "month" if rollover_months == 1 else "months"
    return f"Unused Pass credits carry over for {rollover_months} {unit}, then expire."


def receipt_email(credits: int, balance: int, link: str, renewing: bool = False,
                  rollover_months: int = 2) -> tuple[str, str, str]:
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
        f'{bal_word}{"" if renewing else " · never expire"}</p>'
        f'</td></tr></table></td></tr>'
    )

    body = (
        _heading(f"{credits} {word} added")
        + _lede((f"Your Studio Pass payment went through. It renews monthly until you "
                 f"cancel. {pass_credit_life(rollover_months)} Pack credits never expire.") if renewing else
                ("Thanks for your purchase. Your credits work on every paid tool "
                 "on the site, and nothing renews."))
        + balance_block
        + _cta(link, "Open my account")
        + _lede("Use that link to reach your credits on any device: phone, "
                "laptop, or a browser you haven't used before.")
        + _raw_link(link)
        + _lede("A problem with a run or your purchase? Email "
                '<a href="mailto:contact@audioforges.com" style="color:inherit">'
                "contact@audioforges.com</a> and we'll sort it out.")
    )
    text = (
        f"{credits} AudioForges {word} added\n\n"
        + (f"Studio Pass payment received. It renews monthly until you cancel. "
           f"{pass_credit_life(rollover_months)} Pack credits never expire.\n\n" if renewing else "")
        + f"Balance: {balance} {bal_word}." + ("" if renewing else " Credits never expire.") + "\n\n"
        f"Reach them on any device:\n{link}\n\n"
        "A problem with a run or your purchase? Email contact@audioforges.com.\n\n"
        "audioforges.com"
    )
    return (f"{credits} AudioForges {word} added",
            _wrap(body, f"Balance: {balance} {bal_word}." + ("" if renewing else " Credits never expire.")),
            text)

def _pretty_date(value: str | None) -> str:
    if not value:
        return ""
    from datetime import datetime
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%d %B %Y").lstrip("0")
    except ValueError:
        return value[:10]


_PASS_COPY = {
    "started": (
        "Your Studio Pass is active",
        "Studio Pass is active",
        "{credits} credits are on your account, and {credits} more arrive every month. "
        "Forge Clean and Forge Split cost nothing extra while the Pass is active. {life}",
        "It renews monthly at ${price} until you cancel. You can cancel anytime from your account.",
    ),
    "reactivated": (
        "Your Studio Pass is back on",
        "Studio Pass reactivated",
        "Your payment went through and your Studio Pass is active again, with Forge Clean "
        "and Forge Split included.",
        "Next renewal: {date}.",
    ),
    "on_hold": (
        "Action needed: your Studio Pass payment failed",
        "Payment failed",
        "We couldn't take this month's Studio Pass payment, so the Pass is paused. "
        "Your existing credits still work.",
        "Update your card to switch it back on. Nothing is charged twice.",
    ),
    "cancel_scheduled": (
        "Your Studio Pass will end on {date}",
        "Cancellation confirmed",
        "Your Studio Pass won't renew. It stays active until {date}. Pass credits you "
        "haven't used stay on your account until they expire, and pack credits never expire.",
        "Changed your mind? You can resume it from your account before that date.",
    ),
    "cancel_undone": (
        "Your Studio Pass will keep renewing",
        "Studio Pass resumed",
        "Your Studio Pass is no longer set to end. It renews on {date} as usual.",
        "",
    ),
    "ended": (
        "Your Studio Pass has ended",
        "Studio Pass ended",
        "Your Studio Pass is no longer active. Pass credits you haven't used stay on your "
        "account until they expire, and pack credits never expire.",
        "You can start a new Pass or buy a credit pack anytime.",
    ),
}


def pass_email(kind: str, *, credits: int, price_usd: float, date: str | None,
               manage_url: str, rollover_months: int = 2) -> tuple[str, str, str]:
    subject, heading, lede, extra = _PASS_COPY[kind]
    values = {"credits": credits, "price": f"{price_usd:.2f}",
              "date": _pretty_date(date) or "the end of this billing month",
              "life": pass_credit_life(rollover_months)}
    subject, lede, extra = (part.format(**values) for part in (subject, lede, extra))
    label = "Update my card" if kind == "on_hold" else "Open my account"
    body = _heading(heading) + _lede(lede) + (_lede(extra) if extra else "") + _cta(manage_url, label)
    body += _lede("Questions? Email "
                  '<a href="mailto:contact@audioforges.com" style="color:inherit">'
                  "contact@audioforges.com</a>.")
    text = f"{heading}\n\n{lede}\n\n" + (f"{extra}\n\n" if extra else "") + \
        f"{label}: {manage_url}\n\nQuestions? Email contact@audioforges.com.\n\naudioforges.com"
    return subject, _wrap(body, lede[:110]), text

def _footer(unsubscribe_url: str, why: str) -> str:
    return (f'<tr><td style="padding:8px 32px 28px">'
            f'<p style="margin:0;font-family:{FONT};font-size:12px;line-height:1.6;color:{SUBTLE}">'
            f'{why} <a href="{unsubscribe_url}" style="color:{SUBTLE}">Unsubscribe</a></p></td></tr>')


def low_balance_email(balance: int, buy_url: str, unsubscribe_url: str) -> tuple[str, str, str]:
    word = "credit" if balance == 1 else "credits"
    lede = (f"You have {balance} {word} left. Top up now so your next Studio run "
            "doesn't stop halfway through a session. Credits never expire.")
    body = (_heading(f"{balance} {word} left") + _lede(lede) + _cta(buy_url, "Get more credits")
            + _footer(unsubscribe_url, "You get this when your balance runs low."))
    text = (f"{balance} AudioForges {word} left\n\n{lede}\n\nGet more credits: {buy_url}\n\n"
            f"Unsubscribe: {unsubscribe_url}")
    return f"You have {balance} AudioForges {word} left", _wrap(body, lede[:110]), text


def free_song_email(month: str, url: str, unsubscribe_url: str) -> tuple[str, str, str]:
    lede = (f"Your free Studio Quality song for {month} is on your account. Drop in any "
            "track and get studio-grade vocals and instrumental back as full-length WAV.")
    body = (_heading(f"Your free {month} song is ready") + _lede(lede) + _cta(url, "Use my free song")
            + _footer(unsubscribe_url, "You're getting this because you asked for AudioForges updates."))
    text = f"Your free {month} song is ready\n\n{lede}\n\n{url}\n\nUnsubscribe: {unsubscribe_url}"
    return f"Your free {month} Studio song is ready", _wrap(body, lede[:110]), text


def referral_reward_email(credits: int, url: str, unsubscribe_url: str) -> tuple[str, str, str]:
    lede = (f"A friend you invited just made their first purchase, so {credits} credits were added "
            "to your account. They got the same. Credits never expire.")
    body = (_heading(f"{credits} credits from your invite") + _lede(lede) + _cta(url, "Share my link again")
            + _footer(unsubscribe_url, "You get this when an invite pays off."))
    text = f"{credits} credits from your invite\n\n{lede}\n\n{url}\n\nUnsubscribe: {unsubscribe_url}"
    return f"You earned {credits} AudioForges credits", _wrap(body, lede[:110]), text


def update_email(subject: str, message: str, url: str, unsubscribe_url: str) -> tuple[str, str, str]:
    import html as _html
    paragraphs = [p.strip() for p in message.split("\n\n") if p.strip()]
    body = _heading(_html.escape(subject))
    for p in paragraphs:
        body += _lede(_html.escape(p).replace("\n", "<br>"))
    body += _cta(url, "Open AudioForges")
    body += _footer(unsubscribe_url, "You're getting this because you asked for AudioForges updates.")
    text = f"{subject}\n\n" + "\n\n".join(paragraphs) + f"\n\n{url}\n\nUnsubscribe: {unsubscribe_url}"
    return subject, _wrap(body, (paragraphs[0] if paragraphs else subject)[:110]), text