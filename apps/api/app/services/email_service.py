"""
Email OTP service using Resend.
Sends 6-digit verification codes for signup and login.
"""
import logging
import httpx
from app.core.config import settings

logger = logging.getLogger(__name__)

RESEND_API_URL = "https://api.resend.com/emails"

# Beautiful HTML email template
OTP_EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f6f9;font-family:'Segoe UI',Roboto,sans-serif;">
<div style="max-width:480px;margin:40px auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
  <div style="background:linear-gradient(135deg,#1E3A8A,#3B82F6);padding:32px 28px;text-align:center;">
    <div style="display:inline-block;background:linear-gradient(135deg,#EB6711,#1E3A8A);width:48px;height:48px;border-radius:12px;line-height:48px;color:#fff;font-weight:800;font-size:16px;">SA</div>
    <h1 style="color:#fff;margin:12px 0 0;font-size:22px;font-weight:700;">Suvidha AI</h1>
  </div>
  <div style="padding:32px 28px;">
    <p style="color:#374151;font-size:15px;margin:0 0 8px;">Hi{name_part},</p>
    <p style="color:#6B7280;font-size:14px;margin:0 0 24px;">Your verification code is:</p>
    <div style="text-align:center;margin:0 0 24px;">
      <div style="display:inline-block;background:#F0F4FF;border:2px dashed #3B82F6;border-radius:12px;padding:16px 40px;">
        <span style="font-size:36px;font-weight:800;letter-spacing:8px;color:#1E3A8A;">{otp_code}</span>
      </div>
    </div>
    <p style="color:#9CA3AF;font-size:13px;margin:0 0 8px;">⏱ This code expires in <strong>10 minutes</strong>.</p>
    <p style="color:#9CA3AF;font-size:13px;margin:0;">If you didn't request this code, please ignore this email.</p>
  </div>
  <div style="background:#F9FAFB;padding:16px 28px;text-align:center;border-top:1px solid #F3F4F6;">
    <p style="color:#D1D5DB;font-size:11px;margin:0;">© 2026 Suvidha AI · India's AI-Powered CA Platform</p>
  </div>
</div>
</body>
</html>
"""


async def send_otp_email(to_email: str, otp_code: str, name: str = "") -> bool:
    """
    Send OTP verification email via Resend.
    Returns True if sent successfully.
    """
    if not settings.RESEND_API_KEY:
        logger.error("RESEND_API_KEY not configured")
        return False

    name_part = f" {name}" if name else ""
    html = OTP_EMAIL_TEMPLATE.format(otp_code=otp_code, name_part=name_part)

    subject_map = {
        4: f"Your code: {otp_code}",
        6: f"Your verification code: {otp_code}",
    }
    subject = subject_map.get(len(otp_code), f"Your verification code: {otp_code}")

    payload = {
        "from": "Suvidha AI <noreply@suvidhaai.com>",
        "to": [to_email],
        "subject": subject,
        "html": html,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {settings.RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            if resp.status_code in (200, 201):
                logger.info(f"✅ OTP email sent to {to_email}")
                return True
            else:
                logger.error(f"❌ Resend API error {resp.status_code}: {resp.text}")
                return False
    except Exception as e:
        logger.error(f"❌ Failed to send OTP email: {e}")
        return False
