// Thin wrapper around @sendgrid/mail. No-ops gracefully when SENDGRID_API_KEY
// is unset (e.g., local dev) so callers can fall back to client-side mailto.
//
// Required env: SENDGRID_API_KEY, SENDGRID_FROM_EMAIL
// Optional env: SENDGRID_FROM_NAME (defaults to "Ground Truth 3D")

let sgMail = null;
let configured = false;

function init() {
  if (configured) return;
  configured = true;
  const key = process.env.SENDGRID_API_KEY;
  if (!key) {
    console.warn("[email-service] SENDGRID_API_KEY not set — share invites will not send.");
    return;
  }
  if (!process.env.SENDGRID_FROM_EMAIL) {
    console.warn("[email-service] SENDGRID_FROM_EMAIL not set — share invites will not send.");
    return;
  }
  try {
    sgMail = require("@sendgrid/mail");
    sgMail.setApiKey(key);
  } catch (err) {
    console.error("[email-service] Failed to load @sendgrid/mail:", err.message);
    sgMail = null;
  }
}

function isEnabled() {
  init();
  return !!sgMail;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

async function sendShareInvite({ toEmail, toName, fromName, fromEmail, project, projectUrl, thumbnailUrl }) {
  init();
  if (!sgMail) return { sent: false, reason: "email-not-configured" };

  const sender = fromName ? `${fromName}` : "Someone";
  const subject = `${sender} shared the "${project}" project with you`;

  const text =
    `Hi ${toName},\n\n` +
    `${sender} (${fromEmail}) has given you viewer access to the "${project}" project on Ground Truth 3D.\n\n` +
    `Open the project: ${projectUrl}\n\n` +
    `If you weren't expecting this, you can ignore this message.\n`;

  const thumbnailHtml = thumbnailUrl
    ? `<a href="${projectUrl}" style="display:block; margin: 0 0 20px;">
         <img src="${thumbnailUrl}" alt="${escapeHtml(project)}"
              style="width:100%; max-width:560px; aspect-ratio:4/3; object-fit:cover; display:block; border-radius:6px; border:1px solid #e5e1d8;">
       </a>`
    : "";

  const html = `
    <div style="font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; max-width: 560px; margin: 0 auto; color: #1a1a1a;">
      ${thumbnailHtml}
      <p>Hi ${escapeHtml(toName)},</p>
      <p><strong>${escapeHtml(sender)}</strong> (${escapeHtml(fromEmail)}) has given you viewer access to the
        <strong>${escapeHtml(project)}</strong> project on Ground Truth 3D.</p>
      <p style="margin: 28px 0;">
        <a href="${projectUrl}"
           style="background:#1a1a1a;color:#fff;text-decoration:none;padding:12px 20px;border-radius:6px;display:inline-block;font-weight:600;">
          Open project
        </a>
      </p>
      <p style="font-size: 0.9em; color: #555;">Or paste this link into your browser:<br>
        <a href="${projectUrl}" style="color:#0a66c2; word-break: break-all;">${escapeHtml(projectUrl)}</a>
      </p>
      <p style="font-size: 0.85em; color: #777;">If you weren't expecting this, you can ignore this message.</p>
    </div>
  `;

  const msg = {
    to: { email: toEmail, name: toName },
    from: {
      email: process.env.SENDGRID_FROM_EMAIL,
      name: process.env.SENDGRID_FROM_NAME || "Ground Truth 3D",
    },
    subject,
    text,
    html,
    replyTo: fromEmail ? { email: fromEmail, name: fromName || undefined } : undefined,
  };

  try {
    await sgMail.send(msg);
    return { sent: true };
  } catch (err) {
    const detail = err.response?.body || err.message;
    console.error("[email-service] SendGrid send failed:", JSON.stringify(detail));
    return { sent: false, reason: "send-failed", error: err.message };
  }
}

module.exports = { sendShareInvite, isEnabled };
