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
    ? `<div style="text-align:center; margin: 0 0 20px;">
         <a href="${projectUrl}" style="display:inline-block;">
           <img src="${thumbnailUrl}" alt="${escapeHtml(project)}"
                style="width:100%; max-width:560px; aspect-ratio:4/3; object-fit:cover; display:block; margin:0 auto; border-radius:6px; border:1px solid #e5e1d8;">
         </a>
       </div>`
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

// Anonymous contact-request form on the public landing page. Sends a short
// notification to the platform owner with the requester's contact details and
// sets Reply-To so the owner can reply directly.
async function sendContactRequest({ name, company, email }) {
  init();
  if (!sgMail) return { sent: false, reason: "email-not-configured" };

  const subject = `GTP access request — ${name}${company ? ` (${company})` : ""}`;

  const text =
    `A visitor on the GTP landing page asked for access:\n\n` +
    `Name:    ${name}\n` +
    `Company: ${company}\n` +
    `Email:   ${email}\n\n` +
    `Reply directly to this message to contact them.\n`;

  const html = `
    <div style="font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 560px; margin: 0 auto; padding: 24px; color: #0d0d0d;">
      <h2 style="margin: 0 0 12px; font-size: 1.125em;">New GTP access request</h2>
      <p style="margin: 0 0 16px; color: #4a4a4a;">A visitor on the GTP landing page asked for access:</p>
      <table style="border-collapse: collapse; width: 100%; font-size: 0.95em;">
        <tr><td style="padding: 6px 12px 6px 0; color: #6b6b6b; vertical-align: top;">Name</td><td style="padding: 6px 0;">${escapeHtml(name)}</td></tr>
        <tr><td style="padding: 6px 12px 6px 0; color: #6b6b6b; vertical-align: top;">Company</td><td style="padding: 6px 0;">${escapeHtml(company)}</td></tr>
        <tr><td style="padding: 6px 12px 6px 0; color: #6b6b6b; vertical-align: top;">Email</td><td style="padding: 6px 0;"><a href="mailto:${escapeHtml(email)}" style="color: #0a66c2;">${escapeHtml(email)}</a></td></tr>
      </table>
      <p style="margin-top: 20px; font-size: 0.85em; color: #777;">Reply directly to this message to contact the requester.</p>
    </div>
  `;

  const msg = {
    to: { email: "j@gt3d.com" },
    from: {
      email: process.env.SENDGRID_FROM_EMAIL,
      name: process.env.SENDGRID_FROM_NAME || "Ground Truth 3D",
    },
    subject,
    text,
    html,
    replyTo: { email, name },
  };

  try {
    await sgMail.send(msg);
    return { sent: true };
  } catch (err) {
    const detail = err.response?.body || err.message;
    console.error("[email-service] SendGrid send failed (contact-request):", JSON.stringify(detail));
    return { sent: false, reason: "send-failed", error: err.message };
  }
}

// Heads-up to the platform owner whenever a project is shared (either
// through the authenticated /api/share-project flow or via the anonymous
// public link). Best-effort — the caller treats a non-sent result as a
// log-and-continue, so a hard failure here never breaks the share itself.
async function sendShareNotification({
  project,
  projectUrl,
  recipientEmail,
  recipientName,
  sharerName,
  sharerEmail,
  isPublicLink,
}) {
  init();
  if (!sgMail) return { sent: false, reason: "email-not-configured" };

  const sharerLabel = isPublicLink
    ? "(anonymous public-link share)"
    : `${sharerName || "Someone"}${sharerEmail ? ` <${sharerEmail}>` : ""}`;
  const subject = `GTP project shared — ${project}`;

  const text =
    `${sharerLabel} shared the "${project}" project.\n\n` +
    `Recipient: ${recipientName || "(no name)"} <${recipientEmail}>\n` +
    `Project URL: ${projectUrl}\n` +
    `Time: ${new Date().toISOString()}\n`;

  const html = `
    <div style="font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 560px; margin: 0 auto; padding: 24px; color: #0d0d0d;">
      <h2 style="margin: 0 0 12px; font-size: 1.125em;">Project shared</h2>
      <p style="margin: 0 0 16px; color: #4a4a4a;">${escapeHtml(sharerLabel)} shared the <strong>${escapeHtml(project)}</strong> project.</p>
      <table style="border-collapse: collapse; width: 100%; font-size: 0.95em;">
        <tr><td style="padding: 6px 12px 6px 0; color: #6b6b6b; vertical-align: top;">Recipient</td><td style="padding: 6px 0;">${escapeHtml(recipientName || "(no name)")} &lt;<a href="mailto:${escapeHtml(recipientEmail)}" style="color: #0a66c2;">${escapeHtml(recipientEmail)}</a>&gt;</td></tr>
        <tr><td style="padding: 6px 12px 6px 0; color: #6b6b6b; vertical-align: top;">Project</td><td style="padding: 6px 0;"><a href="${escapeHtml(projectUrl)}" style="color: #0a66c2; word-break: break-all;">${escapeHtml(projectUrl)}</a></td></tr>
        <tr><td style="padding: 6px 12px 6px 0; color: #6b6b6b; vertical-align: top;">Time</td><td style="padding: 6px 0;">${new Date().toISOString()}</td></tr>
      </table>
    </div>
  `;

  const msg = {
    to: { email: "j@gt3d.com" },
    from: {
      email: process.env.SENDGRID_FROM_EMAIL,
      name: process.env.SENDGRID_FROM_NAME || "Ground Truth 3D",
    },
    subject,
    text,
    html,
    replyTo: !isPublicLink && sharerEmail
      ? { email: sharerEmail, name: sharerName || undefined }
      : undefined,
  };

  try {
    await sgMail.send(msg);
    return { sent: true };
  } catch (err) {
    const detail = err.response?.body || err.message;
    console.error("[email-service] SendGrid send failed (share-notification):", JSON.stringify(detail));
    return { sent: false, reason: "send-failed", error: err.message };
  }
}

module.exports = { sendShareInvite, sendContactRequest, sendShareNotification, isEnabled };
