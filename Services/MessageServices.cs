#pragma warning disable 0436
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using MailKit.Net.Smtp;
using MailKit.Security;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using MimeKit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace JobWorker.Services
{
    // Used by the SQS worker to send email notifications from background jobs.
    // SMS operations are no-ops — this host has no Twilio dependency.
    public class JobWorkerMessageSender : IEmailSender, ISimpleEmailSender, ISmsSender
    {
        public AuthMessageSenderOptions Options { get; }
        private readonly ApplicationDbContext _context;

        public JobWorkerMessageSender(
            IOptions<AuthMessageSenderOptions> optionsAccessor,
            ApplicationDbContext context)
        {
            Options = optionsAccessor.Value;
            _context = context;
        }

        // ---------------------------------------------------------------
        // IEmailSender / ISimpleEmailSender
        // ---------------------------------------------------------------

        public async Task SendEmailAsync(
            string email,
            string subject,
            string message,
            string cc = "",
            string bcc = "",
            string sAttachFileName = "",
            byte[]? attachBytes = null,
            MimeKit.ContentType? ctype = null)
        {
            string? sHostName  = Options.EmailHost;
            int     port       = Options.EmailPort ?? 587;
            string? sFromName  = Options.EmailName;
            string? sFromEmail = Options.EmailFrom;
            string? sSvcUser   = Options.EmailServiceUser;
            string? sSvcKey    = Options.EmailServiceKey;

            using var client = new SmtpClient();
            await client.ConnectAsync(sHostName, port, SecureSocketOptions.Auto);

            if (client.Capabilities.HasFlag(SmtpCapabilities.Authentication))
            {
                client.AuthenticationMechanisms.Remove("XOAUTH2");
                await client.AuthenticateAsync(sSvcUser, sSvcKey);
            }

            var msg = new MimeMessage();
            msg.From.Add(new MailboxAddress(sFromName, sFromEmail));
            msg.To.Add(new MailboxAddress(email, email));
            msg.Subject = subject;

            if (!string.IsNullOrEmpty(cc))
                msg.Cc.Add(new MailboxAddress(cc, cc));

            if (!string.IsNullOrEmpty(bcc))
                msg.Bcc.Add(new MailboxAddress(bcc, bcc));

            var bodyBuilder = new BodyBuilder
            {
                HtmlBody = message,
                TextBody = message
            };

            if (!string.IsNullOrEmpty(sAttachFileName) && attachBytes != null && ctype != null)
                bodyBuilder.Attachments.Add(sAttachFileName, attachBytes, ctype);

            msg.Body = bodyBuilder.ToMessageBody();

            await client.SendAsync(msg);
            await client.DisconnectAsync(true);
        }

        public Task SendEmailTemplate(
            string docid,
            string emailtype,
            string username,
            string to,
            string subject,
            string message,
            Dictionary<string, string> replacePattern,
            string cc = "",
            string bcc = "",
            string replyTo = "")
        {
            document? doc  = null;
            emailmessage? tpl  = null;
            emailconfig?  conf = null;

            if (!string.IsNullOrEmpty(docid))
            {
                doc = _context.document
                    .AsNoTracking()
                    .Include(d => d.Publication)
                    .Where(d => d.Id == docid && d.Deleted == false)
                    .SingleOrDefault();

                if (doc?.Publication?.Publisher_id == null)
                    return Task.FromResult(1);

                conf = _context.emailconfig
                    .Where(c => c.Publisher.Id == doc.Publication.Publisher_id)
                    .SingleOrDefault();
            }

            string sHost      = "";
            int    nPort      = 587;
            string sFromName  = "";
            string sFromEmail = "";
            string sSvcUser   = "";
            string sSvcKey    = "";
            string sSubject   = "";
            string sMessage   = "";
            string? sTo       = "";
            string sCC        = "";
            string sBCC       = "";

            if (conf == null)
            {
                sHost      = Options.EmailHost      ?? "";
                nPort      = Options.EmailPort      ?? 587;
                sFromName  = Options.EmailName      ?? "";
                sFromEmail = Options.EmailFrom      ?? "";
                sSvcUser   = Options.EmailServiceUser ?? "";
                sSvcKey    = Options.EmailServiceKey  ?? "";
            }
            else
            {
                sHost      = conf.EmailHost;
                nPort      = conf.EmailPort;
                sFromName  = conf.EmailFromName;
                sFromEmail = conf.EmailFrom;
                sSvcUser   = conf.EmailServiceUser;
                sSvcKey    = conf.EmailServiceKey;
            }

            if (doc != null)
            {
                tpl = _context.emailmessage
                    .Where(m => m.Publication == doc.Publication && m.EmailType == emailtype)
                    .SingleOrDefault();
            }

            if (tpl == null)
            {
                tpl = _context.emailmessage
                    .Where(m => m.Publication == null && m.EmailType == emailtype)
                    .SingleOrDefault();
            }
            else
            {
                if (!string.IsNullOrEmpty(tpl.EmailFrom))
                    sFromEmail = tpl.EmailFrom;
            }

            if (!string.IsNullOrEmpty(username))
                sFromName = username;
            else if (!string.IsNullOrEmpty(tpl?.NameFrom))
                sFromName = tpl.NameFrom;

            if (!string.IsNullOrEmpty(subject))
                sSubject = subject;
            else if (!string.IsNullOrEmpty(tpl?.Subject))
                sSubject = tpl.Subject;

            if (!string.IsNullOrEmpty(message))
                sMessage = message;
            else if (!string.IsNullOrEmpty(tpl?.Message))
                sMessage = tpl.Message;

            sTo = to;
            if (string.IsNullOrEmpty(to))
                sTo = tpl?.CC;

            if (!string.IsNullOrEmpty(cc))
                sCC = cc;
            if (!string.IsNullOrEmpty(to) && !string.IsNullOrEmpty(tpl?.CC))
                sCC = tpl.CC;

            if (!string.IsNullOrEmpty(bcc))
                sBCC = bcc;
            else if (!string.IsNullOrEmpty(tpl?.BCC))
                sBCC = tpl.BCC;

            using var client = new SmtpClient();
            client.Connect(sHost, nPort, SecureSocketOptions.Auto);

            if (client.Capabilities.HasFlag(SmtpCapabilities.Authentication))
            {
                client.AuthenticationMechanisms.Remove("XOAUTH2");
                client.Authenticate(sSvcUser, sSvcKey);
            }

            var msg = new MimeMessage();
            msg.From.Add(new MailboxAddress(sFromName, sFromEmail));

            char[] delimiters = new[] { ',', ';', ' ' };
            string[]? emailarr = sTo?.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);
            if (emailarr != null)
                foreach (string eml in emailarr)
                    msg.To.Add(new MailboxAddress(eml, eml));

            if (!string.IsNullOrEmpty(sBCC))
            {
                emailarr = sBCC.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);
                foreach (string eml in emailarr)
                    msg.Bcc.Add(new MailboxAddress(eml, eml));
            }

            if (!string.IsNullOrEmpty(sCC))
            {
                emailarr = sCC.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);
                foreach (string eml in emailarr)
                    msg.Cc.Add(new MailboxAddress(eml, eml));
            }

            if (replacePattern != null)
            {
                foreach (var kvp in replacePattern)
                {
                    sSubject = sSubject.Replace(kvp.Key, kvp.Value, StringComparison.OrdinalIgnoreCase);
                    sMessage = sMessage.Replace(kvp.Key, kvp.Value, StringComparison.OrdinalIgnoreCase);
                }
            }

            msg.Subject = sSubject;

            var bodyBuilder = new BodyBuilder
            {
                HtmlBody = sMessage,
                TextBody = Regex.Replace(sMessage, "<.*?>", string.Empty)
            };
            msg.Body = bodyBuilder.ToMessageBody();

            client.Send(msg);
            client.Disconnect(true);
            return Task.FromResult(0);
        }

        // ---------------------------------------------------------------
        // ISmsSender — no-op (no Twilio in this host)
        // ---------------------------------------------------------------

        public Task<string?> SendSmsAsync(string number, string message)
            => Task.FromResult((string?)null);

        public Task<VerificationResult> StartVerificationAsync(string phoneNumber)
            => Task.FromResult(new VerificationResult(new List<string> { "SMS not supported" }));

        public Task<VerificationResult> CheckVerificationAsync(string phoneNumber, string code)
            => Task.FromResult(new VerificationResult(new List<string> { "SMS not supported" }));
    }
}
