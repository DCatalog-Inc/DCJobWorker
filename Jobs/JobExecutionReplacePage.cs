using Amazon.SQS;
using core;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Replaces page(s) in an existing document. The full flow (download existing PDF, swap the
    // page, regenerate images/SVG/HTML, update links, re-index, upload, persist status) lives in
    // DCPageManager.ReplacePages.
    //
    // This job type used to map to ActivatePagesJobWorker, which only re-enqueued the job to the
    // legacy HP queue — but JobProcessor had already moved the job out of "Waiting", so the legacy
    // DocProcessor's status guard skipped the forwarded message and the page was never replaced.
    //
    // IMPORTANT: oJob is tracked by the CALLER's DbContext (JobProcessor), not ours — all DB
    // writes go through jobRow (our tracked instance); oJob only gets in-memory mirrors so the
    // processor reports correctly. (Same pattern as JobExecutionAddPages.)
    public class JobExecutionReplacePage : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionReplacePage> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IAmazonSQS _sqs;

        public JobExecutionReplacePage(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<JobExecutionReplacePage> log,
            ISimpleEmailSender emailSender,
            IAmazonSQS sqs)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _sqs = sqs;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JobExecutionReplacePage job {JobId}", oJob.Id);
            job jobRow = null;
            try
            {
                var input = await context.replacepageinput
                    .Include(r => r.Document)
                    .Include(r => r.Document.Publication)
                    .Include(r => r.Document.Publication.Publisher)
                    .Include(r => r.Document.Publication.PublicationTemplate)
                    .Include(r => r.Job)
                    .Where(r => r.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                jobRow = input?.Job ?? await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                if (input == null || input.Document == null)
                {
                    await FailAsync(context, jobRow, oJob, "No replacepageinput/document for job", ct);
                    return false;
                }

                // Serialize page-ops on this document across workers. Two jobs touching the same
                // doc's files concurrently caused "The process cannot access the file ... because it
                // is being used by another process" (the job-level atomic claim only covers same-JOB).
                await using var docLock = await DocumentLock.AcquireAsync(context, input.Document.Id, 600, ct);
                if (!docLock.Acquired)
                {
                    await FailAsync(context, jobRow, oJob, "Could not acquire document lock (another operation on this document is in progress)", ct);
                    return false;
                }

                var mgr = new DCPageManager(context, _emailSender,
                    NullLogger<ProductImportJob>.Instance, _sqs);

                // Full replace flow; writes Completed (or Failed + reason) onto the job row itself.
                await mgr.ReplacePages(input.Id);

                if (jobRow.Status == Constants.JobProcessingStatus.Failed.ToString())
                {
                    _log.LogError("ReplacePage job {JobId} failed: {Reason}", oJob.Id, jobRow.Desctiption);
                    // Mirror so JobProcessor keeps the detailed reason instead of stamping Completed.
                    oJob.Status = jobRow.Status;
                    oJob.Desctiption = jobRow.Desctiption;
                    return false;
                }

                // Page swapped — now refresh search. ReplacePages' indexPageInDocument calls
                // core ElasticSearchEngine.addPage, which is a stub (indexing body commented
                // out), and nothing uploads the regenerated html/ artifacts SearchHighlight
                // reads from S3. Finalize like JobExecutionAddPages: regenerate page text
                // (dcmutool), upload the document dir, then full re-index.
                document oDocument = input.Document;
                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
                PDF2HTML.convertAllPagesText(sPDFFileName, sDocumentPath, "html");
                mgr.updateDocument(oDocument);
                DCJobs.DocumentConvertor.indexDocument(context, oDocument);
                _log.LogInformation("ReplacePage job {JobId}: re-indexed document {DocId}", oJob.Id, oDocument.Id);

                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "ReplacePage failed for job {JobId}", oJob.Id);
                try
                {
                    jobRow ??= await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                    await FailAsync(context, jobRow, oJob, "Replace page failed: " + e.Message, ct);
                }
                catch { /* JobProcessor marks the job Failed via its own context */ }
                return false;
            }
        }

        // Persists Failed on OUR tracked job row, then mirrors onto the caller's instance so
        // JobProcessor keeps the detailed message instead of overwriting with a generic one.
        private static async Task FailAsync(ApplicationDbContext context, job jobRow, job oJob,
            string reason, CancellationToken ct)
        {
            if (reason.Length > 512) reason = reason.Substring(0, 512);
            if (jobRow != null)
            {
                jobRow.Status = Constants.JobProcessingStatus.Failed.ToString();
                jobRow.Desctiption = reason;
                await context.SaveChangesAsync(ct);
                oJob.Status = jobRow.Status;
                oJob.Desctiption = jobRow.Desctiption;
            }
        }
    }
}
