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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Removes page(s) from a document. DCPageManager.DelPage already performs the full flow
    // (delete pages from PDF, RenameFilesDelete, document.json shift, upload, re-index,
    // regenerate text, set Completed + save), so this handler just loads the input and delegates.
    public class JobExecutionRemovePages : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionRemovePages> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IAmazonSQS _sqs;

        public JobExecutionRemovePages(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<JobExecutionRemovePages> log,
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
            _log.LogInformation("JobExecutionRemovePages job {JobId}", oJob.Id);
            // oJob is tracked by the CALLER's context — attaching it here conflicts with the
            // same job row our Include query tracks. Write through jobRow; mirror onto oJob.
            job jobRow = null;
            try
            {
                var input = await context.deletepagesinput
                    .Include(r => r.Job)
                    .Include(r => r.Document)
                    .Where(r => r.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                jobRow = input?.Job ?? await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                if (input == null || input.Document == null)
                {
                    await FailAsync(context, jobRow, oJob, "No deletepagesinput/document for job", ct);
                    return false;
                }

                // Serialize page-ops on this document across workers (prevents same-doc file-in-use
                // collisions; the job-level atomic claim only covers same-JOB processing).
                await using var docLock = await DocumentLock.AcquireAsync(context, input.Document.Id, 600, ct);
                if (!docLock.Acquired)
                {
                    await FailAsync(context, jobRow, oJob, "Could not acquire document lock (another operation on this document is in progress)", ct);
                    return false;
                }

                var mgr = new DCPageManager(context, _emailSender,
                    NullLogger<ProductImportJob>.Instance, _sqs);

                bool ok = mgr.DelPage(input.Id);
                if (!ok)
                {
                    await FailAsync(context, jobRow, oJob, "Remove pages failed (see worker logs)", ct);
                    return false;
                }
                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "RemovePages failed for job {JobId}", oJob.Id);
                try
                {
                    jobRow ??= await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                    await FailAsync(context, jobRow, oJob, "Remove pages failed: " + e.Message, ct);
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
