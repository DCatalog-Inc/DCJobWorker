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
            try
            {
                var input = await context.deletepagesinput
                    .Include(r => r.Job)
                    .Where(r => r.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                if (input == null)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "No deletepagesinput for job";
                    context.Update(oJob);
                    await context.SaveChangesAsync(ct);
                    return false;
                }

                var mgr = new DCPageManager(context, _emailSender,
                    NullLogger<ProductImportJob>.Instance, _sqs);

                bool ok = mgr.DelPage(input.Id);
                if (!ok)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "Remove pages failed (see worker logs)";
                    context.Update(oJob);
                    await context.SaveChangesAsync(ct);
                    return false;
                }
                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "RemovePages failed for job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Remove pages failed: " + e.Message;
                try { context.Update(oJob); await context.SaveChangesAsync(ct); } catch { }
                return false;
            }
        }
    }
}
