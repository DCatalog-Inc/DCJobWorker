using Amazon.SQS;
using core;
using Core;
using Core.Models;
using DCatalogCommon;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Fallback for job types this worker has not ported yet (e.g.
    // JobExecutionIntroPage): reset the job to Waiting and forward it to the
    // legacy HP queue so the old Windows DocProcessor handles it. The status
    // reset is essential — JobProcessor moved the job to Processing before
    // dispatch, and the legacy worker only picks up Waiting/WaitingInQueue
    // (the old ActivatePagesJobWorker forward was dead because of exactly
    // that). JobProcessor sees the job back in Waiting and skips its own
    // Completed stamp.
    public class ForwardToLegacyQueue : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<ForwardToLegacyQueue> _log;
        private readonly IAmazonSQS _sqs;

        public ForwardToLegacyQueue(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<ForwardToLegacyQueue> log,
            IAmazonSQS sqs)
        {
            _dbFactory = dbFactory;
            _log = log;
            _sqs = sqs;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);

            var jobRow = await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
            if (jobRow == null)
            {
                _log.LogError("ForwardToLegacyQueue: job {JobId} not found", oJob.Id);
                return false;
            }

            jobRow.Status = Constants.JobProcessingStatus.Waiting.ToString();
            jobRow.Progress = 0;
            jobRow.ProcessedBy = "LegacyDocProcessor";
            await context.SaveChangesAsync(ct);

            var oDCSQS = new DCSQS(context, _sqs);
            await oDCSQS.addJobToQueue(jobRow, Constants.JobQueueName.DistributedHPClientQueue);

            // Mirror so JobProcessor's completion logic sees the requeue.
            oJob.Status = jobRow.Status;

            _log.LogInformation(
                "Forwarded unported job type {Type} (job {JobId}) to the legacy HP queue",
                oJob.JobType?.Name, oJob.Id);

            return true;
        }
    }
}
