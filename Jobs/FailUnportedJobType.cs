using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Fallback for job types this worker has not ported. This USED to forward such jobs to the
    // legacy Windows DocProcessor; that service is being decommissioned, so forwarding now parks
    // the job on a queue with no consumer — a silent orphan that sits in Waiting forever.
    //
    // Instead we fail the job with a clear, greppable reason so an unported type surfaces as a
    // visible failure (and tells us exactly which handler still needs porting) rather than
    // vanishing. The remaining live unported types as of the 2026-06 audit are
    // JobExecutionConvertPDFFromFeeds, JobExecutionCreateImages, and JobBackgroundImportCSV.
    public class FailUnportedJobType : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<FailUnportedJobType> _log;

        public FailUnportedJobType(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<FailUnportedJobType> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            string typeName = oJob.JobType?.Name ?? "(unknown)";
            string reason = "Unsupported job type '" + typeName +
                "' — not ported to the SQS worker and the legacy DocProcessor is decommissioned.";

            var jobRow = await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
            if (jobRow != null)
            {
                jobRow.Status = Constants.JobProcessingStatus.Failed.ToString();
                jobRow.Desctiption = reason.Length > 512 ? reason.Substring(0, 512) : reason;
                await context.SaveChangesAsync(ct);
                oJob.Status = jobRow.Status;
                oJob.Desctiption = jobRow.Desctiption;
            }

            _log.LogError("FailUnportedJobType: failed job {JobId} of unported type {Type}", oJob.Id, typeName);
            return false;
        }
    }
}
