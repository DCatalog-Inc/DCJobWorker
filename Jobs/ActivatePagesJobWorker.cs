using Amazon.SQS;
using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    public class ActivatePagesJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<ActivatePagesJobWorker> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IAmazonSQS _sqsClient;

        public ActivatePagesJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<ActivatePagesJobWorker> log,
            ISimpleEmailSender emailSender,
            IAmazonSQS sqsClient)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _sqsClient = sqsClient;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            _log.LogInformation("ActivatePagesJobWorker job {JobId}", oJob.Id);

            await using var context = await _dbFactory.CreateDbContextAsync(ct);

            var oReplacePageInput = await context.replacepageinput
                .Include(r => r.Job)
                .Where(r => r.Job.Id == oJob.Id)
                .FirstOrDefaultAsync(ct);

            if (oReplacePageInput == null)
            {
                _log.LogError("ActivatePagesJobWorker: no replacepageinput found for job {JobId}", oJob.Id);
                return false;
            }

            var dcJob = new ActivatePagesJob(
                context,
                _emailSender,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ProductImportJob>.Instance,
                _sqsClient);

            await dcJob.ExecuteJobAsync(oReplacePageInput.Id);

            return true;
        }
    }
}
