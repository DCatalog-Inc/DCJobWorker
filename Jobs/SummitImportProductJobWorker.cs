using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Amazon.SQS;

namespace JobWorker.Jobs
{
    public class SummitImportProductJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<SummitImportProductJobWorker> _log;
        private readonly IEmailSender _emailSender;
        private readonly IConfiguration _config;
        private readonly IAmazonSQS _sqs;

        public SummitImportProductJobWorker(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<SummitImportProductJobWorker> log, IEmailSender emailSender, IConfiguration config, IAmazonSQS sqs)
        { _dbFactory = dbFactory; _log = log; _emailSender = emailSender; _config = config; _sqs = sqs; }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("SummitImportProductJob {JobId}", oJob.Id);
            var dcJob = new SummitImportProductJob(ctx, _emailSender, _log as ILogger<SummitImportProductJob> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<SummitImportProductJob>.Instance, _config, _sqs);
            await dcJob.ExecuteJobAsync();
            return true;
        }
    }
}
