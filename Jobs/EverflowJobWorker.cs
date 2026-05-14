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
    public class EverflowJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<EverflowJobWorker> _log;
        private readonly IEmailSender _emailSender;
        private readonly IConfiguration _config;
        private readonly IAmazonSQS _sqs;

        public EverflowJobWorker(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<EverflowJobWorker> log, IEmailSender emailSender, IConfiguration config, IAmazonSQS sqs)
        { _dbFactory = dbFactory; _log = log; _emailSender = emailSender; _config = config; _sqs = sqs; }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("EverflowJob {JobId}", oJob.Id);
            var dcJob = new EverflowJob(ctx, _emailSender, _log as ILogger<EverflowJob> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<EverflowJob>.Instance, _config, _sqs);
            await dcJob.addProductsEverflowPrivate();
            return true;
        }
    }
}
