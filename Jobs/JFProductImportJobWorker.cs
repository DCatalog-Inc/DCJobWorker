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
    public class JFProductImportJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JFProductImportJobWorker> _log;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IConfiguration _config;
        private readonly IAmazonSQS _sqs;

        public JFProductImportJobWorker(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<JFProductImportJobWorker> log, ILoggerFactory loggerFactory, ISimpleEmailSender emailSender, IConfiguration config, IAmazonSQS sqs)
        { _dbFactory = dbFactory; _log = log; _loggerFactory = loggerFactory; _emailSender = emailSender; _config = config; _sqs = sqs; }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JFProductImportJob {JobId}", oJob.Id);
            var dcJob = new JFProductImportJob(ctx, _emailSender, _loggerFactory.CreateLogger<RubiesJob>(), _config, _sqs);
            await dcJob.ExecuteJobAsync();
            return true;
        }
    }
}
