using core;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Amazon.SQS;

namespace JobWorker.Jobs
{
    public class CeratizitJobWorker : IJobExecution
    {
        private readonly ILogger<CeratizitJobWorker> _log;
        private readonly IEmailSender _emailSender;
        private readonly IConfiguration _config;
        private readonly IAmazonSQS _sqs;

        public CeratizitJobWorker(ILogger<CeratizitJobWorker> log, IEmailSender emailSender, IConfiguration config, IAmazonSQS sqs)
        { _log = log; _emailSender = emailSender; _config = config; _sqs = sqs; }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            _log.LogInformation("CeratizitJob {JobId}", oJob.Id);
            var dbFactory = new AppDbFactory(DCCommon.Instance.DefaultDBConnection);
            var dcJob = new CeratizitJob(dbFactory, _emailSender, _log as ILogger<EverflowJob> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<EverflowJob>.Instance, _config, _sqs);
            await dcJob.ExecuteJobAsync();
            return true;
        }
    }
}
