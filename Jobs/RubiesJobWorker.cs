using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace JobWorker.Jobs
{
    public class RubiesJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<RubiesJobWorker> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IConfiguration _config;

        public RubiesJobWorker(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<RubiesJobWorker> log, ISimpleEmailSender emailSender, IConfiguration config)
        { _dbFactory = dbFactory; _log = log; _emailSender = emailSender; _config = config; }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("RubiesJob {JobId}", oJob.Id);
            var dcJob = new RubiesJob(_config, ctx, _emailSender, _log as ILogger<RubiesJob> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RubiesJob>.Instance);
            await dcJob.ExecuteJobAsync();
            return true;
        }
    }
}
