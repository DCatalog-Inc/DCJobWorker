using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;

namespace JobWorker.Jobs
{
    public class CCMPJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<CCMPJobWorker> _log;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ISimpleEmailSender _emailSender;

        public CCMPJobWorker(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<CCMPJobWorker> log, ILoggerFactory loggerFactory, ISimpleEmailSender emailSender)
        { _dbFactory = dbFactory; _log = log; _loggerFactory = loggerFactory; _emailSender = emailSender; }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("CCMPJob {JobId}", oJob.Id);
            var dcJob = new CCMPJob(ctx, _emailSender, _loggerFactory.CreateLogger<CCMPJob>());
            dcJob.ExecuteJob();
            return true;
        }
    }
}
