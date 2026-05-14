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
    public class ActivateEditionsJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<ActivateEditionsJobWorker> _log;
        private readonly IEmailSender _emailSender;
        private readonly ISmsSender _smsSender;

        public ActivateEditionsJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<ActivateEditionsJobWorker> log,
            IEmailSender emailSender,
            ISmsSender smsSender)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _smsSender = smsSender;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            _log.LogInformation("ActivateEditionsJobWorker job {JobId}", oJob.Id);

            if (string.IsNullOrEmpty(oJob.Desctiption))
            {
                _log.LogError("ActivateEditionsJobWorker: job {JobId} has null/empty Description", oJob.Id);
                return false;
            }

            string sch_id;
            bool activate;
            try
            {
                var p = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(oJob.Desctiption);
                sch_id = (string)p.sch_id;
                activate = (bool)p.Activate;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "ActivateEditionsJobWorker: failed to parse Description for job {JobId}", oJob.Id);
                return false;
            }

            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            var dcJob = new ActivateEditionsJob(context, _emailSender, _smsSender, _log as ILogger<ProductImportJob>
                ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<ProductImportJob>.Instance);
            await dcJob.ExecuteJobAsync(sch_id, activate);

            return true;
        }
    }
}
