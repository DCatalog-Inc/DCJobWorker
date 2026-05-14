using Amazon.SQS;
using core;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    public class HtmlGenerateJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<HtmlGenerateJobWorker> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IConfiguration _config;
        private readonly IAmazonSQS _sqs;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ILoggerFactory _loggerFactory;

        public HtmlGenerateJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<HtmlGenerateJobWorker> log,
            ISimpleEmailSender emailSender,
            IConfiguration config,
            IAmazonSQS sqs,
            IServiceScopeFactory serviceScopeFactory,
            ILoggerFactory loggerFactory)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _config = config;
            _sqs = sqs;
            _serviceScopeFactory = serviceScopeFactory;
            _loggerFactory = loggerFactory;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("HtmlGenerateJobWorker job {JobId}", oJob.Id);

            var input = await ctx.createdocumentxmlinput
                .Include(x => x.Document)
                .Include(x => x.Document.Publication)
                .Include(x => x.Document.Publication.Publisher)
                .Where(x => x.Job.Id == oJob.Id)
                .FirstOrDefaultAsync(ct);

            if (input == null)
            {
                _log.LogWarning("HtmlGenerateJobWorker: no createdocumentxmlinput found for job {JobId}", oJob.Id);
                return false;
            }

            string sBucketName = input.Document?.Publication?.Publisher?.BucketName
                ?? Core.Constants.DEFAULT_DOCS_LOCATION;
            bool updateAll = false;

            ILogger<ProductImportJob> loggerForProductImport = _loggerFactory.CreateLogger<ProductImportJob>();
            var dcJob = new HtmlGenerateJob(ctx, _emailSender, loggerForProductImport, _config, _sqs, _serviceScopeFactory);
            await dcJob.ExecuteJobAsync(oJob.Id, sBucketName, updateAll);

            return true;
        }
    }
}
