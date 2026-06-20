using Amazon.SQS;
using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    public class ValidateIndexDocumentJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<ValidateIndexDocumentJobWorker> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IConfiguration _config;
        private readonly IAmazonSQS _sqs;
        private readonly IHostEnvironment _hostEnv;
        private readonly ILoggerFactory _loggerFactory;

        public ValidateIndexDocumentJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<ValidateIndexDocumentJobWorker> log,
            ISimpleEmailSender emailSender,
            IConfiguration config,
            IAmazonSQS sqs,
            IHostEnvironment hostEnv,
            ILoggerFactory loggerFactory)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _config = config;
            _sqs = sqs;
            _hostEnv = hostEnv;
            _loggerFactory = loggerFactory;
        }

        private sealed class HostEnvironmentBridge : Microsoft.AspNetCore.Hosting.IWebHostEnvironment
        {
            private readonly IHostEnvironment _inner;
            public HostEnvironmentBridge(IHostEnvironment e) => _inner = e;
            public string WebRootPath { get => ""; set { } }
            public IFileProvider WebRootFileProvider
                { get => new NullFileProvider(); set { } }
            public string EnvironmentName { get => _inner.EnvironmentName; set { } }
            public string ApplicationName { get => _inner.ApplicationName; set { } }
            public string ContentRootPath { get => _inner.ContentRootPath; set { } }
            public IFileProvider ContentRootFileProvider
                { get => _inner.ContentRootFileProvider; set { } }
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("ValidateIndexDocumentJobWorker job {JobId}", oJob.Id);

            var input = await ctx.indexdocumentinput
                .Include(x => x.Document)
                .Where(x => x.Job.Id == oJob.Id)
                .FirstOrDefaultAsync(ct);

            if (input == null)
            {
                _log.LogWarning("ValidateIndexDocumentJobWorker: no indexdocumentinput found for job {JobId}", oJob.Id);
                return false;
            }

            string documentId = input.Document.Id;

            ILogger<ProductImportJob> loggerForProductImport = _loggerFactory.CreateLogger<ProductImportJob>();
            var webEnv = new HostEnvironmentBridge(_hostEnv);
            var dcJob = new ValidateIndexDocumentJob(ctx, _emailSender, loggerForProductImport, _config, _sqs, webEnv);
            if (input.RegenerateText)
            {
                // Force a full regenerate + re-index. ExecuteJobAsync only acts when the doc has
                // zero OpenSearch hits, so an already-indexed doc with empty/stale page text would
                // never get fixed by a normal re-index. RegenerateText removes the existing entries
                // first, so the (V1) regenerate-text path runs and the doc is rebuilt.
                _log.LogInformation("ValidateIndexDocumentJobWorker: force re-index (RegenerateText) for {DocId}", documentId);
                dcJob.ForceReindex(documentId);
            }
            else
            {
                dcJob.ExecuteJobAsync(documentId);
            }

            return true;
        }
    }
}
