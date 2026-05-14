using Amazon.SQS;
using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    public class GeneratePDFJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<GeneratePDFJobWorker> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IConfiguration _configuration;
        private readonly IAmazonSQS _sqsClient;

        public GeneratePDFJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<GeneratePDFJobWorker> log,
            ISimpleEmailSender emailSender,
            IConfiguration configuration,
            IAmazonSQS sqsClient)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _configuration = configuration;
            _sqsClient = sqsClient;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            _log.LogInformation("GeneratePDFJobWorker job {JobId}", oJob.Id);

            await using var context = await _dbFactory.CreateDbContextAsync(ct);

            var input = await context.createdocumentxmlinput
                .Include(c => c.Document)
                .Where(c => c.Job.Id == oJob.Id)
                .FirstOrDefaultAsync(ct);

            if (input == null)
            {
                _log.LogError("GeneratePDFJobWorker: no createdocumentxmlinput found for job {JobId}", oJob.Id);
                return false;
            }

            string documentId = input.Document.Id;

            var dcJobLogger = _log as ILogger<GeneratePDFJob>
                ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<GeneratePDFJob>.Instance;

            var dcJob = new GeneratePDFJob(context, _emailSender, dcJobLogger, _configuration, _sqsClient);
            await dcJob.SavePageLabelsAsync(oJob.Id, documentId);

            return true;
        }
    }
}
