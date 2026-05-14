using core;
using Core.Models;
using core.Models;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    public class AddToAILibraryJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<AddToAILibraryJobWorker> _log;

        public AddToAILibraryJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<AddToAILibraryJobWorker> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            _log.LogInformation("AddToAILibraryJobWorker job {JobId}", oJob.Id);

            await using var context = await _dbFactory.CreateDbContextAsync(ct);

            var input = await context.addaiinput
                .Include(a => a.Document)
                .Where(a => a.Job.Id == oJob.Id)
                .FirstOrDefaultAsync(ct);

            if (input == null)
            {
                _log.LogError("AddToAILibraryJobWorker: no addaiinput found for job {JobId}", oJob.Id);
                return false;
            }

            string documentId = input.Document.Id;
            int addType = input.AddType;

            var dcJobLogger = _log as ILogger<AddToAILibraryJob>
                ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<AddToAILibraryJob>.Instance;

            var dcJob = new AddToAILibraryJob(context, dcJobLogger);
            await dcJob.ExecuteAsync(oJob.Id, documentId, addType);

            return true;
        }
    }
}
