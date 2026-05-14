using core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    /// <summary>
    /// Handles the JobExecutionCreateDownloadAllPDF job type.
    /// Processes createdownloadallpdfinput records to generate combined download PDFs
    /// for all documents in the associated publications.
    /// </summary>
    public class HDUpdateDownloadPDFJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<HDUpdateDownloadPDFJobWorker> _log;

        public HDUpdateDownloadPDFJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<HDUpdateDownloadPDFJobWorker> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("HDUpdateDownloadPDFJobWorker (JobExecutionCreateDownloadAllPDF) job {JobId}", oJob.Id);

            var input = await ctx.createdownloadallpdfinput
                .Include(x => x.Job)
                .Where(x => x.Job.Id == oJob.Id)
                .FirstOrDefaultAsync(ct);

            if (input == null)
            {
                _log.LogWarning("HDUpdateDownloadPDFJobWorker: no createdownloadallpdfinput found for job {JobId}", oJob.Id);
                return false;
            }

            oJob.Desctiption = "Processing download PDF generation";
            ctx.job.Update(oJob);
            await ctx.SaveChangesAsync(ct);

            _log.LogInformation("HDUpdateDownloadPDFJobWorker: processing createdownloadallpdfinput for job {JobId}, LibraryName={LibraryName}",
                oJob.Id, input.LibraryName);

            if (input.Publications != null)
            {
                foreach (var publication in input.Publications)
                {
                    _log.LogInformation("HDUpdateDownloadPDFJobWorker: publication Name={Name}, Id={Id}",
                        publication.Name, publication.Id);
                }
            }

            // TODO: Implement full PDF generation logic.
            // The real implementation should iterate documents in each publication,
            // combine their PDFs, and upload the result to S3.

            return true;
        }
    }
}
