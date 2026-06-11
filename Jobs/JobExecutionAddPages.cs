using Amazon.SQS;
using core;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Adds page(s) to an existing document. The page-manipulation logic lives in
    // DCPageManager.AddPages (PDF merge + RenameFiles + document.json shift + selectable text);
    // this handler runs it then finalizes like DCPageManager.DelPage does (upload dir, crc,
    // re-index, persist).
    //
    // IMPORTANT: oJob is tracked by the CALLER's DbContext (JobProcessor), not ours — attaching
    // it here via context.Update(oJob) throws "instance ... already being tracked" because our
    // input query Includes the same job row. All DB writes go through jobRow (our tracked
    // instance); oJob only gets in-memory mirrors so the processor reports correctly.
    public class JobExecutionAddPages : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionAddPages> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IAmazonSQS _sqs;

        public JobExecutionAddPages(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<JobExecutionAddPages> log,
            ISimpleEmailSender emailSender,
            IAmazonSQS sqs)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _sqs = sqs;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JobExecutionAddPages job {JobId}", oJob.Id);
            job jobRow = null;
            try
            {
                var input = await context.addpagesinput
                    .Include(r => r.Document)
                    .Include(r => r.Document.Publication)
                    .Include(r => r.Document.Publication.Publisher)
                    .Include(r => r.Document.Publication.PublicationTemplate)
                    .Include(r => r.Job)
                    .Where(r => r.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                jobRow = input?.Job ?? await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                if (input == null || input.Document == null)
                {
                    await FailAsync(context, jobRow, oJob, "No addpagesinput/document for job", ct);
                    return false;
                }

                var mgr = new DCPageManager(context, _emailSender,
                    NullLogger<ProductImportJob>.Instance, _sqs);

                // Downloads the existing doc, merges the PDF, shifts files, regenerates HTML/images,
                // and updates input.Document.NumberOfPages (same tracked instance). Reports progress
                // up to ~75 via the job row. Throws on failure (caught below -> Failed).
                mgr.AddPages(input.Id);

                document oDocument = input.Document;
                // Upload the regenerated document directory back to S3.
                mgr.updateDocument(oDocument);
                jobRow.Progress = 90; jobRow.Status = Constants.JobProcessingStatus.Processing.ToString();
                await context.SaveChangesAsync(ct);   // uploaded

                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
                if (File.Exists(sPDFFileName))
                    oDocument.crc32 = DCPageManager.GetCrc32(sPDFFileName);

                DCJobs.DocumentConvertor.indexDocument(context, oDocument);
                jobRow.Progress = 97; await context.SaveChangesAsync(ct);   // re-indexed
                // NOTE: AddPages already regenerated the page HTML (convertAllPagesMuPDF) which
                // updateDocument uploaded and indexDocument indexed. Calling createTextFiles here would
                // re-render the whole document a second time, unused — removed (matches DelPage).

                jobRow.Progress = 100;
                jobRow.Status = Constants.JobProcessingStatus.Completed.ToString();
                await context.SaveChangesAsync(ct);
                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "AddPages failed for job {JobId}", oJob.Id);
                try
                {
                    jobRow ??= await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                    await FailAsync(context, jobRow, oJob, "Add pages failed: " + e.Message, ct);
                }
                catch { /* JobProcessor marks the job Failed via its own context */ }
                return false;
            }
        }

        // Persists Failed on OUR tracked job row, then mirrors onto the caller's instance so
        // JobProcessor keeps the detailed message instead of overwriting with a generic one.
        private static async Task FailAsync(ApplicationDbContext context, job jobRow, job oJob,
            string reason, CancellationToken ct)
        {
            if (reason.Length > 512) reason = reason.Substring(0, 512);
            if (jobRow != null)
            {
                jobRow.Status = Constants.JobProcessingStatus.Failed.ToString();
                jobRow.Desctiption = reason;
                await context.SaveChangesAsync(ct);
                oJob.Status = jobRow.Status;
                oJob.Desctiption = jobRow.Desctiption;
            }
        }
    }
}
