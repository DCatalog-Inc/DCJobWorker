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
    // re-index, regenerate text, persist).
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
                if (input == null || input.Document == null)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "No addpagesinput/document for job";
                    context.Update(oJob);
                    await context.SaveChangesAsync(ct);
                    return false;
                }

                var mgr = new DCPageManager(context, _emailSender,
                    NullLogger<ProductImportJob>.Instance, _sqs);

                // Modifies the local document files + input.Document.NumberOfPages (same tracked instance).
                mgr.AddPages(input.Id);

                document oDocument = input.Document;
                // Upload the regenerated document directory back to S3.
                mgr.updateDocument(oDocument);

                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
                if (File.Exists(sPDFFileName))
                    oDocument.crc32 = DCPageManager.GetCrc32(sPDFFileName);

                DCJobs.DocumentConvertor.indexDocument(context, oDocument);
                DCJobs.DocumentConvertor.createTextFiles(context, oDocument).GetAwaiter().GetResult();

                oJob.Progress = 100;
                oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                context.Update(oJob);
                context.Update(oDocument);
                await context.SaveChangesAsync(ct);
                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "AddPages failed for job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Add pages failed: " + e.Message;
                try { context.Update(oJob); await context.SaveChangesAsync(ct); } catch { }
                return false;
            }
        }
    }
}
