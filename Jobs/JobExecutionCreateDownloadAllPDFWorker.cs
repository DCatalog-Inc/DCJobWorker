using Amazon.SQS;
using core;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Builds the single "download all" PDF for a library: gather every published document across
    // the input publication(s), download each PDF from S3, merge them into one file with dcmutool,
    // upload the merged file, and upsert the downloadallpdf row (keyed by LibraryName) the viewer
    // serves. Native port of the legacy WebPublisher.JobExecutionCreateDownloadAllPDF — this job
    // type used to forward to the legacy Windows DocProcessor; once that service is gone the
    // forward is a dead end, so it runs here.
    public class JobExecutionCreateDownloadAllPDFWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionCreateDownloadAllPDFWorker> _log;

        public JobExecutionCreateDownloadAllPDFWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<JobExecutionCreateDownloadAllPDFWorker> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JobExecutionCreateDownloadAllPDF job {JobId}", oJob.Id);
            job jobRow = null;
            string sWorkDir = null;
            try
            {
                var input = await context.createdownloadallpdfinput
                    .Include(r => r.Job)
                    .Include(r => r.Publications).ThenInclude(p => p.Publisher)
                    .Where(r => r.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                jobRow = input?.Job ?? await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                if (input == null || input.Publications == null || input.Publications.Count == 0)
                {
                    await FailAsync(context, jobRow, oJob, "No createdownloadallpdfinput/publications for job", ct);
                    return false;
                }

                string sCompleted = Constants.JobProcessingStatus.Completed.ToString();
                publisher oPublisher = input.Publications[0].Publisher;

                // Gather every published, archive-visible document across the input publication(s)
                // (matches the legacy PublicationUtil.getDocumentsList filter), then order by title.
                var allDocs = new List<document>();
                foreach (publication p in input.Publications)
                {
                    string pubId = p.Id;
                    var docs = await context.document
                        .Include(d => d.Publication).ThenInclude(pp => pp.Publisher)
                        .Where(d => d.IsActive && d.ShowInArchive && !d.Deleted
                            && d.Publication.Id == pubId && d.DocumentStatus == sCompleted)
                        .ToListAsync(ct);
                    allDocs.AddRange(docs);
                }
                allDocs.Sort((x, y) =>
                {
                    if (x.Title == null && y.Title == null) return 0;
                    if (x.Title == null) return -1;
                    if (y.Title == null) return 1;
                    return string.Compare(x.Title, y.Title, StringComparison.Ordinal);
                });

                if (allDocs.Count == 0)
                {
                    await FailAsync(context, jobRow, oJob, "No completed documents to merge for this library", ct);
                    return false;
                }

                jobRow.Progress = 10;
                jobRow.Status = Constants.JobProcessingStatus.Processing.ToString();
                await context.SaveChangesAsync(ct);

                sWorkDir = Path.Combine(Path.GetTempPath(), "dcdownloadall", Guid.NewGuid().ToString());
                Directory.CreateDirectory(sWorkDir);
                var oDCS3Services = new DCS3Services();
                string sBucketName = Constants.DEFAULT_DOCS_LOCATION;

                // 1) Download each document's PDF from S3.
                var downloadedPdfs = new List<string>();
                foreach (document oDocument in allDocs)
                {
                    if (string.IsNullOrEmpty(oDocument.PDFFileName)) continue;
                    string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
                    string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
                    string sKey = string.Format("{0}/{1}/{2}/{3}", sPublisherName, sPublicationName,
                        oDocument.Id, oDocument.PDFFileName);
                    string sLocalPdf = Path.Combine(sWorkDir, Guid.NewGuid().ToString() + ".pdf");
                    if (oDCS3Services.downloadFile(sBucketName, sKey, sLocalPdf) && File.Exists(sLocalPdf))
                        downloadedPdfs.Add(sLocalPdf);
                    else
                        _log.LogWarning("CreateDownloadAllPDF job {JobId}: missing PDF {Key} (skipped)", oJob.Id, sKey);
                }
                if (downloadedPdfs.Count == 0)
                {
                    await FailAsync(context, jobRow, oJob, "Could not download any source PDFs from S3", ct);
                    return false;
                }
                jobRow.Progress = 60; await context.SaveChangesAsync(ct);

                // 2) Merge into a single PDF via dcmutool.
                string sMergedLocal = Path.Combine(sWorkDir, Guid.NewGuid().ToString() + ".pdf");
                if (!MergePdf(sMergedLocal, downloadedPdfs) || !File.Exists(sMergedLocal))
                {
                    await FailAsync(context, jobRow, oJob, "dcmutool merge produced no output", ct);
                    return false;
                }
                jobRow.Progress = 85; await context.SaveChangesAsync(ct);

                // 3) Upload the merged PDF.
                string sOutputDirectoryS3 = oPublisher.Name + "/" + "MergedPDF";
                string sMergedUrl = oDCS3Services.uploadFile(sBucketName, sMergedLocal, sOutputDirectoryS3);

                // 4) Upsert the downloadallpdf row the viewer serves (keyed by LibraryName).
                string sLibraryName = (input.LibraryName ?? string.Empty).ToLower();
                var oDownloadallpdf = await context.downloadallpdf
                    .FirstOrDefaultAsync(d => d.LibraryName == sLibraryName, ct);
                if (oDownloadallpdf == null)
                {
                    oDownloadallpdf = new downloadallpdf { LibraryName = sLibraryName };
                    context.downloadallpdf.Add(oDownloadallpdf);
                }
                oDownloadallpdf.PDFUrl = sMergedUrl;
                oDownloadallpdf.LastModified = DateTime.Now;

                jobRow.Progress = 100;
                jobRow.Status = Constants.JobProcessingStatus.Completed.ToString();
                await context.SaveChangesAsync(ct);
                _log.LogInformation("CreateDownloadAllPDF job {JobId}: merged {Count} PDFs for library '{Lib}' -> {Url}",
                    oJob.Id, downloadedPdfs.Count, sLibraryName, sMergedUrl);
                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "CreateDownloadAllPDF failed for job {JobId}", oJob.Id);
                try
                {
                    jobRow ??= await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                    await FailAsync(context, jobRow, oJob, "Create download all PDF failed: " + e.Message, ct);
                }
                catch { /* JobProcessor marks the job Failed via its own context */ }
                return false;
            }
            finally
            {
                if (sWorkDir != null)
                    try { Directory.Delete(sWorkDir, true); } catch { }
            }
        }

        private static bool MergePdf(string sOutputName, IList<string> inputFiles)
        {
            string exePath = Path.Combine(AppContext.BaseDirectory, "Tools", "dcmutool", "dcmutool.exe");
            var args = new System.Text.StringBuilder();
            args.AppendFormat("merge -o \"{0}\"", sOutputName);
            foreach (string f in inputFiles) args.AppendFormat(" \"{0}\"", f);
            var cmdsi = new ProcessStartInfo(exePath)
            {
                Arguments = args.ToString(),
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            return cmd.ExitCode == 0;
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
