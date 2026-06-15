using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Regenerates the v2 page images for a document from its PDF. Native port of legacy
    // WebPublisher.JobExecutionCreateImages: download the PDF + document.json, render every page to
    // ZPage_*.jpg (dcmutool, 144 dpi), rename each to Page_<guid>.jpg, capture dimensions, build
    // optional thumbnails, write the result JSON onto the generateimagesinput row, and upload the
    // document directory to <pub>/<publ>/<docId>/v2. Used by the v2 editor flow (EditorApi
    // GenerateImages) and HtmlGenerate. Runs on Windows so System.Drawing is available.
    public class JobExecutionCreateImagesWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionCreateImagesWorker> _log;

        public JobExecutionCreateImagesWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<JobExecutionCreateImagesWorker> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JobExecutionCreateImages job {JobId}", oJob.Id);
            job jobRow = null;
            try
            {
                var input = await context.generateimagesinput
                    .Include(r => r.Job)
                    .Include(r => r.Document).ThenInclude(d => d.Publication).ThenInclude(p => p.Publisher)
                    .Where(r => r.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                jobRow = input?.Job ?? await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                if (input == null || input.Document == null)
                {
                    await FailAsync(context, jobRow, oJob, "No generateimagesinput/document for job", ct);
                    return false;
                }

                // Clears and rewrites the shared document dir — serialize against concurrent
                // page-ops/convert on the same document (same lock the page-op handlers use).
                await using var docLock = await DocumentLock.AcquireAsync(context, input.Document.Id, 600, ct);
                if (!docLock.Acquired)
                {
                    await FailAsync(context, jobRow, oJob, "Could not acquire document lock (another operation on this document is in progress)", ct);
                    return false;
                }

                document oDocument = input.Document;
                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                string sLocalPdfName = input.InputFileName;
                string sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName)
                    ? Constants.DEFAULT_DOCS_LOCATION
                    : oDocument.Publication.Publisher.BucketName;

                jobRow.Progress = 10;
                jobRow.Status = Constants.JobProcessingStatus.Processing.ToString();
                await context.SaveChangesAsync(ct);

                // 1) Fresh working dir, download the PDF + document.json.
                if (Directory.Exists(sDocumentPath)) Directory.Delete(sDocumentPath, true);
                Directory.CreateDirectory(sDocumentPath);
                var oDCS3Services = new DCS3Services();
                string sLocalPdfPath = Path.Combine(sDocumentPath, sLocalPdfName);
                if (!oDCS3Services.downloadFile(sBucketName, input.filename, sLocalPdfPath) || !File.Exists(sLocalPdfPath))
                {
                    await FailAsync(context, jobRow, oJob, "Cannot find the PDF file (" + input.filename + ")", ct);
                    return false;
                }
                // document.json lives next to the PDF key (v2/ for 2.0 docs) in the docs bucket.
                string sJsonKey = input.filename.Substring(0, input.filename.LastIndexOf('/') + 1);
                sJsonKey += (oDocument.Version == "2.0" && !sJsonKey.EndsWith("v2/")) ? "v2/document.json" : "document.json";
                string sLocalJson = Path.Combine(sDocumentPath, "document.json");
                oDCS3Services.downloadFile(Constants.DEFAULT_DOCS_LOCATION, sJsonKey, sLocalJson);

                jobRow.Progress = 30; await context.SaveChangesAsync(ct);

                // 2) Render every page to ZPage_*.jpg (144 dpi), matching legacy createImagesFromPDF.
                if (!RenderAllPages(sDocumentPath, sLocalPdfPath))
                {
                    await FailAsync(context, jobRow, oJob, "Failed to create images (dcmutool)", ct);
                    return false;
                }
                var zpages = new DirectoryInfo(sDocumentPath).GetFiles("ZPage_*.jpg")
                    .OrderBy(f => ParsePageNum(f.Name)).ToList();
                if (zpages.Count == 0)
                {
                    await FailAsync(context, jobRow, oJob, "No page images produced", ct);
                    return false;
                }
                jobRow.Progress = 70; await context.SaveChangesAsync(ct);

                // 3) Rename to Page_<guid>.jpg, capture dimensions, optional thumbnails, build result.
                List<string> pageIds = ReadPageIds(sLocalJson);
                bool bThumbs = input.generatethumbnails;
                var arrImages = new JArray();
                var arrThumbs = new JArray();
                bool bFirst = true;
                for (int i = 0; i < zpages.Count; i++)
                {
                    string guid = (bThumbs && pageIds != null && pageIds.Count > i)
                        ? pageIds[i] : Guid.NewGuid().ToString();
                    string sPageFile = "Page_" + guid + ".jpg";
                    string sPageFull = Path.Combine(sDocumentPath, sPageFile);
                    if (File.Exists(sPageFull)) File.Delete(sPageFull);
                    File.Move(zpages[i].FullName, sPageFull);

                    int w, h;
                    using (Image img = Image.FromFile(sPageFull)) { w = img.Width; h = img.Height; }

                    if (bThumbs)
                    {
                        DocumentConvertor.createThumbnails(sDocumentPath, sPageFull, guid);
                        string sThumbFile = "Thumbnail_" + guid + ".jpg";
                        string sThumbFull = Path.Combine(sDocumentPath, sThumbFile);
                        if (File.Exists(sThumbFull))
                        {
                            int tw, th;
                            using (Image timg = Image.FromFile(sThumbFull)) { tw = timg.Width; th = timg.Height; }
                            arrThumbs.Add(new JObject { ["Height"] = th, ["Width"] = tw, ["FileName"] = sThumbFile });
                            if (bFirst)
                            {
                                bFirst = false;
                                string sFirstThumb = Path.Combine(sDocumentPath, "Thumbnail.jpg");
                                if (File.Exists(sFirstThumb)) File.Delete(sFirstThumb);
                                File.Copy(sThumbFull, sFirstThumb);
                            }
                        }
                    }
                    arrImages.Add(new JObject { ["Height"] = h, ["Width"] = w, ["FileName"] = sPageFile });
                }

                // 4) Rename the PDF to a guid and assemble the result JSON.
                string sPdfNew = Guid.NewGuid().ToString() + ".pdf";
                string sPdfNewFull = Path.Combine(sDocumentPath, sPdfNew);
                if (File.Exists(sPdfNewFull)) File.Delete(sPdfNewFull);
                File.Move(sLocalPdfPath, sPdfNewFull);

                var oResult = new JObject { ["Pages"] = arrImages };
                if (bThumbs) oResult["Thumbnails"] = arrThumbs;
                oResult["PDF"] = sPdfNew;
                input.result = oResult.ToString();

                // 5) Upload the regenerated document directory to the v2 prefix.
                string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
                string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
                string sKeyPrefix = string.Format("{0}/{1}/{2}/v2", sPublisherName, sPublicationName, oDocument.Id);
                oDCS3Services.uploadDirectory(sBucketName, sDocumentPath, sKeyPrefix);

                jobRow.Progress = 100;
                jobRow.Desctiption = "Completed successfully";
                jobRow.Status = Constants.JobProcessingStatus.Completed.ToString();
                await context.SaveChangesAsync(ct);
                _log.LogInformation("CreateImages job {JobId}: rendered {Count} pages for document {DocId}",
                    oJob.Id, arrImages.Count, oDocument.Id);
                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "CreateImages failed for job {JobId}", oJob.Id);
                try
                {
                    jobRow ??= await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                    await FailAsync(context, jobRow, oJob, "Create images failed: " + e.Message, ct);
                }
                catch { /* JobProcessor marks the job Failed via its own context */ }
                return false;
            }
        }

        private static bool RenderAllPages(string sOutputDir, string sPdfPath)
        {
            string output = sOutputDir.EndsWith("\\") ? sOutputDir : sOutputDir + "\\";
            string exePath = Path.Combine(AppContext.BaseDirectory, "Tools", "dcmutool", "dcmutool.exe");
            string args = string.Format("convert -O resolution=144 -o \"{0}ZPage_%d.jpg\" \"{1}\"", output, sPdfPath);
            var cmdsi = new ProcessStartInfo(exePath)
            {
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            return cmd.ExitCode == 0;
        }

        private static int ParsePageNum(string fileName)
        {
            // ZPage_<n>.jpg
            int start = fileName.IndexOf('_') + 1;
            int dot = fileName.LastIndexOf('.');
            if (start <= 0 || dot <= start) return int.MaxValue;
            return int.TryParse(fileName.Substring(start, dot - start), out int n) ? n : int.MaxValue;
        }

        private static List<string> ReadPageIds(string sJsonPath)
        {
            try
            {
                if (!File.Exists(sJsonPath)) return null;
                var doc = JObject.Parse(File.ReadAllText(sJsonPath));
                var ids = doc.SelectTokens("..page..@attributes.id").Select(t => t.ToString()).ToList();
                return ids.Count > 0 ? ids : null;
            }
            catch { return null; }
        }

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
