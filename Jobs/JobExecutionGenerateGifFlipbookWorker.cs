using core;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using GifFlippingBook;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    /// <summary>
    /// Worker that generates an animated GIF flipbook for a document.
    /// Implements the same logic as DCJobs.JobExecutionGenerateGifFlipbook.executeJob(),
    /// which is internal to DCJobs and cannot be called directly.
    /// </summary>
    public class JobExecutionGenerateGifFlipbookWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionGenerateGifFlipbookWorker> _log;

        public JobExecutionGenerateGifFlipbookWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<JobExecutionGenerateGifFlipbookWorker> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }

        private bool DownloadFiles(document oDocument, int nNumberOfPages, string sPrefix)
        {
            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            DCS3Services oDCS3Services = new DCS3Services();
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);

            int nPageCount = oDocument.NumberOfPages;
            nNumberOfPages = Math.Min(nPageCount, nNumberOfPages);
            for (int i = 0; i < nNumberOfPages; i++)
            {
                int nCurrentPage = i + 1;
                string sName = string.Format("{0}{1}.jpg", sPrefix, nCurrentPage);
                string sThumbFileToDownload = Path.Combine(sDocumentPath, sName);
                string sThumbDownloadKey = string.Format("{0}/{1}", sKeyPrefix, sName);
                oDCS3Services.downloadFile(sBucketName, sThumbDownloadKey, sThumbFileToDownload);
            }
            return true;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JobExecutionGenerateGifFlipbookWorker job {JobId}", oJob.Id);

            oJob.Progress = 10;
            oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
            ctx.job.Update(oJob);
            await ctx.SaveChangesAsync(ct);

            var oCreategifFlippingbookInput = await ctx.creategifflippingbookinput
                .Include(d => d.Document)
                .Include(d => d.Document.Publication)
                .Include(d => d.Document.Publication.Publisher)
                .Where(d => d.Job.Id == oJob.Id)
                .FirstOrDefaultAsync(ct);

            if (oCreategifFlippingbookInput == null)
            {
                _log.LogWarning("JobExecutionGenerateGifFlipbookWorker: no creategifflippingbookinput found for job {JobId}", oJob.Id);
                return false;
            }

            document oDocument = oCreategifFlippingbookInput.Document;
            int width = oCreategifFlippingbookInput.Width;
            int height = oCreategifFlippingbookInput.Height;
            double ratio = oCreategifFlippingbookInput.Ratio;
            string backgroundColor = oCreategifFlippingbookInput.BackGroundColor;
            int interval = oCreategifFlippingbookInput.FlipInterval;
            int numberOfImages = oCreategifFlippingbookInput.NumberOfImages;
            string gifFileName = oCreategifFlippingbookInput.GifFileName;

            try
            {
                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                if (!Directory.Exists(sDocumentPath))
                    Directory.CreateDirectory(sDocumentPath);

                string sFlippingBookGif = Path.Combine(sDocumentPath, "MiniFlipper");
                if (!Directory.Exists(sFlippingBookGif))
                    Directory.CreateDirectory(sFlippingBookGif);

                string sPrefixName = "Thumbnail_";
                if (width > 300 || height > 300)
                    sPrefixName = "ZPage_";

                DownloadFiles(oDocument, numberOfImages, sPrefixName);

                // The creator appends ".gif" itself — normalize so we call it with the base
                // name and look for the file it actually writes (the old check used the raw
                // name and reported "not found" even on success).
                string gifBaseName = !string.IsNullOrEmpty(gifFileName) && gifFileName.EndsWith(".gif", StringComparison.OrdinalIgnoreCase)
                    ? gifFileName.Substring(0, gifFileName.Length - 4)
                    : gifFileName;

                GifFlippingBookCreator oGifFlippingBookCreator = new GifFlippingBookCreator();
                oGifFlippingBookCreator.CreateGifFlippingBook(
                    width,
                    height,
                    ratio,
                    backgroundColor,
                    interval,
                    sPrefixName,
                    ".jpg",
                    numberOfImages,
                    sDocumentPath,
                    sFlippingBookGif,
                    gifBaseName);

                string sOutputFile = Path.Combine(sFlippingBookGif, gifBaseName + ".gif");
                if (File.Exists(sOutputFile))
                {
                    DCS3Services oDCS3Services = new DCS3Services();
                    string sKeyPrefix = string.Format("{0}/{1}/{2}/MiniFlipper",
                        oDocument.PublisherFolderName,
                        oDocument.TemplateFolderName,
                        oDocument.Id);
                    oDCS3Services.uploadFile(Constants.DEFAULT_DOCS_LOCATION, sOutputFile, sKeyPrefix);
                    _log.LogInformation("JobExecutionGenerateGifFlipbookWorker: uploaded GIF {GifFileName} for job {JobId}", gifBaseName + ".gif", oJob.Id);
                }
                else
                {
                    // No GIF = the job did not do its work — fail it visibly instead of
                    // completing with nothing (the creator used to swallow its errors too).
                    throw new Exception("GIF flipbook was not produced (expected " + sOutputFile + ")");
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "JobExecutionGenerateGifFlipbookWorker: error executing job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = ex.Message;
                oDocument.DocumentStatus = Constants.JobProcessingStatus.Failed.ToString();
                oDocument.DocumentProcessingDescription = ex.Message;
                ctx.job.Update(oJob);
                ctx.document.Update(oDocument);
                await ctx.SaveChangesAsync(ct);
                return false;
            }

            return true;
        }
    }
}
