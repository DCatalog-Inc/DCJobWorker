using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Serilog;

namespace JobWorker.Jobs
{
    // Ported from epaperflip/WebPublisher/JobExecutionSearchProductsInPublication.cs.
    // Builds a dcproxy job xml for a whole publication, optionally downloads every document's
    // page files first (updatecatalogs), runs the recognizer, then uploads the results back.
    public class JobExecutionSearchProductsInPublication : IJobExecution
    {
        private readonly ApplicationDbContext _context;

        public JobExecutionSearchProductsInPublication(ApplicationDbContext context)
        {
            _context = context;
        }

        protected int m_nTimeout = 7200000;
        protected int nFromPage = 1;
        protected int nToPage = 0;
        protected string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
        protected string sKeyPrefix = "";
        protected string sDocumentPath = "";
        protected bool usejson = true;
        protected bool updatecatalogs = false;

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            Log.Information("JobExecutionSearchProductsInPublication job {JobId}", oJob.Id);
            try
            {
                var input = await _context.searchproductsinpublicationinput
                    .Include(c => c.Job)
                    .Include(c => c.Publication)
                    .Include(c => c.Publication.Publisher)
                    .Include(c => c.Publication.Documents)
                    .Where(c => c.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                if (input == null || input.Publication == null)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "No searchproductsinpublicationinput/publication for job";
                    _context.Update(oJob);
                    await _context.SaveChangesAsync(ct);
                    return false;
                }

                updatecatalogs = input.updatealldocumentsinpublication;
                sBucketName = string.IsNullOrEmpty(input.Publication.Publisher.BucketName)
                    ? Constants.DEFAULT_DOCS_LOCATION : input.Publication.Publisher.BucketName;

                string sJobFile = generateJobFile(oJob, input);
                executeSearchProducts(sJobFile, oJob, input.Publication);

                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
            }
            catch (Exception e)
            {
                Log.Error(e, "SearchProductsInPublication failed for job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Search products in publication failed: " + e.Message;
                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
                return false;
            }

            return oJob.Status == Constants.JobProcessingStatus.Completed.ToString();
        }

        protected string generateJobFile(job oJob, searchproductsinpublicationinput input)
        {
            string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
            string sJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");

            XmlDocument oLinksParams = new XmlDocument();
            XmlDeclaration xmlDeclaration = oLinksParams.CreateXmlDeclaration("1.0", "utf-8", null);
            XmlElement rootNode = oLinksParams.CreateElement("job");
            rootNode.SetAttribute("name", "SearchProductsInPublication");
            oLinksParams.InsertBefore(xmlDeclaration, oLinksParams.DocumentElement);
            oLinksParams.AppendChild(rootNode);

            AppendNode(oLinksParams, rootNode, "inputfile", "");
            AppendNode(oLinksParams, rootNode, "outputdir", "");
            AppendNode(oLinksParams, rootNode, "isproduct", input.importasproduct ? "1" : "0");
            AppendNode(oLinksParams, rootNode, "updatecatalogs", input.updatealldocumentsinpublication ? "1" : "0");
            AppendNode(oLinksParams, rootNode, "deletepreviouslinks", input.deletepreviouslinks ? "1" : "0");
            AppendNode(oLinksParams, rootNode, "producticonxoffset", input.producticonxoffset.ToString());
            AppendNode(oLinksParams, rootNode, "producticonyoffset", input.producticonyoffset.ToString());
            AppendNode(oLinksParams, rootNode, "publicationid", input.Publication.Id.ToString());
            AppendNode(oLinksParams, rootNode, "usejson", "1");

            rootNode.SetAttribute("id", oJob.Id.ToString());
            oLinksParams.Save(sJobFile);
            return sJobFile;
        }

        private static void AppendNode(XmlDocument doc, XmlElement parent, string name, string value)
        {
            XmlElement el = doc.CreateElement(name);
            el.InnerText = value;
            parent.AppendChild(el);
        }

        public void executeSearchProducts(string sFileName, job oJob, publication pub)
        {
            try
            {
                if (updatecatalogs)
                    downloadPublicationFiles(pub, oJob);

                string sProcessName = Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
                Process oPDFProcess = new Process();
                ProcessStartInfo startInfo = new ProcessStartInfo(sProcessName)
                {
                    Arguments = sFileName,
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Hidden
                };
                oPDFProcess.StartInfo = startInfo;
                oPDFProcess.EnableRaisingEvents = false;
                oPDFProcess.Start();
                oPDFProcess.WaitForExit(m_nTimeout);

                if (oPDFProcess.HasExited == false)
                {
                    try { oPDFProcess.CloseMainWindow(); } catch { }
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "Timeout";
                    try { oPDFProcess.Close(); } catch { }
                    return;
                }

                if (oPDFProcess.ExitCode == 0)
                {
                    uploadGeneratedFile(oJob, pub);
                    oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                    oJob.Progress = 100;
                }
                else
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "Recognizer exited with code " + oPDFProcess.ExitCode;
                }
            }
            catch (Exception ex)
            {
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = ex.Message;
                Log.Error(ex, "Exception in executeSearchProducts");
            }
        }

        public void downloadPublicationFiles(publication pub, job oJob)
        {
            oJob.Desctiption = "Prepping documents for linking";
            _context.Update(oJob);
            _context.SaveChanges();

            bool bDownloadOnlyActive = (pub.Publisher.ExtraOptions
                & Convert.ToInt32(Constants.PublisherExtraOptions.DownloadOnlyActive)) != 0;

            IList<document> listDocs = pub.Documents;
            int nCount = listDocs.Count;
            int i = 0;
            foreach (document d in listDocs)
            {
                i++;
                if (!d.Deleted && d.DocumentProgressingPercent == 100)
                {
                    if (bDownloadOnlyActive && !d.IsActive)
                        continue;
                    nFromPage = 1;
                    nToPage = d.NumberOfPages;
                    try { downloadDocumentFiles(d, pub); }
                    catch (Exception rx) { Log.Warning("download doc {Id} skipped: {Msg}", d.Id, rx.Message); }
                }
                oJob.Progress = nCount == 0 ? 100 : i * 100 / nCount;
                _context.Update(oJob);
                _context.SaveChanges();
            }
        }

        public void downloadDocumentFiles(document oDocument, publication pub)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            string sPublisherName = Utility.GenerateFriendlyURL(pub.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(pub.Name);
            sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);

            string sPDFKeyName = string.Format("{0}/{1}", sKeyPrefix, oDocument.PDFFileName);
            string sFullFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
            if (!File.Exists(sFullFileName))
            {
                try { oDCS3Services.downloadFile(sBucketName, sPDFKeyName, sFullFileName); }
                catch (Exception ex) { Log.Warning("PDF download skipped {Key}: {Msg}", sPDFKeyName, ex.Message); }
            }

            for (int i = nFromPage; i <= nToPage; i++)
            {
                string sPageName = usejson ? string.Format("Page_{0}.json", i) : string.Format("Page_{0}.xml", i);
                string sPageKey = string.Format("{0}/{1}", sKeyPrefix, sPageName);
                sFullFileName = Path.Combine(sDocumentPath, sPageName);
                try { oDCS3Services.downloadFile(sBucketName, sPageKey, sFullFileName); }
                catch (Exception ex) { Log.Warning("page download skipped {Key}: {Msg}", sPageKey, ex.Message); }
            }

            try
            {
                oDCS3Services.downloadFile(sBucketName, string.Format("{0}/{1}", sKeyPrefix, "document.json"),
                    Path.Combine(sDocumentPath, "document.json"));
            }
            catch (Exception ex) { Log.Warning("document.json download skipped: {Msg}", ex.Message); }
        }

        public void uploadGeneratedFile(job oJob, publication pub)
        {
            if (updatecatalogs)
            {
                oJob.Desctiption = "Finalizing";
                _context.Update(oJob);
                _context.SaveChanges();

                IList<document> listDocs = pub.Documents;
                int i = 0;
                int nCount = listDocs.Count;
                foreach (document d in listDocs)
                {
                    i++;
                    if (!d.Deleted)
                    {
                        uploadGenerateFileForDocument(d, pub, true);
                        oJob.Progress = nCount == 0 ? 100 : i * 100 / nCount;
                        _context.Update(oJob);
                        _context.SaveChanges();
                    }
                }
            }

            oJob.Progress = 100;
            oJob.Desctiption = "Complete";
            oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
        }

        public void uploadGenerateFileForDocument(document d, publication pub, bool bUploadAllPages)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            if (bUploadAllPages)
            {
                nFromPage = 1;
                nToPage = d.NumberOfPages;
            }
            string sPublisherName = Utility.GenerateFriendlyURL(pub.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(pub.Name);
            sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, d.Id);
            sDocumentPath = DocumentUtilBase.getDocumentPath(d);

            for (int i = nFromPage; i <= nToPage; i++)
            {
                string sJsonPageName = string.Format("Page_{0}.json", i);
                string sFullFileName = Path.Combine(sDocumentPath, sJsonPageName);
                if (File.Exists(sFullFileName))
                    oDCS3Services.uploadFile(sBucketName, sFullFileName, sKeyPrefix);
            }

            string sDocFullFileName = Path.Combine(sDocumentPath, "document.json");
            if (File.Exists(sDocFullFileName))
                oDCS3Services.uploadFile(sBucketName, sDocFullFileName, sKeyPrefix);
        }
    }
}
