using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Serilog;

namespace JobWorker.Jobs
{
    // Single-document analog of JobExecutionSearchProductsInPublication: builds a
    // SearchProductsInDocument dcproxy job, downloads the document's page files, runs the
    // recognizer, then uploads the results. Ported from epaperflip SearchProductsInDocument helper.
    public class JobExecutionSearchProductsInDocument : IJobExecution
    {
        private readonly ApplicationDbContext _context;

        public JobExecutionSearchProductsInDocument(ApplicationDbContext context)
        {
            _context = context;
        }

        protected int m_nTimeout = 7200000;
        protected string sBucketName = Constants.DEFAULT_DOCS_LOCATION;

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            Log.Information("JobExecutionSearchProductsInDocument job {JobId}", oJob.Id);
            try
            {
                var input = await _context.searchproductsindocumentinput
                    .Include(c => c.Job)
                    .Include(c => c.Document)
                    .Include(c => c.Document.Publication)
                    .Include(c => c.Document.Publication.Publisher)
                    .Where(c => c.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                if (input == null || input.Document == null)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "No searchproductsindocumentinput/document for job";
                    _context.Update(oJob);
                    await _context.SaveChangesAsync(ct);
                    return false;
                }

                document oDocument = input.Document;
                sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName)
                    ? Constants.DEFAULT_DOCS_LOCATION : oDocument.Publication.Publisher.BucketName;

                string sJobFile = generateJobFile(oDocument, input);
                downloadDocumentFiles(oDocument);

                string sProcessName = Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
                Process oPDFProcess = new Process();
                oPDFProcess.StartInfo = new ProcessStartInfo(sProcessName)
                {
                    Arguments = sJobFile,
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Hidden
                };
                oPDFProcess.Start();
                oPDFProcess.WaitForExit(m_nTimeout);

                if (oPDFProcess.HasExited && oPDFProcess.ExitCode == 0)
                {
                    uploadDocumentFiles(oDocument);
                    oJob.Progress = 100;
                    oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                }
                else
                {
                    try { oPDFProcess.CloseMainWindow(); oPDFProcess.Close(); } catch { }
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = oPDFProcess.HasExited ? "Recognizer exited with code " + oPDFProcess.ExitCode : "Timeout";
                }

                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
            }
            catch (Exception e)
            {
                Log.Error(e, "SearchProductsInDocument failed for job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Search products in document failed: " + e.Message;
                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
                return false;
            }

            return oJob.Status == Constants.JobProcessingStatus.Completed.ToString();
        }

        protected string generateJobFile(document oDocument, searchproductsindocumentinput input)
        {
            string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
            string sJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");

            XmlDocument doc = new XmlDocument();
            XmlDeclaration decl = doc.CreateXmlDeclaration("1.0", "utf-8", null);
            XmlElement root = doc.CreateElement("job");
            root.SetAttribute("name", "SearchProductsInDocument");
            doc.InsertBefore(decl, doc.DocumentElement);
            doc.AppendChild(root);

            Append(doc, root, "inputfile", "");
            Append(doc, root, "outputdir", DocumentUtilBase.getDocumentPath(oDocument));
            Append(doc, root, "docid", oDocument.Id.ToString());
            Append(doc, root, "isproduct", input.importasproduct ? "1" : "0");
            Append(doc, root, "deletepreviouslinks", input.deletepreviouslinks ? "1" : "0");
            Append(doc, root, "producticonxoffset", input.producticonxoffset.ToString());
            Append(doc, root, "producticonyoffset", input.producticonyoffset.ToString());
            Append(doc, root, "usejson", "1");

            root.SetAttribute("id", oDocument.Id.ToString());
            doc.Save(sJobFile);
            return sJobFile;
        }

        private static void Append(XmlDocument doc, XmlElement parent, string name, string value)
        {
            XmlElement el = doc.CreateElement(name);
            el.InnerText = value;
            parent.AppendChild(el);
        }

        private void downloadDocumentFiles(document oDocument)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);

            for (int i = 1; i <= oDocument.NumberOfPages; i++)
            {
                string sPageName = string.Format("Page_{0}.json", i);
                try { oDCS3Services.downloadFile(sBucketName, string.Format("{0}/{1}", sKeyPrefix, sPageName), Path.Combine(sDocumentPath, sPageName)); }
                catch (Exception ex) { Log.Warning("page download skipped {Page}: {Msg}", sPageName, ex.Message); }
            }
            try
            {
                oDCS3Services.downloadFile(sBucketName, string.Format("{0}/{1}", sKeyPrefix, "document.json"),
                    Path.Combine(sDocumentPath, "document.json"));
            }
            catch (Exception ex) { Log.Warning("document.json download skipped: {Msg}", ex.Message); }
        }

        private void uploadDocumentFiles(document oDocument)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);

            for (int i = 1; i <= oDocument.NumberOfPages; i++)
            {
                string sJsonPageName = string.Format("Page_{0}.json", i);
                string sFullFileName = Path.Combine(sDocumentPath, sJsonPageName);
                if (File.Exists(sFullFileName))
                    oDCS3Services.uploadFile(sBucketName, sFullFileName, sKeyPrefix);
            }
            string sDocJson = Path.Combine(sDocumentPath, "document.json");
            if (File.Exists(sDocJson))
                oDCS3Services.uploadFile(sBucketName, sDocJson, sKeyPrefix);
        }
    }
}
