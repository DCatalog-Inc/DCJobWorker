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
    // Ported from epaperflip/WebPublisher/JobExecutionRecognizeLinks.cs (NHibernate) to the
    // new SQS worker. Full flow: download the job xml + the document's pages/PDF from S3,
    // run the dcproxy link-recognizer, then regenerate + upload the page files.
    public class JobExecutionRecognizeLinks : IJobExecution
    {
        private readonly ApplicationDbContext _context;

        public JobExecutionRecognizeLinks(ApplicationDbContext context)
        {
            _context = context;
        }

        protected string sDocumentId = "";
        protected int m_nTimeout = 1200000;
        protected int nFromPage = 1;
        protected int nToPage = 0;
        protected bool bUseRange = false;
        protected string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
        protected string sKeyPrefix = "";
        protected string sDocumentPath = "";
        protected bool usejson = false;

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            Log.Information("JobExecutionRecognizeLinks job {JobId}", oJob.Id);
            try
            {
                var oRecognizeLinks = await _context.recognizelinksinput
                                            .Include(c => c.Job)
                                            .Where(c => c.Job.Id == oJob.Id)
                                            .FirstOrDefaultAsync(ct);
                if (oRecognizeLinks == null)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "No recognizelinksinput row for job";
                    _context.Update(oJob);
                    await _context.SaveChangesAsync(ct);
                    return false;
                }

                recognizeLinks(oRecognizeLinks.RecognizeLinkXml, oJob);
                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
            }
            catch (Exception e)
            {
                Log.Error(e, "RecognizeLinks failed for job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Recognize links failed: " + e.Message;
                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
                return false;
            }

            return oJob.Status == Constants.JobProcessingStatus.Completed.ToString();
        }

        // Downloads the job xml + the document's PDF/pages from S3 to the local document path.
        // Returns the local path of the downloaded job xml (the dcproxy argument).
        public string downloadFiles(string sJobFileName)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
            string sLocalJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");
            oDCS3Services.downloadFileByURL(sJobFileName, sLocalJobFile);

            XmlDocument oJobXML = new XmlDocument();
            oJobXML.Load(sLocalJobFile);

            XmlNode oUseRange = oJobXML.SelectSingleNode("//userange");
            bUseRange = oUseRange != null && oUseRange.InnerText == "1";
            XmlNode oDocumentId = oJobXML.SelectSingleNode("//docid");
            sDocumentId = oDocumentId.InnerText;

            XmlNode oUseJson = oJobXML.SelectSingleNode("//usejson");
            if (oUseJson != null && oUseJson.InnerText == "1")
                usejson = true;

            document oDocument = _context.document
                .Include(d => d.Publication)
                .Include(d => d.Publication.Publisher)
                .Where(d => d.Id == sDocumentId)
                .SingleOrDefault();

            sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName)
                ? Constants.DEFAULT_DOCS_LOCATION : oDocument.Publication.Publisher.BucketName;

            int nNumberOfPages = oDocument.NumberOfPages;
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);

            // The job XML is built by the ADMIN with ITS repository layout (getDocumentPathDB →
            // D:\DCatalog\Docs, the legacy DocProcessor disk). dcproxy reads inputfile/outputdir
            // from that XML, so on this box it was told to open a drive that doesn't exist and
            // exited -1. Rewrite both to the worker-local document path before handing it over.
            LocalizeJobXmlPaths(oJobXML, sDocumentPath, oDocument.PDFFileName);
            oJobXML.Save(sLocalJobFile);

            if (Directory.Exists(sDocumentPath))
            {
                try { Directory.Delete(sDocumentPath, true); }
                catch (Exception ex1) { Log.Warning("Could not clean doc folder {Path}: {Msg}", sDocumentPath, ex1.Message); }
            }
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);

            // Download the source PDF (if present).
            string sPDFKeyName = string.Format("{0}/{1}", sKeyPrefix, oDocument.PDFFileName);
            string sPDFFullFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
            if (File.Exists(sPDFFullFileName))
            {
                try { File.Delete(sPDFFullFileName); } catch { }
            }
            try { oDCS3Services.downloadFile(sBucketName, sPDFKeyName, sPDFFullFileName); }
            catch (Exception ex) { Log.Warning("PDF download skipped {Key}: {Msg}", sPDFKeyName, ex.Message); }

            // Page range.
            nFromPage = 1;
            nToPage = nNumberOfPages;
            if (bUseRange)
            {
                XmlNode oFromPage = oJobXML.SelectSingleNode("//frompage");
                XmlNode oToPage = oJobXML.SelectSingleNode("//topage");
                nFromPage = Convert.ToInt32(oFromPage.InnerText);
                nToPage = Convert.ToInt32(oToPage.InnerText);
            }

            // Download the page files the recognizer reads.
            for (int i = nFromPage; i <= nToPage; i++)
            {
                string sPageName = usejson ? string.Format("Page_{0}.json", i) : string.Format("Page_{0}.xml", i);
                string sPageKey = string.Format("{0}/{1}", sKeyPrefix, sPageName);
                string sFullFileName = Path.Combine(sDocumentPath, sPageName);
                try { oDCS3Services.downloadFile(sBucketName, sPageKey, sFullFileName); }
                catch (Exception ex) { Log.Warning("Page download skipped {Key}: {Msg}", sPageKey, ex.Message); }
            }

            // document.json
            try
            {
                oDCS3Services.downloadFile(sBucketName, string.Format("{0}/{1}", sKeyPrefix, "document.json"),
                    Path.Combine(sDocumentPath, "document.json"));
            }
            catch (Exception ex) { Log.Warning("document.json download skipped: {Msg}", ex.Message); }

            return sLocalJobFile;
        }

        // Regenerates page files and uploads them back to S3 after the recognizer runs.
        public void uploadGeneratedFile()
        {
            DCS3Services oDCS3Services = new DCS3Services();
            if (string.IsNullOrEmpty(sDocumentId))
                return;

            if (!usejson)
                new JobWorker.DocumentConvertor().generateS3Files(_context, sDocumentId);

            for (int i = nFromPage; i <= nToPage; i++)
            {
                string sJsonPageName = string.Format("Page_{0}.json", i);
                string sFullFileName = Path.Combine(sDocumentPath, sJsonPageName);
                if (File.Exists(sFullFileName))
                    oDCS3Services.uploadFile(sBucketName, sFullFileName, sKeyPrefix);
                if (!usejson)
                {
                    string sPageName = string.Format("Page_{0}.xml", i);
                    sFullFileName = Path.Combine(sDocumentPath, sPageName);
                    if (File.Exists(sFullFileName))
                        oDCS3Services.uploadFile(sBucketName, sFullFileName, sKeyPrefix);
                }
            }

            string sDocFullFileName = Path.Combine(sDocumentPath, "document.json");
            if (File.Exists(sDocFullFileName))
                oDCS3Services.uploadFile(sBucketName, sDocFullFileName, sKeyPrefix);
        }

        public void recognizeLinks(string sFileName, job oJob)
        {
            try
            {
                string sLocalJobFile = downloadFiles(sFileName);

                string sProcessName = Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
                Process oPDFProcess = new Process();
                ProcessStartInfo startInfo = new ProcessStartInfo(sProcessName)
                {
                    Arguments = sLocalJobFile,
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
                    oJob.Desctiption = "Recognize links timed out";
                    try { oPDFProcess.Close(); } catch { }
                    return;
                }

                if (oPDFProcess.ExitCode == 0)
                {
                    uploadGeneratedFile();
                    oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                    oJob.Progress = 100;
                }
                else
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "Recognize links exited with code " + oPDFProcess.ExitCode;
                }
            }
            catch (Exception ex)
            {
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = ex.Message;
                Log.Error(ex, "Exception when recognizing links for job {JobId}", oJob.Id);
            }
        }

        // Runs the recognizer against the document files that are ALREADY on local disk
        // (called inline by JobExecutionConvertPDF right after a convert — no S3 download/upload
        // here, the convert flow owns that). Only fetches the job-definition xml + runs dcproxy.
        // Rewrites the admin-generated job XML's inputfile/outputdir to this worker's local
        // document path (the admin writes its own RepositoryLocationDB layout into them).
        private static void LocalizeJobXmlPaths(XmlDocument oJobXML, string sLocalDocPath, string sPdfFileName)
        {
            XmlNode oInputFile = oJobXML.SelectSingleNode("//inputfile");
            if (oInputFile != null && !string.IsNullOrEmpty(sPdfFileName))
                oInputFile.InnerText = Path.Combine(sLocalDocPath, sPdfFileName);
            XmlNode oOutputDir = oJobXML.SelectSingleNode("//outputdir");
            if (oOutputDir != null)
                oOutputDir.InnerText = sLocalDocPath;
        }

        public void recognizeLinksFromLocal(recognizelinksinput oRecognizeLinksInput)
        {
            try
            {
                DCS3Services oDCS3Services = new DCS3Services();
                string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
                string sLocalJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");
                oDCS3Services.downloadFileByURL(oRecognizeLinksInput.RecognizeLinkXml, sLocalJobFile);

                // Same admin-path localization as downloadFiles — the XML carries the legacy
                // D:\ layout that doesn't exist on this box.
                try
                {
                    XmlDocument oJobXML = new XmlDocument();
                    oJobXML.Load(sLocalJobFile);
                    string sDocId = oJobXML.SelectSingleNode("//docid")?.InnerText;
                    var oDoc = _context.document
                        .Include(d => d.Publication)
                        .Include(d => d.Publication.Publisher)
                        .Where(d => d.Id == sDocId)
                        .SingleOrDefault();
                    if (oDoc != null)
                    {
                        LocalizeJobXmlPaths(oJobXML, DocumentUtilBase.getDocumentPath(oDoc), oDoc.PDFFileName);
                        oJobXML.Save(sLocalJobFile);
                    }
                }
                catch (Exception exLocalize)
                {
                    Log.Warning("Job XML path localization skipped: {Msg}", exLocalize.Message);
                }

                string sProcessName = Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
                Process oPDFProcess = new Process();
                ProcessStartInfo startInfo = new ProcessStartInfo(sProcessName)
                {
                    Arguments = sLocalJobFile,
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Hidden
                };
                oPDFProcess.StartInfo = startInfo;
                oPDFProcess.Start();
                oPDFProcess.WaitForExit(m_nTimeout);
            }
            catch (Exception ex)
            {
                if (oRecognizeLinksInput.Job != null)
                    oRecognizeLinksInput.Job.Desctiption = ex.Message;
                Log.Error(ex, "recognizeLinksFromLocal failed");
            }
        }
    }
}
