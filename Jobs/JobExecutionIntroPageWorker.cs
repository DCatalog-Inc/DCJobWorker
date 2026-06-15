using Amazon.SQS;
using core;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace JobWorker.Jobs
{
    // Native port of the legacy WebPublisher.JobExecutionIntroPage (~the last page op that
    // still forwarded to the Windows DocProcessor). The intro page is a synthetic "page 0":
    // it is added/removed in BOTH document.xml (page node with intro="1") and document.json
    // (page entry with isintropage="1"), gets its own Page_0.xml/Page_0.json and
    // ZPage_0/Thumbnail_0 images, and does not shift real page numbers.
    //
    // command == "delete" removes the intro; anything else adds/replaces it from the file
    // uploaded to input.bucketname/input.filename (pdf, png or jpg).
    public class JobExecutionIntroPageWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionIntroPageWorker> _log;
        private readonly ISimpleEmailSender _emailSender;
        private readonly IAmazonSQS _sqs;

        public JobExecutionIntroPageWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<JobExecutionIntroPageWorker> log,
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
            _log.LogInformation("JobExecutionIntroPageWorker job {JobId}", oJob.Id);
            job jobRow = null;
            try
            {
                var input = await context.createintropageinput
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
                    await FailAsync(context, jobRow, oJob, "No createintropageinput/document for job", ct);
                    return false;
                }

                // Serialize page-ops on this document across workers (prevents same-doc file-in-use
                // collisions with concurrent convert/replace/intro operations).
                await using var docLock = await DocumentLock.AcquireAsync(context, input.Document.Id, 600, ct);
                if (!docLock.Acquired)
                {
                    await FailAsync(context, jobRow, oJob, "Could not acquire document lock (another operation on this document is in progress)", ct);
                    return false;
                }

                document oDocument = input.Document;
                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                if (!Directory.Exists(sDocumentPath))
                    Directory.CreateDirectory(sDocumentPath);
                string sDocumentRelativeURL = DocumentUtilBase.getDocumentRelativeURL(oDocument);
                string sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName)
                    ? Constants.DEFAULT_DOCS_LOCATION
                    : oDocument.Publication.Publisher.BucketName;

                var oDCS3Services = new DCS3Services();

                // Fresh copies of the document descriptors — the local working dir may be stale.
                await DocumentConvertor.downloadDocumentFile(oDocument, sDocumentPath, "document.json");
                await DocumentConvertor.downloadDocumentFile(oDocument, sDocumentPath, "document.xml");
                string sDocXmlFileName = Path.Combine(sDocumentPath, "document.xml");
                string sDocJsonFileName = Path.Combine(sDocumentPath, "document.json");
                if (!File.Exists(sDocXmlFileName) || !File.Exists(sDocJsonFileName))
                {
                    await FailAsync(context, jobRow, oJob, "document.xml/document.json missing for document " + oDocument.Id, ct);
                    return false;
                }

                // Any existing intro is removed first (both for delete and for re-add).
                RemoveIntroFromXml(sDocumentPath, sDocXmlFileName);
                RemoveIntroFromJson(sDocJsonFileName);

                if (input.command == "delete")
                {
                    oDCS3Services.uploadFile(sBucketName, sDocXmlFileName, sDocumentRelativeURL);
                    oDCS3Services.uploadFile(sBucketName, sDocJsonFileName, sDocumentRelativeURL);
                    CompleteDocument(context, jobRow, oDocument);
                    await context.SaveChangesAsync(ct);
                    return true;
                }

                // --- addintro ---
                string sIntroID = Guid.NewGuid().ToString();
                string sExt = Path.GetExtension(input.filename ?? "");
                if (string.IsNullOrEmpty(sExt)) sExt = ".pdf";
                sExt = sExt.ToLowerInvariant();
                string sResDir = Path.Combine(sDocumentPath, "res");
                if (!Directory.Exists(sResDir))
                    Directory.CreateDirectory(sResDir);
                string sLocalUpload = Path.Combine(sResDir, Guid.NewGuid().ToString() + sExt);
                oDCS3Services.downloadFile(input.bucketname, input.filename, sLocalUpload);

                int nWidth, nHeight;
                string sShortFileName = "ZPage_0.jpg";
                string sZPage0 = Path.Combine(sDocumentPath, sShortFileName);
                if (sExt == ".png" || sExt == ".jpg" || sExt == ".jpeg")
                {
                    using (var img = System.Drawing.Image.FromFile(sLocalUpload))
                    {
                        nWidth = img.Width;
                        nHeight = img.Height;
                        img.Save(sZPage0, System.Drawing.Imaging.ImageFormat.Jpeg);
                    }
                    DCJobs.DocumentConvertor.createThumbnails(sDocumentPath, sZPage0, "0");
                }
                else
                {
                    var oDocumentConvertor = new DCJobs.DocumentConvertor(_log);
                    if (!oDocumentConvertor.init(sLocalUpload))
                        throw new Exception("The uploaded intro file could not be opened as a PDF: " + input.filename);
                    int nImageQuality = 85;
                    int nResolution = System.Convert.ToInt32(Math.Floor(oDocument.NormalPageResolution));
                    int w, h;
                    // createImageReplace renders PAGE 1 of the upload and names the output for
                    // page 0 — createImageEx renders the TARGET page number, and page 0 doesn't
                    // exist in the uploaded PDF (the first native run died on the missing jpg).
                    // It also regenerates Thumbnail_0.jpg from the rendered image itself.
                    var inNormal = oDocumentConvertor.createImageInput(nResolution, -1, -1, sLocalUpload, sDocumentPath, "Page_", nImageQuality, jobRow, "Creating Normal Pages");
                    oDocumentConvertor.createImageReplace(inNormal, 1, 0, out w, out h);
                    var inHi = oDocumentConvertor.createImageInput(System.Convert.ToInt32(oDocument.HiPageResolution), -1, -1, sLocalUpload, sDocumentPath, "ZPage_", nImageQuality, jobRow, "Creating High Resolution Images");
                    oDocumentConvertor.createImageReplace(inHi, 1, 0, out nWidth, out nHeight);
                    oDocumentConvertor.release();
                }

                AddIntroToXml(sDocXmlFileName, sShortFileName, nWidth.ToString(), nHeight.ToString(), sIntroID);
                WriteIntroPageXml(sDocumentPath, sIntroID);
                AddIntroToJson(sDocJsonFileName, nWidth.ToString(), nHeight.ToString(), sIntroID);

                // Page_0.json for the viewer (links overlay data; intro has none but the file
                // must exist for the viewer to render page 0).
                string sPage0Xml = Path.Combine(sDocumentPath, "Page_0.xml");
                var oXMLResult = new XmlDocument();
                oXMLResult.Load(sPage0Xml);
                string sPageResult = DocumentConvertor.getPageJson(oDocument, oXMLResult, oDocument.Publication.PublicationTemplate, false);
                string sPage0Json = Path.Combine(sDocumentPath, "Page_0.json");
                File.WriteAllText(sPage0Json, sPageResult);

                oDCS3Services.uploadFile(sBucketName, sPage0Json, sDocumentRelativeURL);
                oDCS3Services.uploadFile(sBucketName, sZPage0, sDocumentRelativeURL);
                string sThumb0 = Path.Combine(sDocumentPath, "Thumbnail_0.jpg");
                if (File.Exists(sThumb0))
                    oDCS3Services.uploadFile(sBucketName, sThumb0, sDocumentRelativeURL);
                string sPage0Jpg = Path.Combine(sDocumentPath, "Page_0.jpg");
                if (File.Exists(sPage0Jpg))
                    oDCS3Services.uploadFile(sBucketName, sPage0Jpg, sDocumentRelativeURL);
                oDCS3Services.uploadFile(sBucketName, sDocXmlFileName, sDocumentRelativeURL);
                oDCS3Services.uploadFile(sBucketName, sDocJsonFileName, sDocumentRelativeURL);

                CompleteDocument(context, jobRow, oDocument);
                await context.SaveChangesAsync(ct);
                return true;
            }
            catch (Exception e)
            {
                _log.LogError(e, "IntroPage failed for job {JobId}", oJob.Id);
                try
                {
                    jobRow ??= await context.job.FirstOrDefaultAsync(j => j.Id == oJob.Id, ct);
                    await FailAsync(context, jobRow, oJob, "Intro page failed: " + e.Message, ct);
                }
                catch { /* JobProcessor marks the job Failed via its own context */ }
                return false;
            }
        }

        // Removes //pages/page[@intro='1'] nodes from document.xml and deletes their files.
        private static void RemoveIntroFromXml(string sDocumentPath, string sDocXmlFileName)
        {
            var oXMLDocument = new XmlDocument();
            oXMLDocument.Load(sDocXmlFileName);
            XmlNodeList oIntroList = oXMLDocument.SelectNodes("//pages/page[@intro='1']");
            foreach (XmlNode oIntro in oIntroList)
            {
                foreach (string child in new[] { "normal", "data" })
                {
                    var sFile = oIntro.SelectSingleNode(child)?.Attributes?["file"]?.Value;
                    if (!string.IsNullOrEmpty(sFile))
                    {
                        string sFilePath = Path.Combine(sDocumentPath, sFile);
                        try { if (File.Exists(sFilePath)) File.Delete(sFilePath); } catch { }
                    }
                }
                oIntro.ParentNode.RemoveChild(oIntro);
            }
            oXMLDocument.Save(sDocXmlFileName);
        }

        // Removes the leading isintropage entry from document.json's page array.
        private static void RemoveIntroFromJson(string sDocJsonFileName)
        {
            JObject settingsJson = JObject.Parse(File.ReadAllText(sDocJsonFileName));
            if (settingsJson["issue"]?["page"] is JArray arrPages && arrPages.Count > 0)
            {
                var firstAttrs = arrPages[0]["@attributes"] as JObject;
                if (firstAttrs?.GetValue("isintropage")?.ToString() == "1")
                {
                    arrPages.RemoveAt(0);
                    File.WriteAllText(sDocJsonFileName, settingsJson.ToString());
                }
            }
        }

        // Prepends the intro <page> node into //pages of document.xml.
        private static void AddIntroToXml(string sDocXmlFileName, string sFileName, string sWidth, string sHeight, string sIntroID)
        {
            var oXMLDocument = new XmlDocument();
            oXMLDocument.Load(sDocXmlFileName);

            XmlNode oIntroPageNode = oXMLDocument.CreateNode("element", "page", "");
            void SetAttr(XmlNode node, string name, string value)
            {
                var attr = oXMLDocument.CreateAttribute(name);
                attr.Value = value;
                node.Attributes.Append(attr);
            }
            SetAttr(oIntroPageNode, "num", "0");
            SetAttr(oIntroPageNode, "label", "0");
            SetAttr(oIntroPageNode, "ver", new Random().Next(int.MaxValue - 10).ToString());
            SetAttr(oIntroPageNode, "intro", "1");
            SetAttr(oIntroPageNode, "id", sIntroID);

            XmlNode oNormal = oXMLDocument.CreateNode("element", "normal", "");
            SetAttr(oNormal, "file", sFileName);
            SetAttr(oNormal, "width", sWidth);
            SetAttr(oNormal, "height", sHeight);
            oIntroPageNode.AppendChild(oNormal);

            XmlNode oHi = oXMLDocument.CreateNode("element", "hi", "");
            SetAttr(oHi, "file", sFileName);
            SetAttr(oHi, "width", sWidth);
            SetAttr(oHi, "height", sHeight);
            oIntroPageNode.AppendChild(oHi);

            XmlNode oData = oXMLDocument.CreateNode("element", "data", "");
            SetAttr(oData, "file", "Page_0.xml");
            oIntroPageNode.AppendChild(oData);

            XmlNode oPagesNode = oXMLDocument.SelectSingleNode("//pages");
            oPagesNode.PrependChild(oIntroPageNode);
            oXMLDocument.Save(sDocXmlFileName);
        }

        // Writes the (empty) per-page data file Page_0.xml for the intro page.
        private static void WriteIntroPageXml(string sDocumentPath, string sIntroID)
        {
            var xmlPage = new XmlDocument();
            xmlPage.AppendChild(xmlPage.CreateProcessingInstruction("xml", "version='1.0' encoding='utf-8'"));
            XmlElement rootNode = xmlPage.CreateElement("page");
            xmlPage.AppendChild(rootNode);
            rootNode.SetAttribute("id", sIntroID);
            rootNode.SetAttribute("num", "0");
            foreach (string section in new[] { "links", "videos", "audios", "flashanimations" })
                rootNode.AppendChild(xmlPage.CreateElement(section));
            xmlPage.Save(Path.Combine(sDocumentPath, "Page_0.xml"));
        }

        // Prepends the intro page entry into document.json's page array.
        private static void AddIntroToJson(string sDocJsonFileName, string sWidth, string sHeight, string sIntroID)
        {
            var new_intro = new JObject();
            new_intro["@attributes"] = new JObject
            {
                ["id"] = sIntroID,
                ["version"] = (DateTime.Now.Ticks % int.MaxValue).ToString(),
                ["sequence"] = "0",
                ["thumb"] = "Thumbnail_0.jpg",
                ["name"] = "0",
                ["width"] = sWidth,
                ["height"] = sHeight,
                ["contentType"] = "",
                ["iphoneImage"] = "ZPage_0.jpg",
                ["isintropage"] = "1",
                ["recolor"] = 0
            };
            JObject settingsJson = JObject.Parse(File.ReadAllText(sDocJsonFileName));
            if (settingsJson["issue"]?["page"] is JArray arrPages)
            {
                if (arrPages.Count > 0
                    && (arrPages[0]["@attributes"] as JObject)?.GetValue("isintropage")?.ToString() == "1")
                {
                    arrPages.RemoveAt(0);
                }
                arrPages.Insert(0, new_intro);
            }
            File.WriteAllText(sDocJsonFileName, settingsJson.ToString());
        }

        private static void CompleteDocument(ApplicationDbContext context, job jobRow, document oDocument)
        {
            oDocument.DocumentStatus = Constants.JobProcessingStatus.Completed.ToString();
            oDocument.DocumentProcessingDescription = "Finished Successfully";
            oDocument.DocumentProgressingPercent = 100;
            oDocument.DocumentProcessedBy = "DCJobWorker@" + Environment.MachineName;
            if (jobRow != null)
            {
                jobRow.Progress = 100;
                jobRow.Status = Constants.JobProcessingStatus.Completed.ToString();
                jobRow.Desctiption = "Finished Successfully";
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
