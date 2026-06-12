using core;
using Core;
using Core.Models;
using CsvHelper;
using CsvHelper.Configuration;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Serilog;

namespace JobWorker.Jobs
{
    // Ported from epaperflip/WebPublisher/JobExecutionImportCSV.cs.
    // Downloads the link/product CSV, builds an ImportCSV dcproxy job, downloads the document
    // (and optionally every publication document when importing products to catalog), runs the
    // recognizer, then uploads the results. Includes the Airgas-specific page-category step.
    public class JobExecutionImportCSV : IJobExecution
    {
        private readonly ApplicationDbContext _context;

        public JobExecutionImportCSV(ApplicationDbContext context)
        {
            _context = context;
        }

        protected int m_nTimeout = 7200000;
        protected int nFromPage = 1;
        protected int nToPage = 0;
        protected string sDocumentId = "";
        protected string sPubliactionId = "";
        protected string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
        protected string sKeyPrefix = "";
        protected string sDocumentPath = "";
        protected bool usejson = true;        // generateJobFile always writes usejson=1
        protected bool importproductstodb = false;
        protected bool updatecatalogs = false;
        protected bool bUseRange = false;

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            Log.Information("JobExecutionImportCSV job {JobId}", oJob.Id);
            try
            {
                var input = await _context.importcsvinput
                    .Include(c => c.Job)
                    .Include(c => c.Document)
                    .Include(c => c.Document.Publication)
                    .Include(c => c.Document.Publication.Publisher)
                    .Include(c => c.Publication)
                    .Include(c => c.Publication.Publisher)
                    .Where(c => c.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);
                if (input == null)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "No importcsvinput for job";
                    _context.Update(oJob);
                    await _context.SaveChangesAsync(ct);
                    return false;
                }

                importproductstodb = input.importproductstodb;
                updatecatalogs = input.updatealldocumentsinpublication;
                bUseRange = input.userange;
                document oDocument = input.Document;
                if (oDocument != null)
                {
                    sDocumentId = oDocument.Id;
                    sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName)
                        ? Constants.DEFAULT_DOCS_LOCATION : oDocument.Publication.Publisher.BucketName;
                }

                string sCSVFileName = downloadCSVFile(input);

                if (oDocument != null && oDocument.Publication.Publisher.Name == "Airgas")
                    updatecategoriesAirGas(oDocument, sCSVFileName);

                string sJobFile = generateJobFile(oJob, input, sCSVFileName);
                executeImportCSV(sJobFile, oJob, input);

                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
            }
            catch (Exception e)
            {
                Log.Error(e, "ImportCSV failed for job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Import CSV failed: " + e.Message;
                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
                return false;
            }

            return oJob.Status == Constants.JobProcessingStatus.Completed.ToString();
        }

        protected string downloadCSVFile(importcsvinput input)
        {
            string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
            string sCSVFileName = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".csv");

            string sImportCSVUrlXml = input.ImportCSVUrlXml;
            string bucket = Constants.DEFAULT_DOCS_LOCATION;
            int startindex = sImportCSVUrlXml.IndexOf(bucket) + bucket.Length + 1;
            string sKeyName = sImportCSVUrlXml.Substring(startindex);

            new DCS3Services().downloadFile(bucket, sKeyName, sCSVFileName);
            return sCSVFileName;
        }

        protected string generateJobFile(job oJob, importcsvinput input, string sCSVFileName)
        {
            string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
            string sJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");

            XmlDocument doc = new XmlDocument();
            XmlDeclaration decl = doc.CreateXmlDeclaration("1.0", "utf-8", null);
            XmlElement root = doc.CreateElement("job");
            root.SetAttribute("name", "ImportCSV");
            doc.InsertBefore(decl, doc.DocumentElement);
            doc.AppendChild(root);

            // input.InputFileName/OutputDirectory carry the ADMIN's repository layout
            // (getDocumentPathDB → D:\DCatalog\Docs, the legacy DocProcessor disk) — the
            // same dcproxy exit -1 failure as RecognizeLinks. Localize to this worker's
            // paths, which is also where downloadFiles stages the document.
            string sLocalInputFile = "";
            string sLocalOutputDir;
            if (input.Document != null)
            {
                sLocalOutputDir = DocumentUtilBase.getDocumentPath(input.Document);
                if (!string.IsNullOrEmpty(input.Document.PDFFileName))
                    sLocalInputFile = Path.Combine(sLocalOutputDir, input.Document.PDFFileName);
            }
            else
            {
                sLocalOutputDir = core.Common.PublicationUtil.getPublicationPath(input.Publication);
            }
            Append(doc, root, "inputfile", sLocalInputFile);
            Append(doc, root, "outputdir", sLocalOutputDir);
            if (input.Document != null)
                Append(doc, root, "docid", input.Document.Id.ToString());
            Append(doc, root, "importproductstodb", input.importproductstodb ? "1" : "0");

            if (input.importasproduct)
                Append(doc, root, "isproduct", "1");
            else
            {
                Append(doc, root, "link_type", input.link_type.ToString());
                Append(doc, root, "isproduct", "0");
            }

            Append(doc, root, "updatecatalogs", input.updatealldocumentsinpublication ? "1" : "0");
            Append(doc, root, "deletepreviouslinks", input.deletepreviouslinks ? "1" : "0");
            Append(doc, root, "publicationid",
                (input.Document != null ? input.Document.Publication.Id : input.Publication.Id).ToString());

            if (input.userange)
            {
                Append(doc, root, "userange", "1");
                if (input.frompage > 0)
                {
                    Append(doc, root, "frompage", input.frompage.ToString());
                    Append(doc, root, "topage", input.topage.ToString());
                }
            }
            else
            {
                Append(doc, root, "userange", "0");
            }

            if (!string.IsNullOrEmpty(input.link_color))
            {
                Append(doc, root, "link_color", input.link_color?.Replace("#", ""));
                Append(doc, root, "link_opacity", input.link_opacity.ToString());
                Append(doc, root, "border_type", input.border_type.ToString());
                Append(doc, root, "border_color", input.border_color?.Replace("#", ""));
                Append(doc, root, "border_width", input.border_width.ToString());
            }

            Append(doc, root, "csvfile", sCSVFileName);
            Append(doc, root, "usejson", "1");

            root.SetAttribute("id", oJob.Id.ToString());
            doc.Save(sJobFile);
            return sJobFile;
        }

        private static void Append(XmlDocument doc, XmlElement parent, string name, string value)
        {
            XmlElement el = doc.CreateElement(name);
            el.InnerText = value ?? "";
            parent.AppendChild(el);
        }

        public void executeImportCSV(string sFileName, job oJob, importcsvinput input)
        {
            try
            {
                downloadFiles(input);

                string sProcessName = Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
                Process oPDFProcess = new Process();
                oPDFProcess.StartInfo = new ProcessStartInfo(sProcessName)
                {
                    Arguments = sFileName,
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Hidden
                };
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
                    uploadGeneratedFile(oJob, input);
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
                Log.Error(ex, "Exception in executeImportCSV");
            }
        }

        // Downloads the single document's files, and (when importing products to catalog) every
        // document in the publication.
        public void downloadFiles(importcsvinput input)
        {
            document oDocument = input.Document;
            if (oDocument != null)
            {
                nFromPage = 1;
                nToPage = oDocument.NumberOfPages;
                if (bUseRange && input.frompage > 0)
                {
                    nFromPage = input.frompage;
                    nToPage = input.topage;
                }
                downloadDocumentFiles(oDocument);
            }

            if (importproductstodb && updatecatalogs)
            {
                string pubId = (oDocument != null ? oDocument.Publication.Id : input.Publication.Id).ToString();
                sPubliactionId = pubId;
                publication pub = _context.publication
                    .Include(p => p.Publisher)
                    .Include(p => p.Documents)
                    .Where(p => p.Id == pubId)
                    .FirstOrDefault();
                if (pub != null)
                    downloadPublicationFiles(pub);
            }
        }

        public void downloadPublicationFiles(publication pub)
        {
            foreach (document d in pub.Documents)
            {
                if (!d.Deleted)
                {
                    nFromPage = 1;
                    nToPage = d.NumberOfPages;
                    try { downloadDocumentFiles(d, pub); }
                    catch (Exception ex) { Log.Warning("download doc {Id} skipped: {Msg}", d.Id, ex.Message); }
                }
            }
        }

        public void downloadDocumentFiles(document oDocument, publication pub = null)
        {
            publisher publisher = pub != null ? pub.Publisher : oDocument.Publication.Publisher;
            string pubName = pub != null ? pub.Name : oDocument.Publication.Name;
            sBucketName = string.IsNullOrEmpty(publisher.BucketName) ? Constants.DEFAULT_DOCS_LOCATION : publisher.BucketName;

            string sPublisherName = Utility.GenerateFriendlyURL(publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(pubName);
            sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            if (Directory.Exists(sDocumentPath))
            {
                try { Directory.Delete(sDocumentPath, true); }
                catch (Exception ex1) { Log.Warning("clean doc folder {Path}: {Msg}", sDocumentPath, ex1.Message); }
            }
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);

            DCS3Services oDCS3Services = new DCS3Services();
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

        public void uploadGeneratedFile(job oJob, importcsvinput input)
        {
            if (importproductstodb && updatecatalogs)
            {
                publication pub = _context.publication
                    .Include(p => p.Publisher)
                    .Include(p => p.Documents)
                    .Where(p => p.Id == sPubliactionId)
                    .FirstOrDefault();
                if (pub != null)
                {
                    foreach (document d in pub.Documents)
                    {
                        if (!d.Deleted)
                            uploadGenerateFileForDocument(d, pub, true);
                    }
                }
            }
            else if (input.Document != null)
            {
                uploadGenerateFileForDocument(input.Document, input.Document.Publication, false);
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

        // Airgas-specific: derive each page's dominant category/subcategory from the CSV (joined to
        // type-7 product links) and write them into document.json's PageCategory array.
        private void updatecategoriesAirGas(document oDocument, string sCSVFile)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
            Hashtable CategoriesHash = new Hashtable();

            CsvConfiguration cnf = new CsvConfiguration(CultureInfo.InvariantCulture);
            using (var reader = new StreamReader(sCSVFile))
            using (var csv = new CsvReader(reader, cnf))
            {
                var records = csv.GetRecords<dynamic>();
                foreach (ExpandoObject obj in records)
                {
                    var dict = (IDictionary<string, object>)obj;
                    string sCode = (string)dict["# code"];
                    if (!CategoriesHash.ContainsKey(sCode))
                        CategoriesHash.Add(sCode, dict);
                }
            }

            int NumberOfPages = oDocument.NumberOfPages;
            List<Dictionary<string, string>> oPages = new List<Dictionary<string, string>>();
            for (int i = 1; i < NumberOfPages; i++)
            {
                string sPageName = string.Format("Page_{0}.json", i + 1);
                string JsonPagePath = Path.Combine(sOutputDirectory, sPageName);
                if (!File.Exists(JsonPagePath)) continue;
                JObject pageJson = JObject.Parse(System.IO.File.ReadAllText(JsonPagePath));
                IEnumerable<JToken> pagelinks = pageJson.SelectTokens("..links.link..@attributes");

                Hashtable Categories = new Hashtable();
                Hashtable SubCategories = new Hashtable();
                foreach (JToken pagelink in pagelinks)
                {
                    JValue otype = (JValue)pagelink["type"];
                    if ((string)otype.Value != "7") continue;
                    string sProductID = (string)((JValue)pagelink["productid"]).Value;
                    if (CategoriesHash.ContainsKey(sProductID))
                    {
                        var dict = (IDictionary<string, object>)CategoriesHash[sProductID];
                        string sCategory = (string)dict["Web Cat 1"];
                        string sSubCategory = (string)dict["Web Cat 2"];
                        Categories[sCategory] = Categories.ContainsKey(sCategory) ? (int)Categories[sCategory] + 1 : 1;
                        SubCategories[sSubCategory] = SubCategories.ContainsKey(sSubCategory) ? (int)SubCategories[sSubCategory] + 1 : 1;
                    }
                }

                string sCurrentCategory = "";
                string sCurrentSubCategory = "";
                int nMax = 0;
                foreach (DictionaryEntry c in Categories)
                {
                    if (sCurrentCategory == "") { nMax = (int)c.Value; sCurrentCategory = (string)c.Key; }
                    else if (nMax < (int)c.Value) { nMax = (int)c.Value; sCurrentCategory = (string)c.Key; }

                    nMax = 0;
                    foreach (DictionaryEntry c2 in SubCategories)
                    {
                        if (sCurrentSubCategory == "") { nMax = (int)c2.Value; sCurrentSubCategory = (string)c2.Key; }
                        else if (nMax < (int)c2.Value) { nMax = (int)c2.Value; sCurrentSubCategory = (string)c2.Key; }
                    }
                }

                oPages.Add(new Dictionary<string, string>
                {
                    ["page"] = i.ToString(),
                    ["category"] = sCurrentCategory,
                    ["subcategory"] = sCurrentSubCategory
                });
            }

            string docfilepath = Path.Combine(sOutputDirectory, "document.json");
            JObject docJson = JObject.Parse(System.IO.File.ReadAllText(docfilepath));
            docJson["issue"]["PageCategory"] = JArray.FromObject(oPages);
            System.IO.File.WriteAllText(docfilepath, docJson.ToString());

            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            oDCS3Services.uploadFile(Constants.DEFAULT_DOCS_LOCATION, docfilepath, sKeyPrefix);
        }
    }
}
