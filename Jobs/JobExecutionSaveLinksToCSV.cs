using Amazon.Extensions.NETCore.Setup;
using core;
using core.Models;
using Core;
using Core.Models;
using CsvHelper;
using DCatalogCommon.Data;
using Hangfire.Logging;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Asn1.IsisMtt.X509;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;
using static DCJobs.ImnaseProductsExport;

namespace JobWorker.Jobs
{
    internal class JobExecutionSaveLinksToCSV : IJobExecution
    {
        private readonly ApplicationDbContext _context;

        // IMNASA's manual "Save Links to CSV" export must match their nightly FTP feed
        // (DCJobs.ImnaseProductsExport): type-0 links only, SKU = last URL path segment, 4-digit
        // zero-padded page, raw URL. Gate that FTP-format branch on their publisher id. Everyone
        // else keeps the generic format (type 0/5, sku from ?sku=, normalized URL, plain page).
        private const string ImnasaPublisherId = "16fb4a88-445c-49be-ad06-73edfa980ec2";

        public JobExecutionSaveLinksToCSV(ApplicationDbContext context)
        {
            _context = context;
        }
        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            try
            {
                var savelinkstocsv = await _context.savelinkstocsvinput
                                  .Include(c => c.Job)
                                  .Include(c => c.Document)  // eager load Job if needed
                                   .Include(c => c.Document.Publication)  // eager load Job if needed
                                   .Include(c => c.Document.Publication.PublicationTemplate)
                                   .Include(c => c.Document.Publication.Publisher)  // eager load Job if needed
                                   .Include(c => c.Document.Publication.Publisher.Licenses)  // eager load Job if needed
                                  .Where(c => c.Job.Id == oJob.Id)
                                  .FirstOrDefaultAsync();
                document doc = savelinkstocsv.Document;
                oJob.Progress = 10;
                _context.Update(oJob);
                await _context.SaveChangesAsync();   // ✅ persist progress early
                await downloadJsonPagesAsync(doc, savelinkstocsv, oJob);
                oJob.Progress = 100;
                await _context.SaveChangesAsync();   // ✅ persist progress early
                oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                _context.Update(oJob);

            }
            catch (Exception e)
            {
                // Don't swallow: a silent catch here made failed runs report "Completed"
                // with csvurl = NULL, so the UI never offered the CSV download.
                Console.WriteLine($"# SaveLinksToCSV failed for job {oJob.Id}: {e}");
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Save links to CSV failed: " + e.Message;
                _context.Update(oJob);
                await _context.SaveChangesAsync();
                return false;
            }


            return true;
        }



        string NormalizeUrl(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;

            raw = raw.Trim();

            // If it's like "LMCTRUCK.com" or "www.foo.com", add scheme
            if (!raw.Contains("://"))
                raw = "https://" + raw;

            return raw;
        }

        public async Task<bool> downloadJsonPagesAsync(document doc, savelinkstocsvinput savelinkstocsv, job oJob)
        {
            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
            int numofpages = doc.NumberOfPages;
            string sDocumentPath = DocumentUtilBase.getDocumentPath(doc);
            if (System.IO.Directory.Exists(sDocumentPath))
                Directory.Delete(sDocumentPath, true);
            System.IO.Directory.CreateDirectory(sDocumentPath);
            DCS3Services dcs3services = new DCS3Services();
            string sDocumentRelativeURL = DocumentUtilBase.getDocumentRelativeURL(doc);

            string sPublisherName = Utility.GenerateFriendlyURL(doc.Publication.Publisher.Name);
            string sPublicationId = doc.Publication.Id.ToString();
            string sPublicationName = Utility.GenerateFriendlyURL(doc.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                  doc.Id);

            bool bImnasaFormat = string.Equals(doc.Publication?.Publisher?.Id, ImnasaPublisherId, StringComparison.OrdinalIgnoreCase);
            var records = new List<ImnaseCSVHeader>();
            int lastProgress = oJob.Progress;
            //numofpages = 20;
            for (int i = 1; i <= numofpages; i++)
            {

                // progress from 10..95 while processing pages (leave 100 for the end)
                int progress = 10 + (int)Math.Round((i * 85.0) / numofpages);
                if (progress != lastProgress && (i % 2 == 0 || i == numofpages))
                {
                    lastProgress = progress;
                    oJob.Progress = progress;
                    _context.Update(oJob);
                    await _context.SaveChangesAsync();
                }


                string sPageName = string.Format("Page_{0}.json", i);
                sPageName = string.Format("Page_{0}.json", i);
                string sPagePathSource = string.Format("{0}/{1}", sKeyPrefix, sPageName);
                string sFullFileName = string.Format("{0}\\{1}", sDocumentPath, sPageName);
                try
                {
                    dcs3services.downloadFile(sBucketName, sPagePathSource, sFullFileName);
                }
                catch (Exception)
                {
                    //_logger.LogError("# download Json failed: " + ex.Message);
                }

                JObject pageJson = JObject.Parse(System.IO.File.ReadAllText(sFullFileName));
                // IMNASA: type-0 links only (matches the FTP feed). Generic: type 0 and 5.
                string selectsequence = bImnasaFormat
                    ? "$..link[?(@['@attributes'].type == '0')]"
                    : "$..link[?(@['@attributes'].type == '0' || @['@attributes'].type == '5')]";
                IEnumerable<JToken> jLinks = pageJson.SelectTokens(selectsequence);
                foreach (JToken item in jLinks)
                {
                    JValue linkurl = (JValue)item["@attributes"]["url"];
                    string slinkurl = linkurl.ToString();

                    if (bImnasaFormat)
                    {
                        // Byte-compatible with ImnaseProductsExport: SKU = last URL path segment,
                        // page = 4-digit zero-padded, raw URL (no normalization).
                        string sSku = string.IsNullOrEmpty(slinkurl)
                            ? slinkurl
                            : slinkurl.Substring(slinkurl.LastIndexOf("/") + 1);
                        records.Add(new ImnaseCSVHeader { SKU = sSku, PageNumber = i.ToString("D4"), URL = slinkurl });
                        continue;
                    }

                    string sProductName = "";
                    if (!string.IsNullOrEmpty(slinkurl))
                    {
                        var normalized = NormalizeUrl(slinkurl);
                        var uri = new Uri(normalized);
                        var query = System.Web.HttpUtility.ParseQueryString(uri.Query);
                        sProductName = query["sku"];   // e.g. "38-7505"
                    }

                    string sPageNumber = i.ToString();
                    // Fallback if sku not found (just in case)
                    if (string.IsNullOrEmpty(sProductName))
                    {
                        records.Add(new ImnaseCSVHeader { SKU = slinkurl, PageNumber = sPageNumber, URL = slinkurl });
                    }
                    else
                    {
                        records.Add(new ImnaseCSVHeader { SKU = sProductName, PageNumber = sPageNumber, URL = slinkurl });
                    }
                }
            }
            string sOutputFolder = Path.GetTempPath();
            System.IO.Directory.CreateDirectory(sOutputFolder);
            var sFileName = $"Links_{DateTime.Now:dd_MM_yyyy}_{DateTime.Now.Ticks}.csv";
            string sCSVFullFileName = Path.Combine(sOutputFolder, sFileName);
            using (var writer = new StreamWriter(sCSVFullFileName))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteRecords(records);
            }
            string sURL = dcs3services.uploadFile(sBucketName, sCSVFullFileName, sKeyPrefix);
            savelinkstocsv.csvurl= sURL;
            await _context.SaveChangesAsync(); // persist csvurl — the Progress endpoint reads it for the download link
            return true;

        }

    }
}
