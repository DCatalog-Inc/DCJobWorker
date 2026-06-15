using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Native port of the legacy WebPublisher JobExecutionDeleteAll (whose file confusingly
    // declared "class JobExecutionAddAI"). Bulk-removes links / products / video / audio / image
    // items from a document's Page_N.json on S3 per the deleteitemsinput filters, then re-uploads
    // the changed pages + bumps their version in document.json so the viewer reloads them.
    public class JobExecutionDeleteAll : IJobExecution
    {
        private readonly ApplicationDbContext _context;

        public JobExecutionDeleteAll(ApplicationDbContext context)
        {
            _context = context;
        }

        protected string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
        protected string sKeyPrefix = "";
        protected string sDocumentPath = "";
        protected int nFromPage = 1;
        protected int nToPage = 1;
        protected bool[] arrdirty = Array.Empty<bool>();

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            Log.Information("JobExecutionDeleteAll job {JobId}", oJob.Id);
            try
            {
                var input = await _context.deleteitemsinput
                    .Include(c => c.Job)
                    .Include(c => c.Document)
                    .Include(c => c.Document.Publication)
                    .Include(c => c.Document.Publication.Publisher)
                    .Where(c => c.Job.Id == oJob.Id)
                    .FirstOrDefaultAsync(ct);

                if (input == null || input.Document == null)
                {
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "No deleteitemsinput/document for job";
                    _context.Update(oJob);
                    await _context.SaveChangesAsync(ct);
                    return false;
                }

                document oDocument = input.Document;
                downloadFiles(oDocument, input);
                deleteItems(input);
                uploadUpdatedFile(oDocument);

                oJob.Progress = 100;
                oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                oJob.Desctiption = "Completed";
                _context.Update(oJob);
                await _context.SaveChangesAsync(ct);
            }
            catch (Exception e)
            {
                Log.Error(e, "DeleteAll failed for job {JobId}", oJob.Id);
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = "Delete all failed: " + (e.Message.Length > 400 ? e.Message.Substring(0, 400) : e.Message);
                _context.Update(oJob);
                try { await _context.SaveChangesAsync(ct); } catch { /* best-effort */ }
                return false;
            }

            return oJob.Status == Constants.JobProcessingStatus.Completed.ToString();
        }

        private void downloadFiles(document oDocument, deleteitemsinput input)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName)
                ? Constants.DEFAULT_DOCS_LOCATION : oDocument.Publication.Publisher.BucketName;
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName, oDocument.Id);
            sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);

            nFromPage = 1;
            nToPage = oDocument.NumberOfPages;
            arrdirty = new bool[Math.Max(oDocument.NumberOfPages, 0)];
            if (!input.allpages)
            {
                nFromPage = input.frompage;
                nToPage = input.topage;
            }

            for (int i = nFromPage; i <= nToPage; i++)
            {
                string sPageName = string.Format("Page_{0}.json", i);
                try { oDCS3Services.downloadFile(sBucketName, string.Format("{0}/{1}", sKeyPrefix, sPageName), Path.Combine(sDocumentPath, sPageName)); }
                catch (Exception ex) { Log.Warning("DeleteAll page download skipped {Page}: {Msg}", sPageName, ex.Message); }
            }
            try
            {
                oDCS3Services.downloadFile(sBucketName, string.Format("{0}/{1}", sKeyPrefix, "document.json"),
                    Path.Combine(sDocumentPath, "document.json"));
            }
            catch (Exception ex) { Log.Warning("DeleteAll document.json download skipped: {Msg}", ex.Message); }
        }

        private void deleteItems(deleteitemsinput input)
        {
            // legacy JSON link "type" codes per item kind
            var linkSubTypes = new (int jsonType, int subTypeBit)[]
            {
                (0, (int)Constants.DeleteItemType.DeleteItemTypeExternalLinks),
                (1, (int)Constants.DeleteItemType.DeleteItemTypeEmailLinks),
                (2, (int)Constants.DeleteItemType.DeleteItemTypeGotoPage),
                (3, (int)Constants.DeleteItemType.DeleteItemTypeVideoPopup),
                (4, (int)Constants.DeleteItemType.DeleteItemTypeImagePopup),
                (5, (int)Constants.DeleteItemType.DeleteItemTypeHtmlPopup),
                (6, (int)Constants.DeleteItemType.DeleteItemTypePhone),
            };

            for (int i = nFromPage; i <= nToPage; i++)
            {
                string sFullFileName = Path.Combine(sDocumentPath, string.Format("Page_{0}.json", i));
                if (!File.Exists(sFullFileName))
                    continue;

                JObject pageJson = JObject.Parse(File.ReadAllText(sFullFileName));
                List<JToken> removeList = new List<JToken>();
                bool bDirty = false;

                if (input.itemtype == 1) // Links
                {
                    foreach (var (jsonType, subTypeBit) in linkSubTypes)
                    {
                        if (input.itemsubtype == 0 || (input.itemsubtype & subTypeBit) > 0)
                            collectMatchingLinks(pageJson, jsonType, input, removeList, ref bDirty, i);
                    }
                }

                if ((input.itemtype & (int)Constants.DeleteItemType.DeleteItemTypeProduct) > 0)
                    collectMatchingLinks(pageJson, 7, input, removeList, ref bDirty, i);

                if ((input.itemtype & (int)Constants.DeleteItemType.DeleteItemTypeVideo) > 0)
                    clearMediaArray(pageJson, "..links.video", ref bDirty, i);
                if ((input.itemtype & (int)Constants.DeleteItemType.DeleteItemTypeAudio) > 0)
                    clearMediaArray(pageJson, "..links.audio", ref bDirty, i);
                if ((input.itemtype & (int)Constants.DeleteItemType.DeleteItemTypeImage) > 0)
                    clearMediaArray(pageJson, "..links.image", ref bDirty, i);

                foreach (JToken el in removeList)
                    el.Remove();
                if (bDirty)
                    File.WriteAllText(sFullFileName, pageJson.ToString());
            }
        }

        private void collectMatchingLinks(JObject pageJson, int jsonType, deleteitemsinput input,
            List<JToken> removeList, ref bool bDirty, int pageIndex)
        {
            string selectsequence = "$..link[?(@..type=='" + jsonType + "')]";
            IEnumerable<JToken> jLinks = pageJson.SelectTokens(selectsequence);
            foreach (JToken item in jLinks)
            {
                JToken attrItem = item["@attributes"];
                if (attrItem == null)
                    continue;
                bool coordEmpty = isNullOrEmpty(attrItem["coordinates"]);
                if (input.itemshape == 1 && !coordEmpty)   // 1 = rect (has coordinates)
                    continue;
                if (input.itemshape == 2 && coordEmpty)     // 2 = polygon (no coordinates)
                    continue;
                string linkId = attrItem["link_id"]?.Value<string>() ?? "";
                bool isNumberId = Regex.IsMatch(linkId, @"^\d+$");
                if (input.itemcreationtype == 1 && !isNumberId)   // 1 = manual (numeric id)
                    continue;
                if (input.itemcreationtype == 2 && isNumberId)    // 2 = automatic
                    continue;
                removeList.Add(item);
                bDirty = true;
                if (pageIndex - 1 < arrdirty.Length) arrdirty[pageIndex - 1] = true;
            }
        }

        private void clearMediaArray(JObject pageJson, string path, ref bool bDirty, int pageIndex)
        {
            JArray arr = pageJson.SelectToken(path) as JArray;
            if (arr != null)
            {
                arr.Clear();
                bDirty = true;
                if (pageIndex - 1 < arrdirty.Length) arrdirty[pageIndex - 1] = true;
            }
        }

        // Mirrors the legacy JsonExtensions.IsNullOrEmpty (not present in the new core).
        private static bool isNullOrEmpty(JToken token)
        {
            return token == null
                || token.Type == JTokenType.Null
                || (token.Type == JTokenType.String && string.IsNullOrEmpty(token.Value<string>()))
                || (token.Type == JTokenType.Array && !token.HasValues)
                || (token.Type == JTokenType.Object && !token.HasValues);
        }

        private void uploadUpdatedFile(document oDocument)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            bool bDirty = false;

            for (int i = nFromPage; i <= nToPage; i++)
            {
                if (i - 1 >= arrdirty.Length || !arrdirty[i - 1])
                    continue;
                string sFullFileName = Path.Combine(sDocumentPath, string.Format("Page_{0}.json", i));
                if (File.Exists(sFullFileName))
                {
                    oDCS3Services.uploadFile(sBucketName, sFullFileName, sKeyPrefix);
                    bDirty = true;
                }
            }

            if (!bDirty)
                return;

            // Bump the version of each changed page in document.json so the viewer reloads them,
            // then upload document.json once. (Legacy called updatedocjsonversion + uploadJsonFiles;
            // the new per-page version scheme replaces the global bump.)
            string sDocJson = Path.Combine(sDocumentPath, "document.json");
            if (File.Exists(sDocJson))
            {
                try
                {
                    JObject docJson = JObject.Parse(File.ReadAllText(sDocJson));
                    for (int i = nFromPage; i <= nToPage; i++)
                        if (i - 1 < arrdirty.Length && arrdirty[i - 1])
                            JobWorker.DocumentConvertor.updatedocjsonversion(docJson, i);
                    File.WriteAllText(sDocJson, docJson.ToString());
                }
                catch (Exception ex) { Log.Warning("DeleteAll version bump skipped: {Msg}", ex.Message); }

                oDCS3Services.uploadFile(sBucketName, sDocJson, sKeyPrefix);
            }
        }
    }
}
