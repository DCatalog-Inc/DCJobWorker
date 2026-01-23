using core.Models;
using Core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json.Linq;
using System.Collections;
using System.Text;

namespace JobWorker
{
    public class AIEmbeddingService
    {
        public AIEmbeddingService()
        {
        }

        public async Task<int> DeletePageFromDocumentAsync(
            ApplicationDbContext db,
            string documentId,
            int pageNumber,
            CancellationToken ct = default)
        {
            return await db.PageEmbedding
                .Where(p => EF.Property<string>(p, "Document_id") == documentId &&
                            p.PageNumber == pageNumber)
                .ExecuteDeleteAsync(ct);
        }

        public async Task<int> DeleteEmbeddingForDocumentAsync(
            ApplicationDbContext db,
            string documentId,
            CancellationToken ct = default)
        {
            return await db.PageEmbedding
               .Where(p => EF.Property<string>(p, "Document_id") == documentId)
               .ExecuteDeleteAsync(ct);
        }

        public async Task addDocumentToAIAsync(
            ApplicationDbContext db,
            document oDocument,
            CancellationToken ct = default)
        {
            int nNumberOfPages = oDocument.NumberOfPages;
            string sFullFileName = string.Empty;

            string sAPIKey = oDocument.Publication.Publisher.ConvertSettings.OpenAILicenseKey;

            // async DB call instead of sync SingleOrDefault()
            string? sDCatalogAIKey = await db.serversettings
                .Where(s => s.Name == "DCatalogAIKey")
                .Select(s => s.Value)
                .SingleOrDefaultAsync(ct);

            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);

            sAPIKey = string.IsNullOrEmpty(sAPIKey) ? sDCatalogAIKey : sAPIKey;
            EmbeddingService oEmbeddingService = new EmbeddingService(sAPIKey);

            // CRITICAL: await this so it doesn't run in parallel on the same DbContext
            //await DeleteEmbeddingForDocumentAsync(db, oDocument.Id, ct);

            ArrayList? DocPages = null;

            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string sDocumentURL = DocumentUtilBase.getDocumentRepositoryURL(oDocument);

            string docname = "document.json";
            string sDocumentJson = Path.Combine(sDocumentPath, docname);

            if (File.Exists(sDocumentJson))
            {
                // optional: can use ReadAllTextAsync if you want
                string jsonText = File.ReadAllText(sDocumentJson);
                JObject documentJson = JObject.Parse(jsonText);
                DocPages = DocumentUtilBase.getpagelabels(documentJson);
            }

            for (int i = 1; i <= nNumberOfPages; i++)
            {
                string sPageName = $"Page_{i}.txt";
                sFullFileName = Path.Combine(sOutputDirectory, "html", sPageName);

                try
                {
                    if (!File.Exists(sFullFileName))
                        continue;

                    using (StreamReader reader = new StreamReader(sFullFileName, Encoding.UTF8))
                    {
                        var content = await reader.ReadToEndAsync();
                        var embedding = await oEmbeddingService.GetEmbeddingAsync(content);

                        string sDocumentLinkURL = DocumentUtilBase.getDocumentURLHTML5(oDocument);
                        string sPageThumb = $"{sDocumentURL}/Thumbnail_{i}.jpg";

                        var record = new PageEmbedding
                        {
                            Filename = sPageName,
                            PageNumber = i,
                            Content = content,
                            Embedding = embedding,
                            Document = oDocument,
                            DocumentTitle = oDocument.TitleForURL,
                            DocumentURL = sDocumentLinkURL,
                            PageThumbnail = sPageThumb,
                            PageLabel = DocPages == null ? i.ToString()
                                                              : DocPages[i - 1].ToString()
                        };

                        db.Add(record);   // SaveChangesAsync happens outside
                    }
                }
                catch (Exception ex)
                {
                    // at least log – don’t swallow silently
                    // _log.LogError(ex, "Failed to add embedding for doc {DocId}, page {Page}", oDocument.Id, i);
                }
            }
        }

        public async Task addPageToAIAsync(
            ApplicationDbContext db,
            document oDocument,
            int nPageNumber,
            CancellationToken ct = default)
        {
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string sHTMLFolder = Path.Combine(sDocumentPath, "html");
            string content = TextExtract.getPageTextFS(sHTMLFolder, nPageNumber);

            string sAPIKey = oDocument.Publication.Publisher.ConvertSettings.OpenAILicenseKey;

            string? sDCatalogAIKey = await db.serversettings
                .Where(s => s.Name == "DCatalogAIKey")
                .Select(s => s.Value)
                .SingleOrDefaultAsync(ct);

            sAPIKey = string.IsNullOrEmpty(sAPIKey) ? sDCatalogAIKey : sAPIKey;
            EmbeddingService oEmbeddingService = new EmbeddingService(sAPIKey);

            var embedding = await oEmbeddingService.GetEmbeddingAsync(content);

            string sPageName = $"Page_{nPageNumber}.txt";

            var record = new PageEmbedding
            {
                Filename = sPageName,
                PageNumber = nPageNumber,
                Content = content,
                Embedding = embedding,
                Document = oDocument
            };

            db.Add(record); // caller saves
        }
    }
}
