#pragma warning disable CA1416
using Amazon.Util.Internal;
using Core;
using Core.Models;
using DCatalogCommon;
using DCatalogCommon.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using Microsoft.Extensions.Logging;
using System.Xml;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.Drawing;
using System.Net;
using Nest;
using MySqlX.XDevAPI;
using iText.Kernel.Pdf;
using iText.Kernel.Geom;
using System.IO;
using iText.Kernel.Utils;
using iText.Forms;
using Elasticsearch.Net;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using core.Models.Convertor;
using Image = core.Models.Convertor.Image;
using Hangfire.Logging;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using core.Common;
using static QRCoder.PayloadGenerator;
using core.Models;
using System.Text.Json;
using System.Globalization;
using Org.BouncyCastle.Asn1.Ocsp;
using System.Text.Json.Serialization;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace JobWorker
{
    public class DocumentConvertor
    {
        private readonly ILogger _logger;

        protected string _DocumentXML;
        protected iText.Kernel.Pdf.PdfDocument _PDFDoc = null;
        protected bool _docencrypted = false;
        protected string _InputFileName;
        protected bool _bUseVectorText = true;
        protected int _nImageQuality = 85;
        protected job _CurrentJob = null;
        protected int _nFirstThumbHeight;
        protected ArrayList arrPagesID;
        protected bool _bOptimizeSWF = false;
        protected int _TOCGotoPage = 0;
        protected string _CallbackURL = "";
        protected string _OrderId = "";
        protected bool _importPageLabel = false;

        //Random number generation.
        private static readonly Random random = new Random();
        private static readonly object syncLock = new object();

        protected AWSFileUpload _awsFileUpload { get; set; }

        public DocumentConvertor(bool bInit = true)
        {
        }

        public void release()
        {
            if (_PDFDoc != null)
            {
                _PDFDoc = null;
            }
        }

        public static string GetDocumentAsJson(
        ApplicationDbContext context,
        XmlDocument docXml,
        bool relative = true)
        {
            if (docXml is null) throw new ArgumentNullException(nameof(docXml));

            // Helpers
            static string Attr(XmlNode n, string name, string fallback = "")
                => n?.Attributes?[name]?.Value ?? fallback;

            static double AttrDouble(XmlNode n, string name, double fallback = 0)
            {
                var s = n?.Attributes?[name]?.Value;
                return double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var v) ? v : fallback;
            }

            var jsonOptions = new JsonSerializerOptions
            {
                IncludeFields = true,        // serialize public fields on your DTOs
                PropertyNamingPolicy = null, // keep your original field names (e.g., issueName, docid)
                WriteIndented = false
            };
            string J(object o) => System.Text.Json.JsonSerializer.Serialize(o, jsonOptions);

            var docNode = docXml.SelectSingleNode("//doc")
                         ?? throw new InvalidOperationException("Missing //doc node in XML.");

            var documentId = Attr(docNode, "id");
            if (string.IsNullOrEmpty(documentId))
                throw new InvalidOperationException("The //doc node must have an id attribute.");

            // EF Core (ThenInclude) + Async
            document? oDocument = context.document
                  .Include(d => d.Publication)
                  .Include(d => d.Publication.Publisher)
                  .Include(d => d.Publication.Publisher.ConvertSettings)
                  .Where(d => d.Id == documentId)
                  .SingleOrDefault();

            if (oDocument is null)
                throw new InvalidOperationException($"Document '{documentId}' not found in DB.");

            var repositoryUrl = DocumentUtilBase.getDocumentRepositoryURL(oDocument);

            // Build PDF URL
            var pdfIsAbsolute = Uri.TryCreate(oDocument.PDFFileName, UriKind.Absolute, out _)
                                || oDocument.PDFFileName.StartsWith("//", StringComparison.Ordinal);

            var pdfUrl = relative
                ? oDocument.PDFFileName
                : Utility.CombineURL(repositoryUrl, oDocument.PDFFileName);

            var pdfFileUrl = pdfIsAbsolute ? oDocument.PDFFileName : pdfUrl;

            // Root DTO
            var oDocument_html5 = new document_html5
            {
                issueName = Attr(docNode, "title"),
                docid = documentId
            };

            // Pages
            var pageNodes = docXml.SelectNodes("//page");
            double minImgW = 0, minImgH = 0, imgRatio = 0;

            var pagesSb = new StringBuilder();
            var nPageCount = pageNodes?.Count ?? 0;

            for (int i = 0; i < nPageCount; i++)
            {
                if (i > 0) pagesSb.Append(',');

                var xmlPage = pageNodes![i];

                bool isIntro = Attr(xmlPage, "intro") == "1";
                bool recolor = Attr(xmlPage, "recolor") == "1";

                // Prefer hiimage; otherwise use normal (and 2x, per your original logic)
                var xmlHi = xmlPage.SelectSingleNode("hiimage");
                double imgW, imgH;

                if (xmlHi != null)
                {
                    imgW = AttrDouble(xmlHi, "width");
                    imgH = AttrDouble(xmlHi, "height");
                    if (isIntro)
                    {
                        // Intro pages don't affect min dims per original code
                    }
                }
                else
                {
                    var xmlNormal = xmlPage.SelectSingleNode("normal")
                                   ?? throw new InvalidOperationException($"Page node {i + 1} missing <normal>.");
                    imgW = AttrDouble(xmlNormal, "width") * 2;
                    imgH = AttrDouble(xmlNormal, "height") * 2;
                }

                if (minImgW == 0)
                {
                    minImgW = imgW; minImgH = imgH; imgRatio = minImgW / minImgH;
                }
                else
                {
                    var r = imgW / imgH;
                    if (imgRatio > r) { minImgW = imgW; minImgH = imgH; imgRatio = r; }
                }

                var page = new page_html5();
                page.id = Attr(xmlPage, "id");

                // label fallback to num
                var label = Attr(xmlPage, "label");
                var num = Attr(xmlPage, "num");
                page.name = !string.IsNullOrEmpty(label) ? label : num;

                page.version = Attr(xmlPage, "ver", "1");
                page.sequence = num;

                page.contentType = "";
                page.height = imgH.ToString(CultureInfo.InvariantCulture);
                page.width = imgW.ToString(CultureInfo.InvariantCulture);

                var pageData = $"Page_{num}.json";
                var thumb = $"Thumbnail_{num}.jpg";
                var pageImg = $"ZPage_{num}.jpg";
                var vector = $"SPage_{num}.svg";

                page.thumb = relative ? thumb : Utility.CombineURL(repositoryUrl, thumb);
                page.iphoneImage = relative ? pageImg : Utility.CombineURL(repositoryUrl, pageImg);
                page.svgimg = relative ? vector : Utility.CombineURL(repositoryUrl, vector);

                page.isintropage = isIntro ? "1" : "0";
                page.recolor = recolor ? "1" : "0";

                pagesSb.Append("{\"@attributes\": ");
                pagesSb.Append(J(page));
                pagesSb.Append('}');
            }

            // Background from publication template
            var tpl = oDocument.Publication?.PublicationTemplate
                      ?? throw new InvalidOperationException("PublicationTemplate is missing.");

            var bg = new background_html5
            {
                backgroundmode = Convert.ToString(tpl.bgImgDisplayMode, CultureInfo.InvariantCulture),
                startcolor = (tpl.bgStartColor ?? "").PadLeft(6, '0'),
                endcolor = (tpl.bgEndColor ?? "").PadLeft(6, '0'),
                file = tpl.bgImgFile,
                horizontaloffset = Convert.ToString(tpl.bgHOffset, CultureInfo.InvariantCulture),
                verticaloffset = Convert.ToString(tpl.bgVOffset, CultureInfo.InvariantCulture),
                keepStretchRatio = "false",
                withGradient = tpl.bgIsGradient ? "true" : "false"
            };

            if (!tpl.bgIsGradient)
            {
                bg.endcolor = bg.startcolor;
            }

            // Fill remaining document fields
            oDocument_html5.images_height = minImgH.ToString(CultureInfo.InvariantCulture);
            oDocument_html5.images_width = minImgW.ToString(CultureInfo.InvariantCulture);
            oDocument_html5.pdfURL = !string.IsNullOrEmpty(oDocument.PDFForDownloadFile)
                ? oDocument.PDFForDownloadFile
                : pdfFileUrl;

            oDocument_html5.logo = "";
            oDocument_html5.analytics = "1";
            oDocument_html5.Analytics_URL = "";
            oDocument_html5.autoflip = "true";
            oDocument_html5.disable_archive = tpl.enableArchive ? "false" : "true";
            oDocument_html5.disable_contents = "true";
            oDocument_html5.enablePageLinks = "1";
            oDocument_html5.page_mode = tpl.initPageMode.ToString();
            oDocument_html5.persistentCookies = "false";
            oDocument_html5.printing_disabled = "true";
            oDocument_html5.publicationName = "";
            oDocument_html5.publisherid = "";
            oDocument_html5.reverse = "";
            oDocument_html5.subscription = "";
            oDocument_html5.thumbs_type = "1";
            oDocument_html5.v = "1";
            oDocument_html5.wishlist = "false";
            oDocument_html5.product_prefix = oDocument.Product_prefix;

            // Optional music
            var musicAttr = docXml.SelectSingleNode("//doc")?.Attributes?["musicfile"]?.Value;
            oDocument_html5.background_music = string.IsNullOrEmpty(musicAttr)
                ? ""
                : Utility.CombineURL(repositoryUrl, "res", musicAttr);

            // TOC
            var toc = docXml.SelectSingleNode("//toc");
            oDocument_html5.toc_page = (toc != null && Attr(toc, "type") == "1")
                ? Attr(toc, "targetpage", "0")
                : "0";

            // Build outer JSON (keeping your original template structure)
            string docJson = "{\"issue\":{\"@attributes\": $issue_attr,\"page\": [$pages],\"background\":{\"@attributes\": $background},\"bookmark\":[$bookmarks],\"fonts\":[$fonts]}}";

            // Issue
            docJson = docJson.Replace("$issue_attr", J(oDocument_html5));

            // Pages
            docJson = docJson.Replace("$pages", pagesSb.ToString());

            // Bookmarks
            var bookmarkNodes = docXml.SelectNodes("//bookmark");
            var bookmarksSb = new StringBuilder();
            for (int i = 0; i < (bookmarkNodes?.Count ?? 0); i++)
            {
                if (i > 0) bookmarksSb.Append(',');
                var bNode = bookmarkNodes![i];
                var b = new bookmark
                {
                    bookmarkColor = Utility.uintToHexcolor(Attr(bNode, "bookmarkColor", "0")),
                    bookmarkDesc = Attr(bNode, "bookmarkDesc"),
                    bookmarkTextColor = Utility.uintToHexcolor(Attr(bNode, "bookmarkTextColor", "0")),
                    pageIndex = Attr(bNode, "pageIndex")
                };
                bookmarksSb.Append("{\"@attributes\":");
                bookmarksSb.Append(J(b));
                bookmarksSb.Append('}');
            }
            docJson = docJson.Replace("$bookmarks", bookmarksSb.ToString());

            // Background
            docJson = docJson.Replace("$background", J(bg));

            // Fonts
            var fontNodes = docXml.SelectNodes("//font");
            var fontsSb = new StringBuilder();
            for (int i = 0; i < (fontNodes?.Count ?? 0); i++)
            {
                if (i > 0) fontsSb.Append(',');
                var fname = Attr(fontNodes![i], "name");
                // Your template expects {"@attributes": $font} where $font is just the name serialized (a JSON string)
                fontsSb.Append("{\"@attributes\":");
                fontsSb.Append(J(fname));
                fontsSb.Append('}');
            }
            docJson = docJson.Replace("$fonts", fontsSb.ToString());

            return docJson;
        }




        public void generateS3Files(ApplicationDbContext _context,string sDocumentID)
        {

            document oDocument = _context.document
                   .Include(d => d.Publication)
                   .Include(d => d.Publication.Publisher)
                   .Where(d => d.Id == sDocumentID)
                   .SingleOrDefault();

            publication oPublication = oDocument.Publication;
            publicationtemplate oPublicationTemplate = oPublication.PublicationTemplate;
            bool bUseUrlAsProductId = false;
            if (oPublication.Publisher.Name == "Marine-Warehouse")
                bUseUrlAsProductId = true;
            //Generate Document Use the local file to get XML settings
            XmlDocument oXMLDocument = DocumentConvertor.getAsXML(_context, oDocument, true);
            if (oXMLDocument == null)
            {
                return;
            }

            string sDocumentJsonContent = DocumentConvertor.GetDocumentAsJson(_context, oXMLDocument);
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);
            string sJsonDocument = System.IO.Path.Combine(sDocumentPath, "document.json");
            System.IO.File.WriteAllText(sJsonDocument, sDocumentJsonContent);
            XmlNodeList oXmlNodeList = oXMLDocument.SelectNodes("//page");

            int nStartPage = 1;
            XmlNode oFirstPage = oXmlNodeList.Item(0);
            if (oFirstPage.Attributes["intro"] != null && oFirstPage.Attributes["intro"].Value == "1")
                nStartPage = 0;

            //Generate Pages.
            int nPagesCount = oXmlNodeList.Count;
            for (int i = nStartPage; i <= nPagesCount; i++)
            {
                string sPage_Name = string.Format("Page_{0}.xml", i);
                string sJsonPage_Name = string.Format("Page_{0}.json", i);
                string sPageURL = System.IO.Path.Combine(sDocumentPath, sPage_Name);
                XmlDocument oXMLResult = new XmlDocument();
                if (File.Exists(sPageURL))
                {
                    string sPageResult;
                    if (File.Exists(sPageURL))
                    {
                        try
                        {
                            // we have issue with the xml skip this file
                            oXMLResult.Load(sPageURL);
                            sPageResult = DocumentConvertor.getPageJson(oDocument, oXMLResult, oPublicationTemplate, bUseUrlAsProductId);
                        }
                        catch (Exception exe)
                        {
                            sPageResult = "{\"links\": {\"link\": [],\"video\":[],\"audio\":[],\"image\":[]}}";
                        }



                    }
                    else
                    {
                        //XML is missing create a new json file...
                        sPageResult = "{\"links\": {\"link\": [],\"video\":[],\"audio\":[],\"image\":[]}}";
                    }
                    string sJsonPagePath = System.IO.Path.Combine(sDocumentPath, sJsonPage_Name);
                    System.IO.File.WriteAllText(sJsonPagePath, sPageResult);
                }
            }

            string sPublicationPath = PublicationUtil.getPublicationPath(oDocument.Publication);
            if (!Directory.Exists(sPublicationPath))
                Directory.CreateDirectory(sPublicationPath);
            //Preloader
            string sPreloaderContent = PublicationUtil.getPreloader(oDocument);
            string sPreloaderPath = System.IO.Path.Combine(sPublicationPath, "preloader.json");
            if (File.Exists(sPreloaderPath))
                File.Delete(sPreloaderPath);
            System.IO.File.WriteAllText(sPreloaderPath, sPreloaderContent);

            //Generate Settings
            string sSettingsJsonContent = DocumentConvertor.GetSettingsJson(_context,oPublication);
            string sJsonSettingsPath = System.IO.Path.Combine(sPublicationPath, "settings.json");
            if (File.Exists(sJsonSettingsPath))
                File.Delete(sJsonSettingsPath);
            System.IO.File.WriteAllText(sJsonSettingsPath, sSettingsJsonContent);
        }

       


        public bool addFonts(string sPDFFile, string sOutputDirectory)
        {
            //BackendProcessorName//

            string exePath = System.IO.Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
            string sDocumentXML = System.IO.Path.Combine(sOutputDirectory, "document.xml");
            string sCommand = string.Format("-m createfonts -i \"{0}\" -o \"{1}\"", sPDFFile, sOutputDirectory);
            try
            {
                Process oPDFProcess = new Process();
                ProcessStartInfo startInfo = new ProcessStartInfo(exePath);
                startInfo.Arguments = sCommand;
                oPDFProcess.StartInfo = startInfo;
                startInfo.WindowStyle = ProcessWindowStyle.Hidden;
                oPDFProcess.Start();
                int nTimeout = 60000;
                oPDFProcess.WaitForExit(nTimeout);
                int nExitCode = oPDFProcess.ExitCode;
            }
            catch (Exception ex)
            {
                //Logger.log.Debug("Error when creating fonts....", ex);
                return false;
            }


            return true;
        }

        public int FirstThumbHeight
        {
            get
            {
                return _nFirstThumbHeight;
            }
        }

        public object DocumentUtil { get; private set; }

        protected static bool validateLink(XmlNode oPageLink)
        {
            bool bIsValid = true;
            if (oPageLink.Attributes["url"] == null)
                bIsValid = false;
            return bIsValid;

        }

        public static string GetSettingsJson(
        ApplicationDbContext context,
        publication pub)
        {
            var tpl = pub?.PublicationTemplate
                      ?? throw new InvalidOperationException("PublicationTemplate is null.");

            string sSettingsText =
                "{\"archive\": {\"@attributes\": $sSettingsAttributes,\"issue\": [{\"@attributes\": $sArchiveIssues}]}}";

            var settings = new JSONSettingsResult
            {
                logo_image = tpl.logoImgFile,
                logo_url = tpl.logoURL,
                buttons_color = GetButtonsHex(tpl.buttonColor),

                publisherid = "",
                html5_menu_bg_hex = "CCCCCC",
                html5_toolbar_bg_start = tpl.toolbarStartColor,
                html5_toolbar_bg_end = tpl.toolbarIsGradient ? tpl.toolbarEndColor : tpl.toolbarStartColor,

                html5_menu_bg_opacity = "90",
                html5_menu_fg_hex = "000000",
                html5_archive_bg_hex = "BBBBBB",
                html5_archive_bg_opacity = "90",
                html5_archive_fg_hex = "FFFFFF",
                html5_archive_light_highlight_hex = "3A4A7F",
                html5_archive_dark_highlight_hex = "000000",
                html5_archive_divider_hex = "000000",

                openarchiveasexternallink = tpl.openarchiveasexternallink ? "1" : "0",
                bigarrowside = tpl.bigarrowside ? "1" : "0",
                isDemoMode = ((pub.Publisher?.Name == "MyDemo") || (pub.Publisher?.Name == "DCdemo")) ? "1" : "0",

                html5_links_default_color = (tpl.linkColor ?? "").Replace("0x", ""),
                html5_links_default_opacity = tpl.linkOpacity.ToString(CultureInfo.InvariantCulture),
                html5_links_highlight_time = tpl.linkHighlightDuration.ToString(CultureInfo.InvariantCulture),

                html5_method = "topleft",
                doc_type = "",
                hasthumbnails = tpl.enableThumbnail ? "1" : "0",
                hasshare = tpl.enableShareFeature ? "1" : "0",
                hassearch = tpl.enableSearch ? "1" : "0",
                hasecommerce = tpl.enableShoppingCart ? "1" : "0",
                haswishlist = tpl.enablewishlist ? "1" : "0",
                checkouturl = tpl.checkoutURL,
                storelocatorurl = tpl.storelocatorURL,
                enablega = tpl.enableGA ? "1" : "0",
                gaaccount = tpl.gaAccount,
                showintromobile = tpl.showintromobile ? "1" : "0",
                showproducticon = tpl.showproductasicon ? "1" : "0",
                ecommerceapiurl = tpl.eCommerceAPIURL,
                producticonurl = tpl.productIconURL,
                emailserverurl = tpl.emailserverURL,
                sharepdfaslink = tpl.sharepdfaslink ? "1" : "0",
                hasfullscreen = tpl.enableFullscreen ? "1" : "0",
                hasdownload = tpl.enableDownload ? "1" : "0",
                hasinfo = tpl.enableinfo ? "1" : "0",
                infourl = tpl.infourl,
                helpURL = tpl.helpURL,

                // sync call here:
                hasarchive = (PublicationUtil.GetActiveDocumentCountAsync(context, pub.Id.ToString()) > 1 && tpl.enableArchive) ? "1" : "0",

                hascrop = tpl.enableCrop ? "1" : "0",
                hasnotes = tpl.enableNote ? "1" : "0",
                hasprint = tpl.enablePrint ? "1" : "0",
                haspagemode = tpl.enablePageMode ? "1" : "0",
                haspageshadow = tpl.enablePageMode ? "1" : "0",

                defaultpopupwidth = tpl.defaultpopupwidth.ToString(CultureInfo.InvariantCulture),
                defaultpopupheight = tpl.defualtpopupheight.ToString(CultureInfo.InvariantCulture),
                defaultproducturl = tpl.productURL,
                lanaguge = tpl.language,
                haslinkedin = tpl.enableLinkedIn ? "1" : "0",
                checkouttarget = tpl.checkouttarget,
                flippinghtml5 = tpl.html5useflippingeffect ? "1" : "0",
                autohidetoolbar = tpl.html5autohidetooltip ? "1" : "0",
                enableselectabletext = tpl.enableselectabletext ? "1" : "0",

                arrowscolor = string.IsNullOrEmpty(tpl.arrowscolor) ? "FFFFFF" : tpl.arrowscolor.PadLeft(6, '0'),
                useSVG = tpl.usesvghtml5 ? "1" : "0",

                enablezoomcontrols = tpl.enableZoomControls ? "1" : "0",
                maximumzoomlevel = tpl.maxzoomfactor.ToString(CultureInfo.InvariantCulture),
                zoomfactor = tpl.zoomfactor.ToString(CultureInfo.InvariantCulture),
                autohidezoombuttons = tpl.autohidezoombuttons ? "1" : "0",
                html5buttonsset = tpl.html5buttonsset,
                disclaimer = tpl.linksDisclaimerMessage,
                ecommercepricing = tpl.ecommercepricing ? "1" : "0",
                showecommercedescription = tpl.showecommercedescription ? "1" : "0",
                enablearchivesearchhtml5 = tpl.html5archivesearch ? "1" : "0",

                bg_backgroundmode = Convert.ToString(tpl.bgImgDisplayMode, CultureInfo.InvariantCulture),
                bg_startcolor = (tpl.bgStartColor ?? "").PadLeft(6, '0'),
                bg_endcolor = tpl.bgIsGradient ? (tpl.bgEndColor ?? "").PadLeft(6, '0')
                                               : (tpl.bgStartColor ?? "").PadLeft(6, '0'),
                bg_file = tpl.bgImgFile,
                bg_horizontaloffset = Convert.ToString(tpl.bgHOffset, CultureInfo.InvariantCulture),
                bg_verticaloffset = Convert.ToString(tpl.bgVOffset, CultureInfo.InvariantCulture),
                bg_withGradient = tpl.bgIsGradient ? "true" : "false",
                bg_keepStretchRatio = "false"
            };

            var issueAttr = new JSONSettingsIssueResult
            {
                issue_name = "",
                pdf = "",
                img = "",
                jpg_width = "",
                jpg_height = "",
                img_320 = "",
                img_960 = "",
                url = "",
                pub_url = "",
                docid = "",
                email = "",
                description = "",
                order_index = "1",
                active = "0",
                issue_code = "",
                customincludedirectory = tpl.customincludedirectory ?? "",
                showcustommessage = tpl.showcustommessage
            };

            var jsonOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = null,
                DefaultIgnoreCondition = JsonIgnoreCondition.Never
            };

            var sSettingsAttributes = JsonSerializer.Serialize(settings, jsonOptions);
            var sArchiveIssues = JsonSerializer.Serialize(issueAttr, jsonOptions);

            return "{\"archive\": {\"@attributes\": " + sSettingsAttributes +
                   ",\"issue\": [{\"@attributes\": " + sArchiveIssues + "}]}}";
        }

        private static string GetButtonsHex(string? buttonColor)
        {
            if (string.IsNullOrWhiteSpace(buttonColor)) return "ffffff";
            var bc = buttonColor.Trim();
            if (bc.StartsWith("#")) return bc;
            if (bc.Length == 6) return bc;
            try { return Utility.uintToHexcolor(bc); } catch { return "ffffff"; }
        }
        public bool createImagesEx(createimagesinput oCreateImagesInput, int nPageCount)
        {
            if (oCreateImagesInput.Prefix == "Thumbnail_")
            {
                //Already done
                return true;
            }
            string output_path = oCreateImagesInput.OutputDirectory;
            if (output_path.EndsWith("\\") == false)
                output_path += "\\";

            String command = string.Format("convert -O resolution={0} -o \"{1}ZPage_%d.jpg\" \"{2}\"", oCreateImagesInput.Resolution, output_path, _InputFileName);

            string exePath = System.IO.Path.Combine(AppContext.BaseDirectory, "Tools", "dcmutool", "dcmutool.exe");
            ProcessStartInfo cmdsi = new ProcessStartInfo(exePath);
            cmdsi.Arguments = command;
            Process cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            int result = cmd.ExitCode;
            if (result != 0)
            {

                return false;
            }
            for (int nPageNumber = 1; nPageNumber <= nPageCount; nPageNumber++)
            {
                string filename = string.Format("{0}{1}{2}.jpg", output_path, oCreateImagesInput.Prefix, nPageNumber);
                DocumentConvertor.createThumbnails(output_path, filename, nPageNumber.ToString());
            }


            return true;


        }

      

        public static XmlDocument getAsXML(ApplicationDbContext _context, document oDoc, bool bUseLocal = false)
        {
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDoc);
            string sDocumentFileName = "document.xml";
            string sDocUrl = DocumentUtilBase.getDocumentRepositoryURL(oDoc);
            sDocUrl = Utility.CombineURL(sDocUrl, sDocumentFileName);
            sDocumentFileName = System.IO.Path.Combine(sDocumentPath, sDocumentFileName);
            XmlDocument oDocument = new XmlDocument();
            if (bUseLocal)
            {
                if (File.Exists(sDocumentFileName))
                    oDocument.Load(sDocumentFileName);
                else
                    return null;
            }
            else
                oDocument.Load(sDocUrl);
            return oDocument;
        }

       

        public bool createImageReplace(createimagesinput oCreateImagesInput, int nPageNumber, int nTargetPage, out int pagewidth, out int pageheight)
        {

            string output_path = oCreateImagesInput.OutputDirectory;
            if (output_path.EndsWith("\\") == false)
                output_path += "\\";

            String command = string.Format("convert -O resolution={0} -o \"{1}{2}{3}{4}.jpg\" \"{5}\" {6}", oCreateImagesInput.Resolution, output_path, oCreateImagesInput.Prefix, nTargetPage, "%s", _InputFileName, nPageNumber);
            ProcessStartInfo cmdsi = new ProcessStartInfo("dcmutool.exe");
            cmdsi.Arguments = command;
            Process cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            string sTargetFileName = string.Format("{0}{1}{2}.jpg", output_path, oCreateImagesInput.Prefix, nTargetPage);
            DocumentConvertor.createThumbnails(output_path, sTargetFileName, nTargetPage.ToString());
            using (System.Drawing.Image img = System.Drawing.Image.FromFile(sTargetFileName))
            {
                pagewidth = img.Width;
                pageheight = img.Height;
            }
            return true;
        }

        public bool createImageEx(createimagesinput oCreateImagesInput, int nPageNumber, int nTargetPage, out int pagewidth, out int pageheight)
        {

            string output_path = oCreateImagesInput.OutputDirectory;
            if (output_path.EndsWith("\\") == false)
                output_path += "\\";

            String command = string.Format("convert -O resolution={0} -o \"{1}{2}{3}{4}.jpg\" \"{5}\" {6}", oCreateImagesInput.Resolution, output_path, oCreateImagesInput.Prefix, nTargetPage, "%s", _InputFileName, nTargetPage);
            ProcessStartInfo cmdsi = new ProcessStartInfo("dcmutool.exe");
            cmdsi.Arguments = command;
            Process cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            string sTargetFileName = string.Format("{0}{1}{2}.jpg", output_path, oCreateImagesInput.Prefix, nTargetPage);
            DocumentConvertor.createThumbnails(output_path, sTargetFileName, nTargetPage.ToString());
            using (System.Drawing.Image img = System.Drawing.Image.FromFile(sTargetFileName))
            {
                pagewidth = img.Width;
                pageheight = img.Height;
            }
            return true;
        }

        /*
   * We are using the hi images to generate thumbnail to bypass the lines bug
   */
        public static bool createThumbnails(string sOutputDirectory, string sFileName, string sCurrentPage)
        {
            string sThumbName = string.Format("{0}{1}{2}", "Thumbnail_", sCurrentPage, ".jpg");
            string sDestFileName = System.IO.Path.Combine(sOutputDirectory, sThumbName);
            if (File.Exists(sDestFileName))
            {
                try
                {
                    File.Delete(sDestFileName);
                }
                catch (Exception)
                {
                    return false;
                }
            }
            if (!File.Exists(sFileName))
                return false;
            try
            {
                using (FileStream fs = new FileStream(sFileName, FileMode.Open, FileAccess.Read))
                {
                    using (System.Drawing.Image pageimage = System.Drawing.Image.FromStream(fs))
                    {
                        int nWidth = 300;
                        double dHeight = nWidth / (double)pageimage.Width * pageimage.Height;
                        int nHeight = System.Convert.ToInt32(dHeight);
                        System.Drawing.Image thumbImage = ResizeImage(pageimage, nWidth, nHeight);
                        thumbImage.Save(sDestFileName, System.Drawing.Imaging.ImageFormat.Jpeg);
                        thumbImage.Dispose();
                        thumbImage = null;
                        return true;
                    }
                }

            }
            catch (Exception)
            {
                //Logger.log.Error("Erorr when calling createThumbnails " + e.Message);
                return false;
            }




        }



        public static void updatedocjsonversion(JObject settingsJson, int npagenumber)
        {

            if (settingsJson["issue"]["page"] != null)
            {


                JArray arrPages = (JArray)settingsJson["issue"]["page"];
                foreach (JObject page in arrPages)
                {
                    JValue seq = (JValue)page["@attributes"]["sequence"];

                    if (seq != null && seq.Value.ToString() == npagenumber.ToString())
                    {
                        JValue version = (JValue)page["@attributes"]["version"];

                        string sVersion = version.Value.ToString();
                        int numValue;

                        bool parsed = Int32.TryParse(sVersion, out numValue);
                        if (parsed)
                        {
                            numValue++;
                            version.Value = numValue.ToString();
                        }
                        else
                        {
                            version.Value = "2";
                        }


                    }
                }


            }
        }

        public static string updatedocjsonversion(document oDocument, int npagenumber)
        {
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string sJsonFilePath = System.IO.Path.Combine(sDocumentPath, "document.json");
            JObject settingsJson = JObject.Parse(System.IO.File.ReadAllText(sJsonFilePath));
            updatedocjsonversion(settingsJson, npagenumber);
            File.WriteAllText(sJsonFilePath, settingsJson.ToString());
            return sJsonFilePath;
        }

        /*
        public static bool ReplacePageInPDF(int nPageNumber, string sExistingPDF, string sPDFToAdd)
        {
            try
            {
                // Load the existing PDF and the PDF from which to replace the page
                var existingPdf = PdfDocument.FromFile(sExistingPDF);
                var newPdf = PdfDocument.FromFile(sPDFToAdd);

                // Get the number of pages in the existing PDF
                int nExistingPageCount = existingPdf.PageCount;

                // Check if the page number is valid
                if (nPageNumber <= 0 || nPageNumber > nExistingPageCount)
                {
                    throw new ArgumentOutOfRangeException("Page number is out of range.");
                }

                PdfDocument singlePagePDF = newPdf.CopyPage(nPageNumber - 1);

                // Get the page to add from the new PDF
                var newPage = newPdf.Pages[nPageNumber - 1];  // Pages are 0-based in IronPDF

                // Replace the page in the existing PDF
                existingPdf.RemovePage(nPageNumber - 1);      // Remove the existing page (0-based index)
                existingPdf.InsertPdf(singlePagePDF, nPageNumber - 1);

                // Save the updated PDF
                existingPdf.SaveAs(sExistingPDF);

                // Dispose of the PDF documents
                existingPdf.Dispose();
                newPdf.Dispose();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error when replacing pages in PDF: " + e.Message);
                return false;
            }
            return true;
        }
        */

        static public bool replacePageInPDFHD(int nPageNumber, string sExistingPDF, string sPDFToAdd)
        {
            try
            {
                //In case we are replacing all the pages just copy the file.

                //var oCurrentPDF = PdfDocument.FromFile(sExistingPDF);
                //var oNewPDF = PdfDocument.FromFile(sPDFToAdd);
                //int nExitingPageCount = oCurrentPDF.PageCount;
                //int nNumberOfPageInNewPDF = oNewPDF.PageCount;
                //int nNumberOfPageToReplace = Math.Min(nNumberOfPageInNewPDF, nExitingPageCount - nPageNumber + 1);
                //if (nNumberOfPageToReplace < 1)
                //    return false;
                //if (nNumberOfPageToReplace == nExitingPageCount)
                //{
                //    nNumberOfPageToReplace = 1;
                //    oCurrentPDF.RemovePage(nPageNumber - 1);
                //}
                //else
                //{
                //    oCurrentPDF.RemovePages(nPageNumber - 1, nPageNumber - 1 + nNumberOfPageToReplace - 1);
                //}
                //if (nNumberOfPageInNewPDF > 1)
                //{
                //    PdfDocument oSinglePagePDF = oNewPDF.CopyPage(0);
                //    oCurrentPDF.InsertPdf(oSinglePagePDF, nPageNumber - 1);
                //    oSinglePagePDF.Dispose();
                //}
                //else
                //{
                //    oCurrentPDF.InsertPdf(oNewPDF, nPageNumber - 1);
                //}
                //oCurrentPDF.SaveAs(sExistingPDF, false);

                //oCurrentPDF.Dispose();
                //oNewPDF.Dispose();

                //Updated using Itext7
                string tempFile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sExistingPDF), "temp.pdf");

                using (iText.Kernel.Pdf.PdfDocument existingPdf = new iText.Kernel.Pdf.PdfDocument(new PdfReader(sExistingPDF), new PdfWriter(tempFile)))
                using (iText.Kernel.Pdf.PdfDocument newPdf = new iText.Kernel.Pdf.PdfDocument(new PdfReader(sPDFToAdd)))
                {
                    int existingPageCount = existingPdf.GetNumberOfPages();
                    int newPdfPageCount = newPdf.GetNumberOfPages();

                    int numberOfPagesToReplace = Math.Min(newPdfPageCount, existingPageCount - nPageNumber + 1);

                    if (numberOfPagesToReplace < 1)
                        return false;

                    // Special handling if replacing all remaining pages
                    if (numberOfPagesToReplace == existingPageCount)
                    {
                        // Remove only the target page
                        existingPdf.RemovePage(nPageNumber);
                    }
                    else
                    {
                        // Remove pages one by one in reverse order
                        int startPage = nPageNumber;
                        int endPage = nPageNumber + numberOfPagesToReplace - 1;
                        for (int i = endPage; i >= startPage; i--)
                        {
                            existingPdf.RemovePage(i);
                        }
                    }

                    var copier = new PdfPageFormCopier();

                    // Insert new pages
                    if (newPdfPageCount > 1)
                    {
                        // If multiple pages, copy first page as single-page PDF and insert
                        PdfPage singlePage = newPdf.GetPage(1).CopyTo(existingPdf);
                        existingPdf.AddPage(nPageNumber, singlePage); // insert at index
                    }
                    else
                    {
                        // Single-page PDF: insert at nPageNumber
                        PdfPage pageToInsert = newPdf.GetPage(1).CopyTo(existingPdf);
                        existingPdf.AddPage(nPageNumber, pageToInsert);
                    }
                }

                // Replace original PDF
                File.Delete(sExistingPDF);
                File.Move(tempFile, sExistingPDF);
            }
            catch (Exception)
            {

                //Logger.log.Error("Error when replacing pages in PDF " + e.Message);
            }
            return true;
        }

        static public bool replacePageInPDFEx(int nPageNumber, string sExistingPDF, string sPDFToAdd)
        {
            //try
            //{
            //    //In case we are replacing all the pages just copy the file.

            //    var oCurrentPDF = PdfDocument.FromFile(sExistingPDF);
            //    var oNewPDF = PdfDocument.FromFile(sPDFToAdd);
            //    int nExitingPageCount = oCurrentPDF.PageCount;
            //    int nNumberOfPageToReplace = oNewPDF.PageCount;
            //    nNumberOfPageToReplace = Math.Min(nNumberOfPageToReplace, nExitingPageCount - nPageNumber + 1);
            //    if (nNumberOfPageToReplace < 1)
            //        return false;
            //    if (nPageNumber == 1 && nNumberOfPageToReplace == nExitingPageCount)
            //    {
            //        File.Copy(sPDFToAdd, sExistingPDF, true);
            //        oCurrentPDF.Dispose();
            //        oNewPDF.Dispose();
            //        return true;
            //    }
            //    //it is using start index and end index.
            //    oCurrentPDF.RemovePages(nPageNumber - 1, nPageNumber - 1 + nNumberOfPageToReplace - 1);
            //    oCurrentPDF.InsertPdf(oNewPDF, nPageNumber - 1);
            //    oCurrentPDF.SaveAs(sExistingPDF, false);
            //    oCurrentPDF.Dispose();
            //    oNewPDF.Dispose();
            //}
            //catch (Exception)
            //{

            //    //Logger.log.Error("Error when replacing pages in PDF " + e.Message);
            //}

            // Itext7
            string tempFile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sExistingPDF), "temp.pdf");

            using (iText.Kernel.Pdf.PdfDocument existingPdf = new iText.Kernel.Pdf.PdfDocument(new PdfReader(sExistingPDF), new PdfWriter(tempFile)))
            using (iText.Kernel.Pdf.PdfDocument newPdf = new iText.Kernel.Pdf.PdfDocument(new PdfReader(sPDFToAdd)))
            {
                int existingPageCount = existingPdf.GetNumberOfPages();
                int numberOfPagesToReplace = Math.Min(newPdf.GetNumberOfPages(), existingPageCount - nPageNumber + 1);

                if (numberOfPagesToReplace < 1)
                    return false;

                // Special case: replace entire document
                if (nPageNumber == 1 && numberOfPagesToReplace == existingPageCount)
                {
                    existingPdf.Close();
                    newPdf.Close();
                    File.Copy(sPDFToAdd, sExistingPDF, true);
                    return true;
                }

                int startPage = nPageNumber;                        // 1-based
                int endPage = nPageNumber + numberOfPagesToReplace - 1;

                // Remove pages
                for (int i = endPage; i >= startPage; i--)
                {
                    existingPdf.RemovePage(i);  // RemovePage(int pageNumber) exists
                }

                // Insert new PDF pages at startPage
                var copier = new PdfPageFormCopier();
                newPdf.CopyPagesTo(1, newPdf.GetNumberOfPages(), existingPdf, startPage, copier);
            }

            // Replace original PDF with temp file
            File.Delete(sExistingPDF);
            File.Move(tempFile, sExistingPDF);

            return true;
        }



        public bool createPageXMLReplacePage(createpagesxmlinput oCreatePagesXMLInput, int nPageNumber)
        {
            //Set the page id base on the documnent XML
            string sOutputDir = oCreatePagesXMLInput.OutputDirectory;

            string sPageName = string.Format("Page_{0}.xml", nPageNumber);
            string sPageFileName = System.IO.Path.Combine(sOutputDir, sPageName);

            XmlDocument xmlPage = new XmlDocument();
            xmlPage.Load(sPageFileName);
            XmlNode links = xmlPage.SelectSingleNode("//links");
            links.RemoveAll();

            File.Delete(sPageFileName);
            xmlPage.Save(sPageFileName);

            // c=1 recognize links.
            String command = string.Format("-i \"{0}\" -o \"{1}\" -e 0 -c 1 -p {2}", oCreatePagesXMLInput.InputFileName, sOutputDir, nPageNumber);

            string exePath = System.IO.Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
            ProcessStartInfo cmdsi = new ProcessStartInfo("PDFUtils.exe");
            cmdsi.Arguments = command;
            Process cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            return true;

        }


        public static async Task downloadDocumentFile(document oDocument, string sDirectory, string sFileName)
        {
            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
            PostSubmitter postClient = new();
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                oDocument.Id);
            string sS3FileLocation = string.Format("https://s3.amazonaws.com/{0}/{1}/{2}", sBucketName,
                sKeyPrefix, sFileName);
            if (!Directory.Exists(sDirectory))
                Directory.CreateDirectory(sDirectory);
            string sFullFileName = string.Format("{0}\\{1}", sDirectory, sFileName);
            await postClient.DownloadFileAsync(sS3FileLocation, sFullFileName);
        }


        public static string getCordinate(string sCordinate, double dRatio)
        {
            double dCordinate = Math.Abs(System.Convert.ToDouble(sCordinate) * dRatio);
            return dCordinate.ToString("0.##");

        }

        public static string getCordinate(string l1, string l2)
        {
            double dCordinate1 = System.Convert.ToDouble(l1);
            double dCordinate2 = System.Convert.ToDouble(l2);
            return (dCordinate1 + dCordinate2).ToString();
        }

        public static string getPageJson(document oDocument, XmlDocument oPageXML, publicationtemplate oPublicationTemplate, bool bUseUrlAsProductId = false)
        {

            //var serializer = new JavaScriptSerializer();
            //var sSettingsAttributes = serializer.Serialize(oJSONSettingsResult);
            string sPageResult = "";
            string sPageTemplate = "{\"links\": {\"link\": [$links],\"video\":[$videos],\"audio\":[$audios],\"image\":[$images]}}";
            string sLinkTemplate = "{\"@attributes\":$link}";
            string sVideoTemplate = "{\"@attributes\":$video}";
            string sAudioTemplate = "{\"@attributes\":$audio}";
            string sImagesTemplate = "{\"@attributes\":$image}";
            string sPageLinks = "";
            XmlNodeList oPageLinks = oPageXML.SelectNodes("//link");
            int i;

            //In Adobe the link resolution is always 72.
            double LinksRatio = oDocument.HiPageResolution / 72;





            bool bIsFirstLink = true;
            int nLinkID;
            lock (syncLock)
            { // synchronize
                nLinkID = random.Next(1, 10000000);
            }


            for (i = 0; i < oPageLinks.Count; i++)
            {

                XmlNode oPageLink = oPageLinks[i];
                if (validateLink(oPageLink) == false)
                    continue;
                if (!bIsFirstLink)
                    sPageLinks += ",";
                Link oLink = new Link();
                string sLocation = oPageLink.Attributes["loc"].Value;
                string[] stringSeparators = new string[] { " " };
                string[] result = sLocation.Split(stringSeparators, StringSplitOptions.RemoveEmptyEntries);
                oLink.link_id = nLinkID.ToString();

                double y2 = System.Convert.ToDouble(result[3]);
                if (y2 < 0)
                {
                    oLink.y1 = getCordinate(getCordinate(result[1], result[3]), LinksRatio);
                    oLink.y2 = getCordinate(result[1], LinksRatio);

                }
                else
                {
                    oLink.y1 = getCordinate(result[1], LinksRatio);
                    oLink.y2 = getCordinate(getCordinate(result[3], LinksRatio), oLink.y1);
                }



                double x2 = System.Convert.ToDouble(result[2]);
                if (x2 < 0)
                {

                    oLink.x1 = getCordinate(getCordinate(result[0], result[2]), LinksRatio);
                    oLink.x2 = getCordinate(result[0], LinksRatio);
                }
                else
                {
                    oLink.x1 = getCordinate(result[0], LinksRatio);
                    oLink.x2 = getCordinate(getCordinate(result[2], LinksRatio), oLink.x1);
                }
                oLink.type = oPageLink.Attributes["linktype"].Value;

                oLink.description = oPageLink.Attributes["text"].Value;
                if (oPageLink.Attributes["playonhover"] != null)
                    oLink.hover = oPageLink.Attributes["playonhover"].Value == "true" ? "1" : "0";
                else
                {

                    oLink.hover = "0";
                }
                if (oPageLink.Attributes["url"] != null)
                    oLink.url = oPageLink.Attributes["url"].Value;
                else
                    oLink.url = "";

                if (oDocument.Publication.Publisher.ConvertSettings != null && oDocument.Publication.Publisher.ConvertSettings.UseURLAsId && oLink.url != "")
                {
                    oLink.link_id = oLink.url;
                }

                if (oLink.type == "7")
                {
                    if (bUseUrlAsProductId)
                        oLink.productid = oLink.url;
                    else
                        oLink.productid = oLink.link_id;
                }

                oLink.popupwidth = oPageLink.Attributes["popupwidth"] != null ? oPageLink.Attributes["popupwidth"].Value : oPublicationTemplate.defaultpopupwidth.ToString();
                oLink.popupheight = oPageLink.Attributes["popupheight"] != null ? oPageLink.Attributes["popupheight"].Value : oPublicationTemplate.defualtpopupheight.ToString();

                if (oPageLink.Attributes["opacity"] != null)
                {
                    Double outopacity;
                    if (Double.TryParse(oPageLink.Attributes["opacity"].Value, out outopacity))
                    {
                        outopacity = Math.Round(outopacity, 2);
                        oLink.opacity = outopacity.ToString();
                    }
                }


                if (oPageLink.Attributes["coordinates"] != null)
                {
                    oLink.coordinates = oPageLink.Attributes["coordinates"].Value;
                }
                if (oPageLink.Attributes["color"] != null)
                {
                    string sColor = oPageLink.Attributes["color"].Value;
                    oLink.color = sColor.Replace("0x", "");
                }

                if (oPageLink.Attributes["showdisclaimer"] != null)
                {
                    oLink.showdisclaimer = oPageLink.Attributes["showdisclaimer"].Value;
                }
                oLink.frompdf = "1";
                JObject oLinkJObject = JObject.FromObject(oLink);
                if (string.IsNullOrEmpty(oLink.opacity))
                    oLinkJObject.Property("opacity").Remove();
                if (string.IsNullOrEmpty(oLink.color))
                    oLinkJObject.Property("color").Remove();
                string sLinkText = oLinkJObject.ToString();

                //string sLinkText = serializer.Serialize(oLink);
                sPageLinks += sLinkTemplate.Replace("$link", sLinkText);
                nLinkID++;
                bIsFirstLink = false;
            }


            //For now we are using the product as a link
            XmlNodeList oPageProducts = oPageXML.SelectNodes("//product");
            for (i = 0; i < oPageProducts.Count; i++)
            {
                if (i > 0 || oPageLinks.Count > 0)
                    sPageLinks += ",";
                XmlNode oPageProduct = oPageProducts[i];
                Link oLink = new Link();
                string sLocation = oPageProduct.Attributes["loc"].Value;
                string[] stringSeparators = new string[] { " " };
                string[] result = sLocation.Split(stringSeparators, StringSplitOptions.RemoveEmptyEntries);
                oLink.x1 = getCordinate(result[0], LinksRatio);
                oLink.y1 = getCordinate(result[1], LinksRatio);
                oLink.x2 = getCordinate(getCordinate(result[2], LinksRatio), oLink.x1);
                oLink.y2 = getCordinate(getCordinate(result[3], LinksRatio), oLink.y1);
                oLink.type = "7";
                oLink.tooltiptext = oPageProduct.Attributes["tooltiptext"].Value;
                oLink.url = oPageProduct.Attributes["url"].Value;
                oLink.productid = oPageProduct.Attributes["productid"].Value;
                oLink.link_id = nLinkID.ToString();
                if (oPageProduct.Attributes["coordinates"] != null)
                {
                    oLink.coordinates = oPageProduct.Attributes["coordinates"].Value;
                }
                if (oPageProduct.Attributes["color"] != null)
                {
                    string sColor = oPageProduct.Attributes["color"].Value;
                    oLink.color = sColor.Replace("0x", "");
                }
                else
                {
                    oLink.color = "";
                }

                if (oPageProduct.Attributes["description"] != null)
                {
                    oLink.description = oPageProduct.Attributes["description"].Value;
                }
                if (oPageProduct.Attributes["price"] != null)
                {
                    oLink.price = oPageProduct.Attributes["price"].Value;
                }
                if (oPageProduct.Attributes["opacity"] != null)
                {
                    Double outopacity;
                    if (Double.TryParse(oPageProduct.Attributes["opacity"].Value, out outopacity))
                    {
                        outopacity = outopacity * 100;
                        int nOpacity = System.Convert.ToInt32(outopacity);
                        oLink.opacity = nOpacity.ToString();
                    }
                }
                else
                {
                    oLink.opacity = "";

                }


                oLink.popupwidth = oPageProduct.Attributes["popupwidth"] != null ? oPageProduct.Attributes["popupwidth"].Value : oPublicationTemplate.defaultpopupwidth.ToString();
                oLink.popupheight = oPageProduct.Attributes["popupheight"] != null ? oPageProduct.Attributes["popupheight"].Value : oPublicationTemplate.defualtpopupheight.ToString();

                JObject oLinkJObject = JObject.FromObject(oLink);
                if (oLink.opacity == "")
                    oLinkJObject.Property("opacity").Remove();
                if (oLink.color == "")
                    oLinkJObject.Property("color").Remove();
                string sLinkText = oLinkJObject.ToString();


                //string sLinkText = serializer.Serialize(oLink);
                sPageLinks += sLinkTemplate.Replace("$link", sLinkText);
                nLinkID++;
            }

            string sPageVideos = "";
            XmlNodeList oPageVideos = oPageXML.SelectNodes("//video");
            for (i = 0; i < oPageVideos.Count; i++)
            {

                if (i > 0)
                    sPageVideos += ",";
                XmlNode oPageVideo = oPageVideos[i];
                Video oVideo = new Video();
                string sLocation = oPageVideo.Attributes["loc"].Value;
                string[] stringSeparators = new string[] { " " };
                string[] result = sLocation.Split(stringSeparators, StringSplitOptions.RemoveEmptyEntries);
                oVideo.link_id = nLinkID.ToString();
                oVideo.x1 = getCordinate(result[0], LinksRatio);
                oVideo.y1 = getCordinate(result[1], LinksRatio);
                oVideo.x2 = getCordinate(getCordinate(result[2], LinksRatio), oVideo.x1);
                oVideo.y2 = getCordinate(getCordinate(result[3], LinksRatio), oVideo.y1);
                oVideo.url = oPageVideo.Attributes["url"].Value;
                oVideo.mp4_url = oVideo.url;


                if (oPageVideo.Attributes["playloop"] != null && oPageVideo.Attributes["playloop"].Value == "1")
                    oVideo.playloop = "1";
                else
                    oVideo.playloop = "0";
                oVideo.color = "";
                oVideo.type = oPageVideo.Attributes["type"].Value; //todo check
                oVideo.description = "";

                oVideo.autostart = oPageVideo.Attributes["autoplay"].Value;

                var sVideoText = JsonConvert.SerializeObject(oVideo);
                sPageVideos += sVideoTemplate.Replace("$video", sVideoText);
                nLinkID++;
            }




            string sPageAudios = "";
            XmlNodeList oPageAudios = oPageXML.SelectNodes("//audio");
            for (i = 0; i < oPageAudios.Count; i++)
            {

                if (i > 0)
                    sPageAudios += ",";
                XmlNode oPageAudio = oPageAudios[i];
                Audio oAudio = new Audio();
                string sLocation = oPageAudio.Attributes["loc"].Value;
                string[] stringSeparators = new string[] { " " };
                string[] result = sLocation.Split(stringSeparators, StringSplitOptions.RemoveEmptyEntries);
                oAudio.link_id = nLinkID.ToString();
                oAudio.x1 = getCordinate(result[0], LinksRatio);
                oAudio.y1 = getCordinate(result[1], LinksRatio);
                oAudio.x2 = getCordinate(getCordinate(result[2], LinksRatio), oAudio.x1);
                oAudio.y2 = getCordinate(getCordinate(result[3], LinksRatio), oAudio.y1);
                oAudio.url = oPageAudio.Attributes["url"].Value;
                oAudio.showcontrols = oPageAudio.Attributes["showcontrols"].Value;

                oAudio.color = "";
                oAudio.description = "";

                oAudio.autostart = oPageAudio.Attributes["autoplay"].Value;
                oAudio.showasicon = oPageAudio.Attributes["showasicon"].Value;
                string sAudioText = JsonConvert.SerializeObject(oAudio);
                sPageAudios += sAudioTemplate.Replace("$audio", sAudioText);
                nLinkID++;
            }

            string sPageImages = "";
            XmlNodeList oPageImages = oPageXML.SelectNodes("//image");
            for (i = 0; i < oPageImages.Count; i++)
            {

                if (i > 0)
                    sPageImages += ",";
                XmlNode oPageImage = oPageImages[i];
                Image oImage = new Image();
                string sLocation = oPageImage.Attributes["loc"].Value;
                string[] stringSeparators = new string[] { " " };
                string[] result = sLocation.Split(stringSeparators, StringSplitOptions.RemoveEmptyEntries);
                oImage.link_id = nLinkID.ToString();
                oImage.x1 = getCordinate(result[0], LinksRatio);
                oImage.y1 = getCordinate(result[1], LinksRatio);
                oImage.x2 = getCordinate(getCordinate(result[2], LinksRatio), oImage.x1);
                oImage.y2 = getCordinate(getCordinate(result[3], LinksRatio), oImage.y1);
                oImage.url = oPageImage.Attributes["linkurl"].Value;
                oImage.type = oPageImage.Attributes["linktype"].Value;

                oImage.imageurl = oPageImage.Attributes["url"].Value;
                string sImageText = JsonConvert.SerializeObject(oImage);
                sPageImages += sImagesTemplate.Replace("$image", sImageText);
                nLinkID++;
            }

            sPageResult = sPageTemplate.Replace("$links", sPageLinks);
            sPageResult = sPageResult.Replace("$videos", sPageVideos);
            sPageResult = sPageResult.Replace("$audios", sPageAudios);
            sPageResult = sPageResult.Replace("$images", sPageImages);
            return sPageResult;

        }

        public createimagesinput createImageInput(int nResolution, int nWidth, int nHeigth, string sFullPDFFileName, string sOutputDirectory, string sPrefix, int nImageQuality, job oJob, string sDescription)
        {
            createimagesinput oCreateImagesInput = new createimagesinput();
            oCreateImagesInput.Resolution = nResolution; //Will be optimized base on the page
            oCreateImagesInput.Width = nWidth;
            oCreateImagesInput.Height = nHeigth;
            oCreateImagesInput.InputFileName = sFullPDFFileName;
            oCreateImagesInput.OutputDirectory = sOutputDirectory;
            oCreateImagesInput.Prefix = sPrefix;
            oCreateImagesInput.Quality = nImageQuality;
            oCreateImagesInput.Job = oJob;
            oCreateImagesInput.Description = sDescription;
            return oCreateImagesInput;
        }



        public bool createImageEx(createimagesinput oCreateImagesInput, int nPageNumber, int nTargetPage)
        {

            string output_path = oCreateImagesInput.OutputDirectory;
            if (output_path.EndsWith("\\") == false)
                output_path += "\\";

            String command = string.Format("convert -O resolution={0} -o \"{1}{2}{3}{4}.jpg\" \"{5}\" {6}", oCreateImagesInput.Resolution, output_path, oCreateImagesInput.Prefix, nTargetPage, "%s", _InputFileName, nTargetPage);
            ProcessStartInfo cmdsi = new ProcessStartInfo("dcmutool.exe");
            cmdsi.Arguments = command;
            Process cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            string sTargetFileName = string.Format("{0}{1}{2}.jpg", output_path, oCreateImagesInput.Prefix, nTargetPage);


            createThumbnails(output_path, sTargetFileName, nTargetPage.ToString());
            return true;


        }

        public static Bitmap ResizeImage(System.Drawing.Image image, int width, int height)
        {
            var destRect = new System.Drawing.Rectangle(0, 0, width, height);
            var destImage = new Bitmap(width, height);

            destImage.SetResolution(image.HorizontalResolution, image.VerticalResolution);

            using (var graphics = Graphics.FromImage(destImage))
            {
                graphics.CompositingMode = CompositingMode.SourceCopy;
                graphics.CompositingQuality = CompositingQuality.HighQuality;
                graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
                graphics.SmoothingMode = SmoothingMode.HighQuality;
                graphics.PixelOffsetMode = PixelOffsetMode.HighQuality;

                using (var wrapMode = new ImageAttributes())
                {
                    wrapMode.SetWrapMode(WrapMode.TileFlipXY);
                    graphics.DrawImage(image, destRect, 0, 0, image.Width, image.Height, GraphicsUnit.Pixel, wrapMode);
                }
            }

            return destImage;
        }





        public bool createPageXML(string sOutputDirectory, int nPageNumber, string sPageID)
        {
            //Set the page id base on the documnent XML
            string sOutputDir = sOutputDirectory;
            XmlDocument xmlPage = new XmlDocument();
            XmlProcessingInstruction oPI = xmlPage.CreateProcessingInstruction("xml", "version='1.0' encoding='utf-8'");
            XmlElement rootNode = xmlPage.CreateElement("page");
            xmlPage.AppendChild(oPI);
            xmlPage.AppendChild(rootNode);
            XmlAttribute pageAttribute;
            pageAttribute = xmlPage.CreateAttribute("id");
            pageAttribute.Value = sPageID;                        //Todo Populate

            rootNode.Attributes.Append(pageAttribute);
            pageAttribute = xmlPage.CreateAttribute("num");
            pageAttribute.Value = System.Convert.ToString(nPageNumber);                        //Todo Populate
            rootNode.Attributes.Append(pageAttribute);


            //IronPdf.Pages.IPdfPage oPage = _PDFDoc.Pages[nPageNumber - 1];
            //int nWidth = (int)oPage.Width;
            //int nHeight = (int)oPage.Height;

            PdfPage oPage = _PDFDoc.GetPage(nPageNumber);
            iText.Kernel.Geom.Rectangle pageSize = oPage.GetPageSize();
            float widthPoints = pageSize.GetWidth();
            float heightPoints = pageSize.GetHeight();

            // Optionally convert to int
            int nWidth = (int)widthPoints;
            int nHeight = (int)heightPoints;


            XmlElement links = xmlPage.CreateElement("links");
            rootNode.AppendChild(links);
            /*
            if(oCreatePagesXMLInput.ImportLinks)
		    {
                ArrayList arrLinks = importLinksFromPDF(xmlPage,oPage, true, true, true);
                int nLen = arrLinks.Count;
			    for(int i=0;i<nLen;i++)
			    {
                    XmlElement link = (XmlElement)arrLinks[i];
                    links.AppendChild(link);
			    }
		    }
            */
            XmlElement videos = xmlPage.CreateElement("videos");
            rootNode.AppendChild(videos);


            XmlElement audios = xmlPage.CreateElement("audios");
            rootNode.AppendChild(audios);

            XmlElement flashanimations = xmlPage.CreateElement("flashanimations");
            rootNode.AppendChild(flashanimations);

            string sPageName = string.Format("Page_{0}.xml", nPageNumber);
            string sPageFileName = System.IO.Path.Combine(sOutputDir, sPageName);
            xmlPage.Save(sPageFileName);
            return true;
        }

        public bool RenameFiles(int nPageNumber, document doc, string sFolderName, int nNumberOfPagesAdded, bool bAddafter)
        {
            // if (bAddafter)
            //     nPageNumber++;
            int nPageCount = doc.NumberOfPages;
            for (int i = nPageCount; i >= nPageNumber; i--)
            {
                string sJsonFileToRename = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.json", i));
                string sJsonFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.json", i + nNumberOfPagesAdded));
                if (File.Exists(sJsonFileToRenameNew))
                {
                    File.Delete(sJsonFileToRenameNew);
                }
                if (File.Exists(sJsonFileToRename))
                    File.Move(sJsonFileToRename, sJsonFileToRenameNew);

                string sXmlFileToRename = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.xml", i));
                string sXmlFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.xml", i + nNumberOfPagesAdded));
                if (File.Exists(sXmlFileToRenameNew))
                    File.Delete(sXmlFileToRenameNew);
                if (File.Exists(sXmlFileToRename))
                    File.Move(sXmlFileToRename, sXmlFileToRenameNew);


                string sSvgFileToRename = System.IO.Path.Combine(sFolderName, string.Format("SPage_{0}.svg", i));
                string sSvgFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("SPage_{0}.svg", i + nNumberOfPagesAdded));
                if (File.Exists(sSvgFileToRenameNew))
                    File.Delete(sSvgFileToRenameNew);
                if (File.Exists(sSvgFileToRename))
                    File.Move(sSvgFileToRename, sSvgFileToRenameNew);


                string sThumbFileToRename = System.IO.Path.Combine(sFolderName, string.Format("Thumbnail_{0}.jpg", i));
                string sThumbFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("Thumbnail_{0}.jpg", i + nNumberOfPagesAdded));
                if (File.Exists(sThumbFileToRenameNew))
                    File.Delete(sThumbFileToRenameNew);
                if (File.Exists(sThumbFileToRename))
                    File.Move(sThumbFileToRename, sThumbFileToRenameNew);

                string sZPageFileToRename = System.IO.Path.Combine(sFolderName, string.Format("ZPage_{0}.jpg", i));
                string sZPageFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("ZPage_{0}.jpg", i + nNumberOfPagesAdded));
                if (File.Exists(sZPageFileToRenameNew))
                    File.Delete(sZPageFileToRenameNew);
                if (File.Exists(sZPageFileToRename))
                    File.Move(sZPageFileToRename, sZPageFileToRenameNew);

                string sHTMLFolderName = System.IO.Path.Combine(sFolderName, "html");
                string sXPageXMLFileToRename = System.IO.Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.xml", i));
                string sXPageXMLFileToRenameNew = System.IO.Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.xml", i + nNumberOfPagesAdded));
                if (File.Exists(sXPageXMLFileToRenameNew))
                    File.Delete(sXPageXMLFileToRenameNew);
                if (File.Exists(sXPageXMLFileToRename))
                    File.Move(sXPageXMLFileToRename, sXPageXMLFileToRenameNew);

                string sXPageJsonFileToRename = System.IO.Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.json", i));
                string sXPageJsonFileToRenameNew = System.IO.Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.json", i + nNumberOfPagesAdded));
                if (File.Exists(sXPageJsonFileToRenameNew))
                    File.Delete(sXPageJsonFileToRenameNew);
                if (File.Exists(sXPageJsonFileToRename))
                    File.Move(sXPageJsonFileToRename, sXPageJsonFileToRenameNew);
                //Thumbnail_1
            }
            return true;

        }

        public bool AddPagesToPDF(int nPageNumber, string sExistingPDF, string sPDFToAdd, bool bAddAfter)
        {
            try
            {
                //// Load the existing PDF and the new PDF to add
                //var existingPdf = PdfDocument.FromFile(sExistingPDF);
                //var newPdf = PdfDocument.FromFile(sPDFToAdd);

                //// Determine the page index to start adding pages
                //int startPageIndex = bAddAfter ? nPageNumber : nPageNumber - 1;

                //existingPdf.InsertPdf(newPdf, startPageIndex);

                //// Save the updated PDF
                //existingPdf.SaveAs(sExistingPDF);

                //// Dispose of the documents
                //existingPdf.Dispose();
                //newPdf.Dispose();

                //Updated Itext7
                // Paths
                string existingPdfPath = sExistingPDF;
                string newPdfPath = sPDFToAdd;
                string tempOutput = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(existingPdfPath), "temp.pdf");

                // Determine start page index (0-based in IronPDF, 1-based in iText7)
                int startPageIndex = bAddAfter ? nPageNumber : nPageNumber - 1;

                using (iText.Kernel.Pdf.PdfDocument pdfDoc = new iText.Kernel.Pdf.PdfDocument(new PdfReader(existingPdfPath), new PdfWriter(tempOutput)))
                using (iText.Kernel.Pdf.PdfDocument newPdf = new iText.Kernel.Pdf.PdfDocument(new PdfReader(newPdfPath)))
                {
                    // PdfPageFormCopier preserves annotations, form fields, and other content
                    var copier = new PdfPageFormCopier();

                    // iText7 pages are 1-based
                    int totalExistingPages = pdfDoc.GetNumberOfPages();
                    int insertPosition = startPageIndex + 1; // insertion point in iText7

                    // Copy pages from new PDF to the existing PDF at the desired position
                    newPdf.CopyPagesTo(1, newPdf.GetNumberOfPages(), pdfDoc, insertPosition, copier);
                }

                // Replace original PDF
                System.IO.File.Delete(existingPdfPath);
                System.IO.File.Move(tempOutput, existingPdfPath);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception when adding pages: " + ex.Message);
                return false;
            }

            return true;
        }

        /*
        //Old implementation.
        public bool AddPagesToPDF(int nPageNumber, string sExistingPDF, string sPDFToAdd, bool bAddafter)
        {
            try
            {
                PdfDocument oCurrentPDF = PdfDocument.FromFile(sExistingPDF);
                PdfDocument oNewPDF = PdfDocument.FromFile(sPDFToAdd);

                int nPageCount = oNewPDF.PageCount;
                for (int i = 0; i < nPageCount; i++)
                {
                    pdftron.PDF.Page oNewPage = oNewPDF.GetPage(i + 1);
                    PageIterator oPageIterator = null;
                    if (bAddafter)
                        oPageIterator = oCurrentPDF.GetPageIterator(nPageNumber + i + 1);
                    else
                        oPageIterator = oCurrentPDF.GetPageIterator(nPageNumber + i);
                    oCurrentPDF.PageInsert(oPageIterator, oNewPage);
                }
                oCurrentPDF.Save(sExistingPDF, SDFDoc.SaveOptions.e_compatibility);
                oCurrentPDF.Close();
                oCurrentPDF.Dispose();
                oNewPDF.Close();
                oNewPDF.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError("Exception when extracting page  " + ex.Message);
            }
            return true;
        }
        */



        public int getNumberOfPages()
        {
            if (_PDFDoc != null)
            {
                int nPageCount = _PDFDoc.GetNumberOfPages();
                return nPageCount;
            }
            return -1;
        }

        public bool init(string sFileName)
        {
            bool bRet = true;
            FileInfo fi = new FileInfo(sFileName);
            if (fi.Exists)
            {
                _InputFileName = sFileName;
                try
                {
                    //_PDFDoc = IronPdf.PdfDocument.FromFile(sFileName);
                    _PDFDoc = new iText.Kernel.Pdf.PdfDocument(new PdfReader(sFileName));
                }
                catch (Exception ex)
                {
                    // IronPDF throws a general IronPdfException for encrypted PDFs
                    if (ex.Message.Contains("password") || ex.Message.Contains("encrypted"))
                    {

                        Console.WriteLine("The PDF is encrypted and requires a password.");
                        _docencrypted = true;
                    }
                    else
                    {
                        Console.WriteLine($"An error occurred: {ex.Message}");
                    }
                    bRet = false;
                }
            }
            return bRet;
        }

        public bool IsEncrypted()
        {
            return _docencrypted;
        }

        static public void indexDocument(ApplicationDbContext context, document oDocument)
        {
            string sIndexName = context.serversettings.FirstOrDefault(x => x.Name == "esdefaultindex").Value;
            if (!string.IsNullOrEmpty(oDocument.Publication.searchindexname))
                sIndexName = oDocument.Publication.searchindexname;

            if (!string.IsNullOrEmpty(oDocument.Publication.searchindexname))
            {
                sIndexName = oDocument.Publication.searchindexname;
            }

            ElasticSearchEngine oElasticSearchEngine = new ElasticSearchEngine(sIndexName);
            oElasticSearchEngine.createIndex(sIndexName);

            //Delete the document if already exists.
            oElasticSearchEngine.deleteDocument(sIndexName, oDocument.Id.ToString());
            oElasticSearchEngine.addDocument(sIndexName, oDocument);
        }

        static public bool createTextFiles(ApplicationDbContext context, document oDocument)
        {
            /*
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string htmlDir = System.IO.Path.Combine(sDocumentPath, "html");
            if (!Directory.Exists(htmlDir))
            {
                Directory.CreateDirectory(htmlDir);
            }
            string sFullPDFFile = System.IO.Path.Combine(sDocumentPath, oDocument.PDFFileName);
            PDF2HTML.convertAllPagesText(sFullPDFFile, sDocumentPath, htmlDir);
            SearchHighlight oSearchHighlight = new SearchHighlight(context);
            //-1 add all the pages to the DB.
            oSearchHighlight.addToDB(sFullPDFFile, htmlDir, -1, oDocument);
            */
            return true;
        }

        public bool createDocumentXML(string sPDFFile, createdocumentxmlinput oCreateDocumentXMLInput, bool bCreateVectorText, out bool docpagesizewarning)
        {
            docpagesizewarning = false;
            PDFDetails oPDFDetails = new PDFDetails();
            oPDFDetails.Init(sPDFFile);

            double dHiImageRes = oCreateDocumentXMLInput.Document.HiPageResolution;
            double dNormalImageRes = oCreateDocumentXMLInput.Document.NormalPageResolution;

            double dMinWidth, dMinHeight, dMaxWidth, dMaxHeight;
            dMinWidth = dMinHeight = dMaxWidth = dMaxHeight = 0;


            arrPagesID = new ArrayList();
            FileInfo oFileInfo = new FileInfo(sPDFFile);

            XmlDocument xmlDoc = new XmlDocument();
            XmlProcessingInstruction oPI = xmlDoc.CreateProcessingInstruction("xml", "version='1.0' encoding='utf-8'");
            XmlElement rootNode = xmlDoc.CreateElement("doc");

            XmlAttribute docAttribute;
            docAttribute = xmlDoc.CreateAttribute("id");
            docAttribute.Value = oCreateDocumentXMLInput.Document.Id.ToString();
            rootNode.Attributes.Append(docAttribute);

            docAttribute = xmlDoc.CreateAttribute("version");
            docAttribute.Value = oCreateDocumentXMLInput.Version;
            rootNode.Attributes.Append(docAttribute);

            string sUploadDate = DateTime.Now.ToShortDateString();

            docAttribute = xmlDoc.CreateAttribute("uploaddate");
            docAttribute.Value = sUploadDate;
            rootNode.Attributes.Append(docAttribute);

            docAttribute = xmlDoc.CreateAttribute("filename");
            docAttribute.Value = oFileInfo.Name;
            rootNode.Attributes.Append(docAttribute);

            docAttribute = xmlDoc.CreateAttribute("filesize");
            docAttribute.Value = oFileInfo.Length.ToString();
            rootNode.Attributes.Append(docAttribute);

            docAttribute = xmlDoc.CreateAttribute("publishdate");
            docAttribute.Value = DateTime.Now.ToShortDateString();
            rootNode.Attributes.Append(docAttribute);

            docAttribute = xmlDoc.CreateAttribute("title");
            docAttribute.Value = oCreateDocumentXMLInput.Document.Title;
            rootNode.Attributes.Append(docAttribute);

            docAttribute = xmlDoc.CreateAttribute("hidpi");
            docAttribute.Value = dHiImageRes.ToString();
            rootNode.Attributes.Append(docAttribute);

            docAttribute = xmlDoc.CreateAttribute("normaldpi");
            docAttribute.Value = dNormalImageRes.ToString();
            rootNode.Attributes.Append(docAttribute);


            xmlDoc.AppendChild(oPI);
            xmlDoc.AppendChild(rootNode);

            XmlElement docMetadata = xmlDoc.CreateElement("docmetadata");
            rootNode.AppendChild(docMetadata);

            XmlElement xmlPages = xmlDoc.CreateElement("pages");
            rootNode.AppendChild(xmlPages);

            int nPageCount = oPDFDetails.PagesCount;
            //int nWidth = 0;
            //int nHeight = 0;
            for (int i = 0; i < nPageCount; i++)
            {
                string sLabel = "";
                PdfPage oPage = _PDFDoc.GetPage(i + 1);
                if (oCreateDocumentXMLInput.ImportPageLabels && oPDFDetails.m_arrPageLabels != null)
                {
                    string sPageLabel = oPDFDetails.m_arrPageLabels[i];
                    if (!string.IsNullOrEmpty(sPageLabel))
                    {
                        sLabel = sPageLabel;
                    }
                }

                iText.Kernel.Geom.Rectangle PageSize = oPage.GetPageSize();

                double dPageWidth = PageSize.GetWidth();
                double dPageHeight = PageSize.GetHeight();
                double dHiRatio = dHiImageRes / 72.0;
                double dNormalRatio = dNormalImageRes / 72.0;
                /*
                if (nWidth > 0 && nWidth != (int)dPageWidth)
                    docpagesizewarning = true;
                if (nHeight > 0 && nHeight != (int)dPageHeight)
                    docpagesizewarning = true;
                */
                if (dMinWidth == 0)
                    dMinWidth = dPageWidth;
                if (dMinHeight == 0)
                    dMinHeight = dPageHeight;

                dMinWidth = Math.Min(dPageWidth, dMinWidth);
                dMinHeight = Math.Min(dPageHeight, dMinHeight);

                dMaxHeight = Math.Max(dPageHeight, dMaxHeight);
                dMaxWidth = Math.Max(dPageWidth, dMaxWidth);

                //nWidth  = (int)dPageWidth; 
                //nHeight = (int)dPageHeight;

                int nThumbWidth;
                int nThumbheight;

                if (oCreateDocumentXMLInput.ThumbWidth > 0)
                {
                    nThumbWidth = oCreateDocumentXMLInput.ThumbWidth;
                    nThumbheight = System.Convert.ToInt32(dPageHeight / dPageWidth * nThumbWidth);
                }
                else
                {
                    nThumbWidth = (int)(dPageWidth * 20 / 72);
                    nThumbheight = (int)(dPageHeight * 20 / 72);
                }
                if (i == 0)
                    _nFirstThumbHeight = nThumbheight;
                XmlElement xmlPage = xmlDoc.CreateElement("page");
                xmlPages.AppendChild(xmlPage);

                XmlAttribute pageAttribute;



                int nPageNumber = i + 1;
                pageAttribute = xmlDoc.CreateAttribute("num");
                pageAttribute.Value = nPageNumber.ToString();
                xmlPage.Attributes.Append(pageAttribute);


                pageAttribute = xmlDoc.CreateAttribute("label");
                pageAttribute.Value = sLabel;
                xmlPage.Attributes.Append(pageAttribute);

                pageAttribute = xmlDoc.CreateAttribute("ver");
                pageAttribute.Value = "1";
                xmlPage.Attributes.Append(pageAttribute);

                string sPageID = Guid.NewGuid().ToString();
                pageAttribute = xmlDoc.CreateAttribute("id");
                pageAttribute.Value = sPageID;
                xmlPage.Attributes.Append(pageAttribute);
                arrPagesID.Add(sPageID);

                ////////////////////////////////////////////////////////////////////////////////////////////
                string sThumbnail = string.Format("{0}{1}.jpg", oCreateDocumentXMLInput.ThumbPrefix, i + 1);
                XmlElement xmlThumbnail = xmlDoc.CreateElement("thumbnail");
                xmlPage.AppendChild(xmlThumbnail);

                XmlAttribute oThumbnailAttr;
                oThumbnailAttr = xmlDoc.CreateAttribute("file");
                oThumbnailAttr.Value = sThumbnail;
                xmlThumbnail.Attributes.Append(oThumbnailAttr);

                oThumbnailAttr = xmlDoc.CreateAttribute("width");
                oThumbnailAttr.Value = nThumbWidth.ToString();
                xmlThumbnail.Attributes.Append(oThumbnailAttr);

                oThumbnailAttr = xmlDoc.CreateAttribute("height");
                oThumbnailAttr.Value = nThumbheight.ToString();
                xmlThumbnail.Attributes.Append(oThumbnailAttr);

                string sPages = string.Format("{0}{1}.jpg", oCreateDocumentXMLInput.PagesPrefix, i + 1);
                XmlElement xmlNormal = xmlDoc.CreateElement("normal");
                xmlPage.AppendChild(xmlNormal);

                XmlAttribute oNormalAttr;
                oNormalAttr = xmlDoc.CreateAttribute("width");
                double dNormalWidth = System.Math.Round(dPageWidth * dNormalRatio, 2);
                oNormalAttr.Value = dNormalWidth.ToString();
                xmlNormal.Attributes.Append(oNormalAttr);

                oNormalAttr = xmlDoc.CreateAttribute("height");
                double dNormalHeight = System.Math.Round(dPageHeight * dNormalRatio, 2);
                oNormalAttr.Value = dNormalHeight.ToString();
                xmlNormal.Attributes.Append(oNormalAttr);

                string sNormalPages = string.Format("{0}{1}.{2}", oCreateDocumentXMLInput.PagesPrefix, i + 1, "jpg");
                oNormalAttr = xmlDoc.CreateAttribute("file");
                oNormalAttr.Value = sNormalPages;
                xmlNormal.Attributes.Append(oNormalAttr);



                string sZoomPages = string.Format("{0}{1}.{2}", oCreateDocumentXMLInput.ZoomPrefix, i + 1, oCreateDocumentXMLInput.ZoomPostfix);
                XmlElement xmlHi = xmlDoc.CreateElement("hi");
                xmlPage.AppendChild(xmlHi);

                XmlAttribute oHiAttr;
                oHiAttr = xmlDoc.CreateAttribute("file");
                oHiAttr.Value = sZoomPages;
                xmlHi.Attributes.Append(oHiAttr);

                oHiAttr = xmlDoc.CreateAttribute("width");
                oHiAttr.Value = dNormalWidth.ToString();
                xmlHi.Attributes.Append(oHiAttr);

                oHiAttr = xmlDoc.CreateAttribute("height");
                oHiAttr.Value = dNormalHeight.ToString();
                xmlHi.Attributes.Append(oHiAttr);


                string sZoomPagesImages = string.Format("{0}{1}.{2}", oCreateDocumentXMLInput.ZoomPrefix, i + 1, "jpg");
                XmlElement xmlHiImage = xmlDoc.CreateElement("hiimage");
                xmlPage.AppendChild(xmlHiImage);

                XmlAttribute oHiImageAttr;
                oHiImageAttr = xmlDoc.CreateAttribute("file");
                oHiImageAttr.Value = sZoomPagesImages;
                xmlHiImage.Attributes.Append(oHiImageAttr);

                oHiImageAttr = xmlDoc.CreateAttribute("width");
                double nHiWidth = System.Math.Round(dNormalWidth * dHiRatio);
                oHiImageAttr.Value = nHiWidth.ToString();
                xmlHiImage.Attributes.Append(oHiImageAttr);

                oHiImageAttr = xmlDoc.CreateAttribute("height");
                double nHiHeight = System.Math.Round(dNormalHeight * dHiRatio);
                oHiImageAttr.Value = nHiHeight.ToString();
                xmlHiImage.Attributes.Append(oHiImageAttr);

                string sData = string.Format("Page_{0}.xml", i + 1);
                XmlElement xmlData = xmlDoc.CreateElement("data");
                xmlPage.AppendChild(xmlData);

                XmlAttribute oDataAttr;
                oDataAttr = xmlDoc.CreateAttribute("file");
                oDataAttr.Value = sData;
                xmlData.Attributes.Append(oDataAttr);


                string sHTMLFileName = string.Format("Page_{0}.html", i + 1);
                XmlElement xmlHTML = xmlDoc.CreateElement("htmlview");
                xmlPage.AppendChild(xmlHTML);

                XmlAttribute oHTMLAttr;
                oHTMLAttr = xmlDoc.CreateAttribute("file");
                oHTMLAttr.Value = sHTMLFileName;
                xmlHTML.Attributes.Append(oHTMLAttr);
            }

            XmlElement xmlBookmarks = xmlDoc.CreateElement("bookmarks");
            rootNode.AppendChild(xmlBookmarks);
            XmlElement xmlTOC;
            XmlAttribute oXmlAttribute = xmlDoc.CreateAttribute("type");
            xmlTOC = xmlDoc.CreateElement("toc");
            oXmlAttribute.Value = oCreateDocumentXMLInput.TOCTargetPage == "0" ? "0" : "1";
            xmlTOC.Attributes.Append(oXmlAttribute);
            if (oCreateDocumentXMLInput.TOCTargetPage != "0")
            {
                XmlAttribute oXmlTargetPage = xmlDoc.CreateAttribute("targetpage");
                oXmlTargetPage.Value = oCreateDocumentXMLInput.TOCTargetPage;
                xmlTOC.Attributes.Append(oXmlTargetPage);
            }
            rootNode.AppendChild(xmlTOC);


            XmlElement xmlLinks = xmlDoc.CreateElement("links");
            rootNode.AppendChild(xmlLinks);

            XmlElement xmlnotes = xmlDoc.CreateElement("notes");
            rootNode.AppendChild(xmlnotes);

            _DocumentXML = System.IO.Path.Combine(oCreateDocumentXMLInput.OutputDirectory, "document.xml");
            xmlDoc.Save(_DocumentXML);
            return true;

        }

        public bool createPagesXML(string sPDFFile, createpagesxmlinput oCreatePagesXMLInput, string sOutputDirectory, int nPageCount)
        {
            //Set the page id base on the documnent XML
            string sOutputDir = sOutputDirectory;
            for (int j = 0; j < nPageCount; j++)
            {
                createPageXML(sOutputDirectory, j + 1, arrPagesID[j].ToString());
            }

            // c=1 recognize links.
            String command = string.Format("-i \"{0}\" -o \"{1}\" -e 0 -c 1 -d \"{2}\"", sPDFFile, sOutputDir, oCreatePagesXMLInput.Document.Id.ToString());
            string exePath = System.IO.Path.Combine(AppContext.BaseDirectory, "Tools", "PDFUtils", "PDFUtils.exe");
            ProcessStartInfo cmdsi = new ProcessStartInfo("PDFUtils.exe");
            cmdsi.Arguments = command;
            Process cmd = Process.Start(cmdsi);
            cmd.WaitForExit();
            return true;
        }

        public bool RenameFilesDelete(int nPageNumber, document doc, string sFolderName, int nNumberOfPagesRemoved)
        {
            //Delete the json and xml files.
            for (int i = 0; i < nNumberOfPagesRemoved; i++)
            {
                int nCurrentPage = nPageNumber + i;
                string sJsonFileToDelete = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.json", nCurrentPage));
                File.Delete(sJsonFileToDelete);

                string sXmlFileToDelete = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.xml", nCurrentPage));
                File.Delete(sXmlFileToDelete);

                string sThumbFileToDelete = System.IO.Path.Combine(sFolderName, string.Format("Thumbnail_{0}.jpg", nCurrentPage));
                File.Delete(sThumbFileToDelete);

                string sZPageFileToDelete = System.IO.Path.Combine(sFolderName, string.Format("ZPage_{0}.jpg", nCurrentPage));
                File.Delete(sZPageFileToDelete);

                string sSvgPageFileToDelete = System.IO.Path.Combine(sFolderName, string.Format("SPage_{0}.svg", nCurrentPage));
                if (File.Exists(sSvgPageFileToDelete))
                {
                    File.Delete(sSvgPageFileToDelete);
                }

                /*
                string sHTMLFolderName = Path.Combine(sFolderName, "html");
                string sXPageXMLFileToDelete = Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.xml", nCurrentPage));
                File.Delete(sXPageXMLFileToDelete);

                string sXPageJsonFileToRename = Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.json", i));
                */



            }

            int nPageCount = doc.NumberOfPages;
            for (int i = 0; i < nPageCount - nPageNumber; i++)
            {
                int nCurrentPage = nPageNumber + nNumberOfPagesRemoved + i;
                int nNewCurrentPage = nPageNumber + i;
                string sJsonFileToRename = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.json", nCurrentPage));
                string sJsonFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.json", nNewCurrentPage));
                if (File.Exists(sJsonFileToRename))
                    File.Move(sJsonFileToRename, sJsonFileToRenameNew);

                string sXmlFileToRename = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.xml", nCurrentPage));
                string sXmlFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("Page_{0}.xml", nNewCurrentPage));
                if (File.Exists(sXmlFileToRename))
                    File.Move(sXmlFileToRename, sXmlFileToRenameNew);

                string sThumbFileToRename = System.IO.Path.Combine(sFolderName, string.Format("Thumbnail_{0}.jpg", nCurrentPage));
                string sThumbFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("Thumbnail_{0}.jpg", nNewCurrentPage));
                if (File.Exists(sThumbFileToRename))
                    File.Move(sThumbFileToRename, sThumbFileToRenameNew);

                string sZPageFileToRename = System.IO.Path.Combine(sFolderName, string.Format("ZPage_{0}.jpg", nCurrentPage));
                string sZPageFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("ZPage_{0}.jpg", nNewCurrentPage));
                if (File.Exists(sZPageFileToRename))
                    File.Move(sZPageFileToRename, sZPageFileToRenameNew);

                string sZSVGPageFileToRename = System.IO.Path.Combine(sFolderName, string.Format("SPage_{0}.svg", nCurrentPage));
                string sZSVGPageFileToRenameNew = System.IO.Path.Combine(sFolderName, string.Format("SPage_{0}.svg", nNewCurrentPage));
                if (File.Exists(sZSVGPageFileToRename))
                    File.Move(sZSVGPageFileToRename, sZSVGPageFileToRenameNew);

                /*
                string sHTMLFolderName = Path.Combine(sFolderName, "html");
                string sXPageXMLFileToRename = Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.xml", nCurrentPage));
                string sXPageXMLFileToRenameNew = Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.xml", i + nNewCurrentPage));
                if (File.Exists(sXPageXMLFileToRename))
                    File.Move(sXPageXMLFileToRename, sXPageXMLFileToRenameNew);

                string sXPageJsonFileToRename = Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.json", nCurrentPage));
                string sXPageJsonFileToRenameNew = Path.Combine(sHTMLFolderName, string.Format("XPage_{0}.json", i + nNewCurrentPage));
                if (File.Exists(sXPageJsonFileToRename))
                    File.Move(sXPageJsonFileToRename, sXPageJsonFileToRenameNew);
                */
                //Thumbnail_1
            }
            string sHTMLFolderName = System.IO.Path.Combine(sFolderName, "html");
            if (Directory.Exists(sHTMLFolderName))
            {
                Directory.Delete(sHTMLFolderName, true);
                Directory.CreateDirectory(sHTMLFolderName);
            }

            return true;
        }
    }
}