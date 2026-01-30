using Amazon.Util.Internal;
using core;
using Core;
using DocumentFormat.OpenXml.Office2010.PowerPoint;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Asn1.IsisMtt.X509;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Core.Models;
using DCatalogCommon.Data;
using iText.Kernel.Pdf.Annot;
using iTextSharp.text.pdf;
using iTextSharp.text;
using DocumentFormat.OpenXml.Wordprocessing;
using Force.Crc32;

namespace JobWorker.Jobs
{
    public class JobExecutionSaveLinksToPDF : IJobExecution
    {

        private readonly ApplicationDbContext _context;
        public JobExecutionSaveLinksToPDF(ApplicationDbContext context)
        {
            _context = context;
        }

        protected string generateJobFile(job oJob, savelinkstopdfinput oSaveLinksToPdfInput)
        {
            string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
            string sJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");


            XmlDocument oLinksParams = new XmlDocument();
            XmlDeclaration xmlDeclaration = oLinksParams.CreateXmlDeclaration("1.0", "utf-8", null);
            //Create the root element
            XmlElement rootNode = oLinksParams.CreateElement("job");
            rootNode.SetAttribute("name", "SaveLinksToPdf");

            oLinksParams.InsertBefore(xmlDeclaration, oLinksParams.DocumentElement);
            oLinksParams.AppendChild(rootNode);

            XmlElement inputfilexml = oLinksParams.CreateElement("inputfile");
            inputfilexml.InnerText = oSaveLinksToPdfInput.InputFileName;
            rootNode.AppendChild(inputfilexml);

            XmlElement outputdir = oLinksParams.CreateElement("outputdir");
            outputdir.InnerText = oSaveLinksToPdfInput.OutputDirectory;
            rootNode.AppendChild(outputdir);

            if (oSaveLinksToPdfInput.Document != null)
            {
                XmlElement xmldocid = oLinksParams.CreateElement("docid");
                xmldocid.InnerText = oSaveLinksToPdfInput.Document.Id.ToString();
                rootNode.AppendChild(xmldocid);
            }

            XmlElement savebookmarks = oLinksParams.CreateElement("savebookmarks");
            if (oSaveLinksToPdfInput.savebookmarks)
                savebookmarks.InnerText = "1";
            else
                savebookmarks.InnerText = "0";
            rootNode.AppendChild(savebookmarks);


            XmlElement usejson = oLinksParams.CreateElement("usejson");
            usejson.InnerText = "1";
            rootNode.AppendChild(usejson);




            rootNode.SetAttribute("id", oJob.Id.ToString());

            oLinksParams.Save(sJobFile);
            return sJobFile;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {

            oJob.Progress = 10;
            oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
            document oDocument = null;
            //try
            {
                
                savelinkstopdfinput oSaveLinksToPdfinput = _context.savelinkstopdfinput
                  .Where(d => d.Job.Id == oJob.Id)
                  .SingleOrDefault();
                if (oSaveLinksToPdfinput != null)
                {

                    oDocument = oSaveLinksToPdfinput.Document;
                    //sInputFileName = oSaveLinksToPdfinput.InputFileName;
                    //sOutputDirectory = oSaveLinksToPdfinput.OutputDirectory;
                    //string sJobFile = generateJobFile(oJob, oSaveLinksToPdfinput);
                    //sFilename = oGenerateImagesInput.filename;
                    //bGenerateThumbnails = oGenerateImagesInput.generatethumbnails;
                }

                DCS3Services oDCS3Services = new DCS3Services();
                string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
                string sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName) ? Core.Constants.DEFAULT_DOCS_LOCATION : oDocument.Publication.Publisher.BucketName;
                string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
                string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
                string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                    oDocument.Id);


                //1. Download files.
                bool bDownloadedPDF = downloadFiles(oDocument, sBucketName);
                if (!bDownloadedPDF)
                {
                    oJob.Progress = 100;
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJob.Desctiption = "Cannot find the PDF File";

                }
                oJob.Progress = 30;
                oJob.Status = Constants.JobProcessingStatus.Processing.ToString();

                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);

                string sPDFWithLinksFileName = oDocument.PDFFileName.Replace(".pdf", "_Links.pdf");
                string sFullPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
                string sFullTempFileName = Path.Combine(sDocumentPath, sPDFWithLinksFileName);
                bool bSaveLinks = saveLinksToPDF(oDocument, sFullTempFileName);
                if (bSaveLinks)
                {
                    oDocument.PDFForDownloadFile = sPDFWithLinksFileName;
                    //oDocument.PDFFileName = sPDFWithLinksFileName;
                    oDCS3Services.uploadFile(sBucketName, sFullTempFileName, sKeyPrefix, sPDFWithLinksFileName);
                    string sDocumentJson = Path.Combine(sDocumentPath, "document.json");
                    oDCS3Services.uploadFile(sBucketName, sDocumentJson, sKeyPrefix);

                }
                oJob.Progress = 100;
                oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
            }
            return true;

        }

        //show "D:\DCatalog\AyrKing-LLC-Capabilities-Brochure.pdf" savelinksfromjson "D:\DCatalog\Docs\DCatalog-Inc\Test\eaf95318-b509-4432-a7e6-cbf48499e325\document.json"
        /*
        public static bool saveLinksToPDF(document oDocument,string sFullTempFileName)
        {

            bool bSaveLinkBorder = true;
            string sColor = oDocument.Publication.PublicationTemplate.bordercolor;

            if (oDocument.Publication.PublicationTemplate.linkBorder == 0 || sColor == "")
                bSaveLinkBorder = false;
           
            double dOpacity = oDocument.Publication.PublicationTemplate.linkOpacity;

            string sDocumentPath = DocumentUtil.getDocumentPath(oDocument);
            string sDocumentJson = Path.Combine(sDocumentPath, "document.json");
            string sFullPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
            int nPageCount = oDocument.NumberOfPages;
            bool bDirty = false;
           
            PdfReader reader = new PdfReader(sFullPDFFileName);
            PdfStamper stamper = new PdfStamper(reader, new FileStream(sFullTempFileName, FileMode.Create));
            PdfContentByte overContent = stamper.GetOverContent(2);
            


            double pagerex = oDocument.HiPageResolution;

            PdfWriter writer = stamper.Writer;
            for (int i = 0; i < nPageCount; i++)
            {
                iTextSharp.text.Rectangle pagerectangle = reader.GetCropBox(i+1);
                float pageHeight = pagerectangle.Height;
                float pageWidth= pagerectangle.Width;
                int nCurrentPage = i + 1;
                string sName = string.Format("Page_{0}.json", nCurrentPage);
                string sPagePathSource = Path.Combine(sDocumentPath, sName);
                JObject pageJson = JObject.Parse(System.IO.File.ReadAllText(sPagePathSource));
                foreach (JObject x in pageJson["links"]["link"])
                {
                    JValue JLinkFromPDF = (JValue)x["@attributes"]["frompdf"];
                    if(JLinkFromPDF !=null && JLinkFromPDF.Value.ToString() =="1")
                        continue;
                    JValue JLinkX1 = (JValue)x["@attributes"]["x1"];
                    JValue JLinkX2 = (JValue)x["@attributes"]["x2"];
                    JValue JLinkY1 = (JValue)x["@attributes"]["y1"];
                    JValue JLinkY2 = (JValue)x["@attributes"]["y2"];

                    JValue JOpacity = (JValue)x["@attributes"]["opacity"];
                    JValue JColor = (JValue)x["@attributes"]["color"];
                    if (JColor != null)
                        sColor = JColor.Value.ToString();
                    if (JOpacity != null)
                        dOpacity = System.Convert.ToDouble(JOpacity.Value);
                    if (!sColor.StartsWith("0x"))
                        sColor = "0x" + sColor;
                    Color color = ColorTranslator.FromHtml(sColor);
                    int r = Convert.ToInt16(color.R);
                    int g = Convert.ToInt16(color.G);
                    int b = Convert.ToInt16(color.B);


                    double x1 = System.Convert.ToDouble(JLinkX1.Value.ToString()) / (pagerex / 72);
                    double x2 = System.Convert.ToDouble(JLinkX2.Value.ToString()) / (pagerex / 72);
                    double y1 = System.Convert.ToDouble(JLinkY1.Value.ToString()) / (pagerex / 72);
                    double y2 = System.Convert.ToDouble(JLinkY2.Value.ToString()) / (pagerex / 72);

                    x1 = x1 + pagerectangle.Left;
                    x2 = x2 + pagerectangle.Left;
                    y1 = pageHeight - y1;
                    y2 = pageHeight - y2;

                    if (x1 > pageWidth + pagerectangle.Left)
                        continue;

                    if (x1 < 0 || x2 < 0 || y1 < 0 || y2 < 0)
                        continue;

                    bDirty = true;
                    JValue JLinkType = (JValue)x["@attributes"]["type"];
                    JValue JLinkURL = (JValue)x["@attributes"]["url"];

                    iTextSharp.text.Rectangle rectangle = new iTextSharp.text.Rectangle((float)x1, (float)y1, (float)x2, (float)y2, 0);
                    overContent.Rectangle(rectangle);
                    string sExternalURL = JLinkURL?.Value.ToString();
                    if (JLinkType.Value.ToString() == "0" || JLinkType.Value.ToString() =="1")
                    {
                       
                        PdfAnnotation annotation = PdfAnnotation.CreateLink(stamper.Writer, rectangle, PdfAnnotation.HIGHLIGHT_OUTLINE, new PdfAction(sExternalURL));
                        if(bSaveLinkBorder)
                            annotation.Color = new BaseColor(r, g, b, (int)dOpacity * 255);
                        else
                            annotation.BorderStyle = new PdfBorderDictionary(0, 0);

                        stamper.AddAnnotation(annotation, nCurrentPage);


                    }
                    if (JLinkType.Value.ToString() == "2")
                    {
                        int nPageNumber = System.Convert.ToInt32(sExternalURL);
                        if (nPageNumber > 0)
                        {
                            try
                            {
                                PdfAction oPdfAction = PdfAction.GotoLocalPage(nPageNumber, new PdfDestination(PdfDestination.FIT), writer);
                                PdfAnnotation annotation = PdfAnnotation.CreateLink(stamper.Writer, rectangle, PdfAnnotation.HIGHLIGHT_OUTLINE, oPdfAction);
                                if (bSaveLinkBorder)
                                    annotation.Color = new BaseColor(r, g, b, (int)dOpacity * 255);
                                else
                                    annotation.BorderStyle = new PdfBorderDictionary(0, 0);
                                stamper.AddAnnotation(annotation, nCurrentPage);
                            }
                            catch(Exception ex)
                            {
                                //Invlid link continue
                            }

                        }
                        
                    }

                   

                }


            }

            JObject documentJson = JObject.Parse(System.IO.File.ReadAllText(sDocumentJson));
            documentJson["issue"]["@attributes"]["pdfURL"] = new FileInfo(sFullTempFileName).Name;
            File.WriteAllText(sDocumentJson, documentJson.ToString());
            stamper.Close();
            try
            {
                reader.Close();
            }
            catch(Exception e)
            {

            }
           

            return true;
            
        }
        */

        public static bool saveLinksToPDF(document oDocument, string sFullTempFileName)
        {
            bool bSaveLinkBorder = true;
            string sColor = oDocument.Publication.PublicationTemplate.bordercolor;
            if (oDocument.Publication.PublicationTemplate.linkBorder == 0 || string.IsNullOrEmpty(sColor))
                bSaveLinkBorder = false;

            double dOpacity = oDocument.Publication.PublicationTemplate.linkOpacity;

            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string sDocumentJson = Path.Combine(sDocumentPath, "document.json");
            string sFullPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
            int nPageCount = oDocument.NumberOfPages;
            bool bDirty = false;
            double pagerex = oDocument.HiPageResolution;

            using (PdfReader reader = new PdfReader(sFullPDFFileName))
            using (PdfStamper stamper = new PdfStamper(reader, new FileStream(sFullTempFileName, FileMode.Create)))
            {
                PdfWriter writer = stamper.Writer;

                for (int i = 0; i < nPageCount; i++)
                {
                    int nCurrentPage = i + 1;
                    var pageRect = reader.GetCropBox(nCurrentPage);
                    float pageHeight = pageRect.Height;
                    float pageWidth = pageRect.Width;

                    string sPageJsonPath = Path.Combine(sDocumentPath, $"Page_{nCurrentPage}.json");
                    if (!File.Exists(sPageJsonPath)) continue;

                    JObject pageJson = JObject.Parse(File.ReadAllText(sPageJsonPath));
                    foreach (JObject x in pageJson["links"]["link"])
                    {
                        JValue JLinkFromPDF = (JValue)x["@attributes"]["frompdf"];
                        if (JLinkFromPDF?.Value?.ToString() == "1") continue;

                        double x1 = Convert.ToDouble(x["@attributes"]["x1"].ToString()) / (pagerex / 72);
                        double x2 = Convert.ToDouble(x["@attributes"]["x2"].ToString()) / (pagerex / 72);
                        double y1 = Convert.ToDouble(x["@attributes"]["y1"].ToString()) / (pagerex / 72);
                        double y2 = Convert.ToDouble(x["@attributes"]["y2"].ToString()) / (pagerex / 72);

                        // Adjust for CropBox offsets
                        x1 += pageRect.Left;
                        x2 += pageRect.Left;
                        y1 = pageRect.Bottom + (pageHeight - y1);
                        y2 = pageRect.Bottom + (pageHeight - y2);

                        if (x1 < 0 || x2 < 0 || y1 < 0 || y2 < 0 || x1 > pageRect.Right || x2 > pageRect.Right)
                            continue;

                        bDirty = true;

                        JValue JLinkType = (JValue)x["@attributes"]["type"];
                        JValue JLinkURL = (JValue)x["@attributes"]["url"];
                        JValue JColor = (JValue)x["@attributes"]["color"];
                        JValue JOpacity = (JValue)x["@attributes"]["opacity"];

                        if (JColor != null)
                            sColor = JColor.Value.ToString();
                        if (JOpacity != null)
                            dOpacity = Convert.ToDouble(JOpacity.Value);

                        if (!sColor.StartsWith("0x"))
                            sColor = "0x" + sColor;
                        System.Drawing.Color color = ColorTranslator.FromHtml(sColor);
                        int r = color.R, g = color.G, b = color.B;

                        // Safe rectangle
                        float rectX1 = (float)Math.Min(x1, x2);
                        float rectX2 = (float)Math.Max(x1, x2);
                        float rectY1 = (float)Math.Min(y1, y2);
                        float rectY2 = (float)Math.Max(y1, y2);
                        var rectangle = new iTextSharp.text.Rectangle(rectX1, rectY1, rectX2, rectY2);

                        iTextSharp.text.pdf.PdfAnnotation annotation = null;
                        string sExternalURL = JLinkURL?.Value?.ToString();

                        if (JLinkType?.Value?.ToString() == "0" || JLinkType?.Value?.ToString() == "1" || JLinkType?.Value?.ToString() == "5")
                        {
                            annotation = iTextSharp.text.pdf.PdfAnnotation.CreateLink(writer, rectangle, iTextSharp.text.pdf.PdfAnnotation.HIGHLIGHT_OUTLINE, new PdfAction(sExternalURL));
                        }
                        else if (JLinkType?.Value?.ToString() == "2" && int.TryParse(sExternalURL, out int nPageTarget) && nPageTarget > 0)
                        {
                            try
                            {
                                var action = PdfAction.GotoLocalPage(nPageTarget, new PdfDestination(PdfDestination.FIT), writer);
                                annotation = iTextSharp.text.pdf.PdfAnnotation.CreateLink(writer, rectangle, iTextSharp.text.pdf.PdfAnnotation.HIGHLIGHT_OUTLINE, action);
                            }
                            catch (Exception ex)
                            {
                                // Optionally log: invalid internal page number
                                continue;
                            }
                        }

                        if (annotation != null)
                        {
                            if (bSaveLinkBorder)
                                annotation.Color = new BaseColor(r, g, b, (int)(dOpacity * 255));
                            else
                                annotation.BorderStyle = new PdfBorderDictionary(0, 0);

                            stamper.AddAnnotation(annotation, nCurrentPage);
                        }
                    }
                }

                // Save document.json with updated pdfURL
                if (File.Exists(sDocumentJson))
                {
                    JObject documentJson = JObject.Parse(File.ReadAllText(sDocumentJson));
                    documentJson["issue"]["@attributes"]["pdfURL"] = new FileInfo(sFullTempFileName).Name;
                    File.WriteAllText(sDocumentJson, documentJson.ToString());
                }
            }

            return true;
        }




        public bool downloadFiles(document oDocument, string sBucketName)
        {
            //Download the PDF File.
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                oDocument.Id);
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);

            DCS3Services oDCS3Services = new DCS3Services();
            //PDF
            string sPDFKeyName = string.Format("{0}/{1}", sKeyPrefix, oDocument.PDFFileName);
            string sHTMLDocumentPath = Path.Combine(sDocumentPath, "html");
            string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);


            bool CRCMatch = false;
            if (File.Exists(sPDFFileName) && oDocument.crc32 != 0)
            {
                byte[] data = File.ReadAllBytes(sPDFFileName);
                uint hash = Crc32Algorithm.Compute(data);
                if (oDocument.crc32 == hash)
                {
                    CRCMatch = true;

                }
            }

            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);
            else
            {
                if (CRCMatch == false)
                {
                    Directory.Delete(sDocumentPath, true);
                    Directory.CreateDirectory(sDocumentPath);
                }
                else
                {
                    List<string> files = new List<string>(System.IO.Directory.GetFiles(sDocumentPath));
                    files.ForEach(x =>
                    {
                        try
                        {
                            if (x != sPDFFileName)
                                System.IO.File.Delete(x);
                        }
                        catch { }
                    });
                    if (Directory.Exists(sHTMLDocumentPath))
                        Directory.Delete(sHTMLDocumentPath, true);
                }
            }


            if (!File.Exists(sPDFFileName))
                oDCS3Services.downloadFile(sBucketName, sPDFKeyName, sPDFFileName);

            //Json
            string sDocumentJsonKey = string.Format("{0}/{1}", sKeyPrefix, "document.json");
            string sDocumentJsonFile = Path.Combine(sDocumentPath, "document.json");
            oDCS3Services.downloadFile(sBucketName, sDocumentJsonKey, sDocumentJsonFile);




            //Images,SVG and Thumbnail.
            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
            int nPageCount = oDocument.NumberOfPages;

            for (int i = 0; i < nPageCount; i++)
            {
                int nCurrentPage = i + 1;
                string sName = string.Format("Page_{0}.json", nCurrentPage);
                string sJsonFileToDownload = Path.Combine(sDocumentPath, sName);
                string sJsonFileToDownloadKey = string.Format("{0}/{1}", sKeyPrefix, sName);
                oDCS3Services.downloadFile(sBucketName, sJsonFileToDownloadKey, sJsonFileToDownload);
            }
            return true;
        }
    }
}
