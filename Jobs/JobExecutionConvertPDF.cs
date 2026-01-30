using Amazon.Util.Internal;
using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using MySqlX.XDevAPI;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Org.BouncyCastle.Asn1.IsisMtt.X509;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using GifFlippingBook;
using DCJobs;
using Renci.SshNet;
using Core.Services;
using Force.Crc32;
using core.Common;

namespace JobWorker.Jobs
{
    public class JobExecutionConvertPDF : IJobExecution
    {
        protected bool docpagesizewarning = false;
        private readonly ILogger<JobExecutionConvertPDF> _log;
        //ApplicationDbContext _context = null;
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;

        public object DocumentUtil { get; private set; }

        public JobExecutionConvertPDF(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<JobExecutionConvertPDF> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }

        protected bool generateSelectableText(document doc, out bool bDocumentContainText)
        {
            try
            {
                bDocumentContainText = false;
                int nNumberOfPages = doc.NumberOfPages;
                string sDocumentPath = DocumentUtilBase.getDocumentPath(doc);
                if (!Directory.Exists(sDocumentPath))
                    Directory.CreateDirectory(sDocumentPath);
                string sPublisherName = Utility.GenerateFriendlyURL(doc.Publication.Publisher.Name);
                string sPublicationName = Utility.GenerateFriendlyURL(doc.Publication.Name);

                string sPageName = string.Format("XPage_{0}.xml", 1);
                string sOutputPageName = string.Format("JPage_{0}.json", 1);
                string sPageFileName = Path.Combine(sDocumentPath, sPageName);
                sDocumentPath = Path.Combine(sDocumentPath, "html");
                for (int i = 0; i < nNumberOfPages; i++)
                {
                    int id = 0;
                    sPageName = string.Format("XPage_{0}.xml", i + 1);
                    string sOutPageName = string.Format("XPage_{0}.json", i + 1);


                    sPageFileName = Path.Combine(sDocumentPath, sPageName);
                    string sOutputPageFileName = Path.Combine(sDocumentPath, sOutPageName);
                    string sKeyPrefix = string.Format("{0}/{1}/{2}/{3}", sPublisherName, sPublicationName, doc.Id.ToString(), sPageName);
                    //if (oDCS3Services.downloadFile(DCCore.Constants.DEFAULT_DOC_BUCKET, sKeyPrefix, sPageFileName))
                    {
                        if (File.Exists(sPageFileName))
                        {
                            XmlDocument xmlDoc = new XmlDocument();

                            using (StreamReader oReader = new StreamReader(sPageFileName, Encoding.GetEncoding("utf-8")))
                            {
                                xmlDoc.Load(oReader);

                            }

                            //XmlDocument xmlDoc = new XmlDocument();
                            //xmlDoc.Load(sPageFileName);
                            if (xmlDoc.FirstChild.NodeType == XmlNodeType.XmlDeclaration)
                                xmlDoc.RemoveChild(xmlDoc.FirstChild);

                            string jsonPageData = JsonConvert.SerializeXmlNode(xmlDoc);
                            JObject pageJson = JObject.Parse(jsonPageData);
                            IEnumerable<JToken> jWords = pageJson.SelectTokens("..Line..Word");

                            foreach (JToken jword in jWords)
                            {
                                bDocumentContainText = true;
                                int nWordsCount = jword.Count();
                                for (int j = 0; j < nWordsCount; j++)
                                {
                                    if (jword is JArray)
                                    {
                                        jword[j]["id"] = id.ToString();
                                        id++;
                                    }
                                    else
                                    {
                                        jword["id"] = id.ToString();
                                        id++;
                                        break; //This is not array only 1 word 
                                    }
                                }
                            }

                            System.IO.File.WriteAllText(sOutputPageFileName, pageJson.ToString());
                        }
                    }
                }
            }
            catch (Exception e)
            {
                bDocumentContainText = false;
                return false;
            }
            return true;
        }

        public void copyIntro(ApplicationDbContext context,string sSourceDocumentId, string sTargetDocumentId)
        {

            DCS3Services oDCS3Services = new DCS3Services();

            document oDocument = context.document
                   .Include(d => d.Publication)
                   .Include(d => d.Publication.Publisher)
                   .Where(d => d.Id == sTargetDocumentId)
                   .SingleOrDefault();

            document oSourceDocument = context.document
                  .Include(d => d.Publication)
                  .Include(d => d.Publication.Publisher)
                  .Where(d => d.Id == sSourceDocumentId)
                  .SingleOrDefault();


            //document oDocument = DocumentUtil.getDocumentById(sTargetDocumentId);
            //document oSourceDocument = DocumentUtil.getDocumentById(sSourceDocumentId);

            string sPublisherName = Utility.GenerateFriendlyURL(oSourceDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oSourceDocument.Publication.Name);

            string sSourceDirectory = DocumentUtilBase.getDocumentPath(oSourceDocument);
            string sTargetDirectory = DocumentUtilBase.getDocumentPath(oDocument);

            string sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName) ? Constants.DEFAULT_DOCS_LOCATION : oDocument.Publication.Publisher.BucketName;

            string sSourceFile = "";
            string sTargetFile = "";

            //File to copy.
            string sHiResImage = "ZPage_0.jpg";
            string sPageXMLFile = "Page_0.xml";
            string sDocumentXML = "document.xml";

            string sKeyPrefix = "";

            if (File.Exists(sSourceFile))
                File.Copy(sSourceFile, sTargetFile, true);

            sSourceFile = Path.Combine(sSourceDirectory, sHiResImage);
            if (!Directory.Exists(sSourceDirectory))
                Directory.CreateDirectory(sSourceDirectory);
            sTargetFile = Path.Combine(sTargetDirectory, sHiResImage);
            if (File.Exists(sSourceFile))
                File.Copy(sSourceFile, sTargetFile, true);
            else
            {
                sKeyPrefix = string.Format("{0}/{1}/{2}/{3}", sPublisherName, sPublicationName, sSourceDocumentId, sHiResImage);
                oDCS3Services.downloadFile(sBucketName, sKeyPrefix, sTargetFile);

            }
            sSourceFile = Path.Combine(sSourceDirectory, sPageXMLFile);
            sTargetFile = Path.Combine(sTargetDirectory, sPageXMLFile);
            if (File.Exists(sSourceFile))
                File.Copy(sSourceFile, sTargetFile, true);
            else
            {
                sKeyPrefix = string.Format("{0}/{1}/{2}/{3}", sPublisherName, sPublicationName, sSourceDocumentId, sPageXMLFile);
                oDCS3Services.downloadFile(sBucketName, sKeyPrefix, sTargetFile);
            }

            sSourceFile = Path.Combine(sSourceDirectory, sDocumentXML);
            sTargetFile = Path.Combine(sTargetDirectory, sDocumentXML);

            XmlDocument oDocumentXMLSource = new XmlDocument();
            XmlDocument oDocumentXMLTarget = new XmlDocument();
            if (!File.Exists(sSourceFile))
            {
                sKeyPrefix = string.Format("{0}/{1}/{2}/{3}", sPublisherName, sPublicationName, sSourceDocumentId, sDocumentXML);
                oDCS3Services.downloadFile(sBucketName, sKeyPrefix, sSourceFile);

            }

            if (!File.Exists(sTargetFile))
            {
                sKeyPrefix = string.Format("{0}/{1}/{2}/{3}", sPublisherName, sPublicationName, sTargetDocumentId, sDocumentXML);
                oDCS3Services.downloadFile(sBucketName, sKeyPrefix, sTargetFile);

            }

            oDocumentXMLSource.Load(sSourceFile);

            string sXPathPage = string.Format("//page[@intro={0}]", "1");
            XmlNode oXmlNodeIntro = oDocumentXMLSource.SelectSingleNode(sXPathPage);

            oDocumentXMLTarget.Load(sTargetFile);

            XmlNode oXmlNodeIntroTarget = oDocumentXMLTarget.SelectSingleNode(sXPathPage);
            if (oXmlNodeIntroTarget != null)
            {
                oXmlNodeIntroTarget.ParentNode.RemoveChild(oXmlNodeIntroTarget);
            }


            XmlNode oXmlNodeListsSource = oDocumentXMLTarget.ImportNode(oXmlNodeIntro, true);
            XmlNode oPages = oDocumentXMLTarget.SelectSingleNode("//pages");
            oPages.PrependChild(oXmlNodeListsSource);

            oDocumentXMLTarget.Save(sTargetFile);



        }

        private void SearchProductsInDocument(ApplicationDbContext context,document d)
        {
            searchproductsindocumentinput osearchproductsindocumentinput = new searchproductsindocumentinput();
            osearchproductsindocumentinput.importasproduct = true;
            osearchproductsindocumentinput.importproductstodb = false;
            osearchproductsindocumentinput.updatealldocumentsinpublication = true;
            osearchproductsindocumentinput.producticonxoffset = 0;
            osearchproductsindocumentinput.producticonyoffset = 0;
            osearchproductsindocumentinput.Document = d;
            osearchproductsindocumentinput.deletepreviouslinks = true;

            SearchProductsInDocument oSearchProductsInDocument = new SearchProductsInDocument();
            oSearchProductsInDocument.executeSearchProducts(context, osearchproductsindocumentinput);

        }


        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JobExecutionConvertPDF job {JobId}", oJob.Id);
            bool bDocumentContainText = true;
            string sPDFFile = "";
            bool bUseVectorText;
            document oDocument = null;
            publisher? oPublisher = null;
            PostConversionCommunication oPostConversionCommunication = new PostConversionCommunication();
            postconversioninput oPostConversionInput = null;
            bool IsExceptionRaised = false;
            string ExceptionMsg = "";
            DocumentConvertor oDocumentConvertor = new DocumentConvertor();
            DCS3Services oDCS3Services = new DCS3Services();
            string sOutputDirectory = "";
            var oConvertPDFDocumentInput = await context.convertpdfdocumentinput
                            .Include(c => c.Job)  // eager load Job if needed
                            .Include(c => c.AWSFileUpload)  // eager load Job if needed
                            .Include(c => c.Document)  // eager load Job if needed
                            .Include(c => c.Document.Publication)  // eager load Job if needed
                            .Include(c=>c.Document.Publication.PublicationTemplate)
                            .Include(c => c.Document.Publication.Publisher)  // eager load Job if needed
                            .Include(c => c.Document.Publication.Publisher.Licenses)  // eager load Job if needed
                            .Where(c => c.Job.Id == oJob.Id)
                            .FirstOrDefaultAsync();
            if (oConvertPDFDocumentInput != null)
            {
                oDocument = oConvertPDFDocumentInput.Document;
                sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
                oDocument.DocumentStatus = Constants.JobProcessingStatus.Processing.ToString();
                context.Update(oDocument);
                context.SaveChanges();
            }

            //Download the file from the S3 before processing.
            if (oConvertPDFDocumentInput?.AWSFileUpload != null)
            {
                sPDFFile = Path.Combine(sOutputDirectory, oConvertPDFDocumentInput.AWSFileUpload.FileName);
                Utility.createDirectory(sOutputDirectory);
                oDCS3Services.downloadFile(oConvertPDFDocumentInput.AWSFileUpload.BucketName, oConvertPDFDocumentInput.AWSFileUpload.AWSFileName, sPDFFile);
            }

            oPostConversionInput = await context.postconversioninput
                                    .Include(c => c.Job)
                                    .Where(c => c.Job.Id == oJob.Id)
                                    .FirstOrDefaultAsync();



            var oPDF2VectorInput = await context.pdf2vectorinput
                                   .Include(c => c.Job)
                                   .Where(c => c.Job.Id == oJob.Id)
                                   .FirstOrDefaultAsync();

            bUseVectorText = oPDF2VectorInput != null;
            oPublisher = oDocument?.Publication?.Publisher;
            var oCreateDocumentXMLInput = await context.createdocumentxmlinput
                                           .Include(c => c.Job)
                                           .Where(c => c.Job.Id == oJob.Id)
                                           .FirstOrDefaultAsync();
            if (oCreateDocumentXMLInput != null)
            {
                oDocument = oCreateDocumentXMLInput.Document;
                //Override the document resolution based on publisher selection.
                oDocument.HiPageResolution = oDocument.Publication.ImageResolution;
                oDocument.DocumentProcessingDescription = oCreateDocumentXMLInput.Description;
                oDocument.DocumentProgressingPercent = 10;
                if (File.Exists(sPDFFile))
                    File.Delete(sPDFFile);
                sPDFFile = oCreateDocumentXMLInput.InputFileName;
                if (!Directory.Exists(sOutputDirectory))
                    Utility.createDirectory(sOutputDirectory);
                //If the file is a link to URL.

                Uri uriResult;
                bool bIsUrl = Uri.TryCreate(sPDFFile, UriKind.Absolute, out uriResult);
                if (bIsUrl)
                {
                    sPDFFile = Path.Combine(sOutputDirectory, oDocument.PDFFileName);
                    if (oConvertPDFDocumentInput.AWSFileUpload != null)
                        oDCS3Services.downloadFileByURL(oCreateDocumentXMLInput.InputFileName, sPDFFile);

                }

                if (!File.Exists(sPDFFile))
                {
                    oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.DoesNotExist;
                    throw new Exception("Error when uploading " + oDocument.Id);
                }
                else
                {
                    oDocumentConvertor.init(sPDFFile);
                    if (oDocumentConvertor.IsEncrypted())
                    {
                        oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.Encrypted;
                        throw new Exception("The uploaded document is encrypted " + oDocument.Id);
                    }

                    if (oPublisher.ConvertSettings != null && oPublisher.ConvertSettings.CompressPDF)
                    {
                        //DCCore.Logger.log.Debug("Compressing PDF");

                        FileInfo fi = new FileInfo(sPDFFile);
                        string sCompressName = fi.Name.Replace(".pdf", "");
                        sCompressName = sCompressName + "_comp" + ".pdf";
                        string sCompressFileFullName = Path.Combine(fi.DirectoryName, sCompressName);
                        BMPDFDoc.Init();
                        bool bRest = BMPDFDoc.executeCompressPDF(oJob, sPDFFile, sCompressFileFullName, oPublisher.ConvertSettings);
                        if (bRest)
                        {
                            //DCCore.Logger.log.Debug("Compressing PDF done");
                            oDocument.PDFForDownloadFile = sCompressName;
                            if ((oPublisher.ConvertSettings.CompressPDFOptions & (int)Constants.CompressPDFOptions.ConvertUsingCompressedFile) > 1)
                            {
                                oDocumentConvertor.release();
                                //We might want to delete the origitan file.
                                File.Delete(sPDFFile);
                                oDocument.PDFFileName = sCompressName;
                                sPDFFile = Path.Combine(sOutputDirectory, oDocument.PDFFileName);

                                oDocumentConvertor.init(sPDFFile);
                            }
                        }
                        else
                        {
                            //DCCore.Logger.log.Debug("Compressing PDF error");
                        }
                    }

                    //oCreateDocumentXMLInput.InputFileName = sPDFFile; // Temperary fix
                    oCreateDocumentXMLInput.OutputDirectory = sOutputDirectory;
                    oDocumentConvertor.createDocumentXML(sPDFFile, oCreateDocumentXMLInput, bUseVectorText, out docpagesizewarning);
                    if (docpagesizewarning)
                        oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.DifferentPageSize;
                    oDocumentConvertor.addFonts(sPDFFile, sOutputDirectory);


                }

                bool bUseMuTools = false;
                bUseMuTools = context.serversettings.FirstOrDefault(x => x.Name == "ImageConvertorType").Value != "0";
                if (oPublisher?.ConvertSettings?.ImageConvertorType > -1)
                {
                    if (oPublisher.ConvertSettings.ImageConvertorType == 1)
                        bUseMuTools = true;
                    if (oPublisher.ConvertSettings.ImageConvertorType == 0)
                        bUseMuTools = false;
                }


                oDocument.NumberOfPages = oDocumentConvertor.getNumberOfPages();
                var listCreateImagesInput = await context.createimagesinput
                                                 .Include(c => c.Job)
                                                .Where(c => c.Job.Id == oJob.Id)
                                                .ToListAsync();
                bool res = false;
                foreach (createimagesinput oCreateImagesInput in listCreateImagesInput)
                {
                    oDocument.DocumentProcessingDescription = oCreateImagesInput.Description;
                    oDocument.DocumentProgressingPercent = 25;
                    oCreateImagesInput.OutputDirectory = sOutputDirectory;
                    if (bUseMuTools)
                        res = oDocumentConvertor.createImagesEx(oCreateImagesInput, oDocument.NumberOfPages);
                    else
                    {
                        /*
                        res = oDocumentConvertor.createImages(oCreateImagesInput);
                        if (!res)
                        {
                            bUseMuTools = true;
                            oDocumentConvertor.createImagesEx(oCreateImagesInput);
                        }
                        */
                    }
                }

                var oCreatePagesXMLInput = await context.createpagesxmlinput
                                    .Include(c => c.Job)
                                    .Where(c => c.Job.Id == oJob.Id)
                                    .FirstOrDefaultAsync();
                if (oCreatePagesXMLInput != null)
                {
                    oDocument.DocumentProcessingDescription = oCreatePagesXMLInput.Description;
                    oDocument.DocumentProgressingPercent = 50;
                    oCreatePagesXMLInput.InputFileName = sPDFFile; // Temperary fix
                    oDocumentConvertor.createPagesXML(sPDFFile, oCreatePagesXMLInput, sOutputDirectory, oDocument.NumberOfPages);
                }


                var oPDF2HTMLInput = await context.pdf2htmlinput
                                           .Include(c => c.Job)
                                           .Where(c => c.Job.Id == oJob.Id)
                                           .FirstOrDefaultAsync();
                if (oPDF2HTMLInput != null)
                {
                    oDocument.DocumentProcessingDescription = oPDF2HTMLInput.Description;
                    oDocument.DocumentProgressingPercent = 60;
                    //This step also import links from the PDF.

                    bool bTextExtractor = context.serversettings.FirstOrDefault(x => x.Name == "TextExtractor").Value != "0";
                    bool bres;
                    if (bTextExtractor)
                    {
                        bres = PDF2HTML.convertAllPagesCommandLine(sPDFFile, sOutputDirectory, oPDF2HTMLInput.DirectoryName);
                        if (!bres)
                        {
                            PDF2HTML.convertAllPagesMuPDF(sPDFFile, sOutputDirectory, oPDF2HTMLInput.DirectoryName);
                        }
                    }
                    else
                    {
                        bres = PDF2HTML.convertAllPagesMuPDF(sPDFFile, sOutputDirectory, oPDF2HTMLInput.DirectoryName);
                        if (!bres)
                        {
                            PDF2HTML.convertAllPagesCommandLine(sPDFFile, sOutputDirectory, oPDF2HTMLInput.DirectoryName);
                        }

                    }

                    generateSelectableText(oDocument, out bDocumentContainText);
                    if (!bDocumentContainText)
                        oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.Scanned;

                    context.Update(oDocument);
                    context.SaveChanges();


                    var oGifInput = await context.creategifflippingbookinput
                                          .Include(c => c.Job)
                                          .Where(c => c.Job.Id == oJob.Id)
                                          .FirstOrDefaultAsync();
                    if (oGifInput != null)
                    {
                        oDocument.DocumentProcessingDescription = oGifInput.Description;
                        oDocument.DocumentProgressingPercent = 70;
                        int nNumberOfPages = Math.Min(6, oDocument.NumberOfPages);
                        GifFlippingBookCreator oGifFlippingBookCreator = new GifFlippingBookCreator();
                        oGifFlippingBookCreator.CreateGifFlippingBook(oGifInput.Width, oDocumentConvertor.FirstThumbHeight, oGifInput.Ratio, oGifInput.BackGroundColor, oGifInput.FlipInterval, oGifInput.Prefix, oGifInput.Postfix, nNumberOfPages, oGifInput.InputDirectory, oGifInput.OutputDirectory, oGifInput.GifFileName);
                    }


                    var oPdf2SVGInput = await context.pdf2svginput
                                          .Include(c => c.Job)
                                          .Where(c => c.Job.Id == oJob.Id)
                                          .FirstOrDefaultAsync();
                  
                    if (oPdf2SVGInput != null && oPdf2SVGInput.GenerateSVG)
                    {
                        PDFRemoveLayers oPdfRemoveLayers = new PDFRemoveLayers();
                        oPdfRemoveLayers.m_nTimeout = 50000;
                        oPdfRemoveLayers.PDFSourceFileName = sPDFFile;
                        string sNoImagesPDF = string.Format("{0}\\{1}", sOutputDirectory, "Full_No_Images.pdf");
                        oPdfRemoveLayers.PDFTargetFileName = sNoImagesPDF;
                        oPdfRemoveLayers.LayerFlag = "T";
                        oPdfRemoveLayers.Execute();
                        oPdfRemoveLayers.NumberOfPages = oDocument.NumberOfPages;
                        oPdfRemoveLayers.convertAllPages(sOutputDirectory);
                        oPdfRemoveLayers.deleteOutput();
                        oPdfRemoveLayers.deleteOutput();
                    }
                    document oSourceDocument = oDocument.Publication.PublicationTemplate.introtemplatedoc;
                    if (oSourceDocument != null)
                    {
                        string sSourceDocumentId = oSourceDocument.Id.ToString();
                        copyIntro(context,sSourceDocumentId, oDocument.Id.ToString());
                    }

                    //DCCore.Logger.log.Debug("Generating S3 start  " + oDocument.Id);
                    oDocumentConvertor.generateS3Files(context, oDocument.Id.ToString());


                    bool bEnableAI = (oDocument.Publication.Publisher.ExtraOptions & Convert.ToInt32(Constants.PublisherExtraOptions.EnableAI)) != 0;
                    if (bDocumentContainText && bEnableAI)
                    {
                        AIEmbeddingService oAIEmbeddingService = new AIEmbeddingService();
                        await oAIEmbeddingService.DeleteEmbeddingForDocumentAsync(context, oDocument.Id,ct);
                        await oAIEmbeddingService.addDocumentToAIAsync(context, oDocument,ct);
                    }


                    var oImportbookmarksinput = await context.importbookmarksinput
                                        .Include(c => c.Job)
                                        .Where(c => c.Job.Id == oJob.Id)
                                        .FirstOrDefaultAsync();
                    if (oImportbookmarksinput != null)
                    {
                        //Adding bookmark as a job so we can add it also from multimedia section.
                        oImportbookmarksinput.OutputDirectory = sOutputDirectory;
                        JobExecutionCreateBookmarks oJobExecutionCreateBookmarks = new JobExecutionCreateBookmarks(context);
                        oImportbookmarksinput.InputFileName = sPDFFile;
                        oJobExecutionCreateBookmarks.createBookmarks(oImportbookmarksinput);
                        JobExecutionCreateBookmarks.UpdateTOCType(sOutputDirectory);
                    }


                    var oImportNotesInput = await context.importnotesinput
                                       .Include(c => c.Job)
                                       .Where(c => c.Job.Id == oJob.Id)
                                       .FirstOrDefaultAsync();
                    if (oImportNotesInput != null)
                    {
                        JobExecutionImportNotes oJobExecutionImportNotes = new JobExecutionImportNotes(context);
                        oImportNotesInput.OutputDirectory = sOutputDirectory;
                        oImportNotesInput.InputFileName = sPDFFile;
                        oJobExecutionImportNotes.importNotes(oImportNotesInput);
                    }


                    var oRecognizelinksinput = await context.recognizelinksinput
                                      .Include(c => c.Job)
                                      .Include(c=>c.Expresions)
                                      .Where(c => c.Job.Id == oJob.Id)
                                      .FirstOrDefaultAsync();
                    if (oRecognizelinksinput != null)
                    {
                        JobExecutionRecognizeLinks oJobExecutionRecognizeLinks = new JobExecutionRecognizeLinks(context);
                        oJobExecutionRecognizeLinks.recognizeLinksFromLocal(oRecognizelinksinput);
                    }

                    if (oDocument.NumberOfPages > 200 && (oDocument.Publication.Publisher.Licenses[0].LicenseType == Constants.ProductType.Trial.ToString() || oDocument.Publication.Publisher.Licenses[0].LicenseType == Constants.ProductType.Select.ToString()))
                    {
                        oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.TrialOver200Pages;
                        oDocument.IsActive = false;
                    }

                    if (oDocument.NumberOfPages > 500 && (oDocument.Publication.Publisher.Licenses[0].LicenseType == Constants.ProductType.Professional.ToString() || oDocument.Publication.Publisher.Licenses[0].LicenseType == Constants.ProductType.Elite.ToString()))
                    {
                        oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.TrialOver500Pages;
                        oDocument.IsActive = false;
                    }

                    if (oPublisher.ConvertSettings?.AddProductsLinksFromDB == true)
                    {
                        SearchProductsInDocument(context,oDocument);
                    }

                    var oSaveLinksTopdfInput = await context.savelinkstopdfinput
                                     .Include(c => c.Job)
                                     .Where(c => c.Job.Id == oJob.Id)
                                     .FirstOrDefaultAsync();

                    if(oSaveLinksTopdfInput != null)
                    {
                        string sPDFWithLinksFileName = oDocument.PDFFileName.Replace(".pdf", "_Links.pdf");
                        string sFullPDFFileName = Path.Combine(sOutputDirectory, oDocument.PDFFileName);
                        string sFullTempFileName = Path.Combine(sOutputDirectory, sPDFWithLinksFileName);
                        bool bSaveLinks = JobExecutionSaveLinksToPDF.saveLinksToPDF(oDocument, sFullTempFileName);

                    }







                    _log.LogDebug("Generating S3 end  " + oDocument.Id);

                    _log.LogDebug("Uploading to S3 start  " + oDocument.Id);

                    sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
                    string sPublicationPath = PublicationUtil.getPublicationPath(oDocument.Publication, DCCommon.Instance.RepositoryLocation);
                    string sDefaultBucketName = Constants.DEFAULT_DOCS_LOCATION;

                    string sBucketName = string.IsNullOrEmpty(oDocument.Publication.Publisher.BucketName) ? sDefaultBucketName : oDocument.Publication.Publisher.BucketName;
                    string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
                    string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
                    string sKeyPrefix = string.Format("/{0}/{1}/{2}", sPublisherName, sPublicationName,
                        oDocument.Id);
                    oDCS3Services.uploadDirectory(sBucketName, sOutputDirectory, sKeyPrefix);


                    //Upload settings and preloader.
                    string sPublicationPrefix = string.Format("{0}/{1}", sPublisherName, sPublicationName);
                    string sSettingsURL = string.Format("{0}/{1}", sPublicationPrefix, "settings.json");
                    bool bRet = await oDCS3Services.fileExistsAsync(sBucketName, sSettingsURL);
                    string sSettingFilePath = Path.Combine(sPublicationPath, "settings.json");
                    if (!bRet)
                        oDCS3Services.uploadFile(sBucketName, sSettingFilePath, sPublicationPrefix);

                    string sPreloaderURL = string.Format("{0}/{1}", sPublicationPrefix, "preloader.json");
                    bRet = await oDCS3Services.fileExistsAsync(sBucketName, sPreloaderURL);
                    string sPreloaderFilePath = Path.Combine(sPublicationPath, "preloader.json");
                    if (!bRet)
                        oDCS3Services.uploadFile(sBucketName, sPreloaderFilePath, sPublicationPrefix);

                    //DCCore.Logger.log.Debug("Uploading to S3 end  " + oDocument.Id);
                    //if(sPublicationName != ""){
                    //    Directory.Delete(sOutputDirectory, true);
                    //}

                    oDocument.DocumentStatus = Constants.JobProcessingStatus.Completed.ToString();
                    oDocument.DocumentProcessingDescription = "Finished Successfully";
                    oDocument.DocumentProgressingPercent = 100;
                    if (oDocument.IsActive)
                    {
                        oDocument.Publication.Publisher.Licenses[0].Used++;
                    }
                    oDocument.TemplateFolderName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
                    oDocument.PublisherFolderName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);


                    DownloadAllPDFFlag oDownloadAllPDFFlag = await context.DownloadAllPDFFlag
                                        .Include(c => c.Publication)
                                        .Where(c => c.Publication.Id== oDocument.Publication.Id)
                                        .FirstOrDefaultAsync();

                    
                    if (oDownloadAllPDFFlag != null)
                    {
                        oDownloadAllPDFFlag.generateAllPDF = true;
                        context.Update(oDownloadAllPDFFlag);
                    }

                    oDocumentConvertor.release();
                    //Calculate CRC32
                    
                    uint hash = 0;
                    byte[] data = File.ReadAllBytes(sPDFFile);
                    uint crc = Crc32Algorithm.Compute(data);              // numeric CRC-32
                    Console.WriteLine("CRC-32 is {0}", hash);
                    oDocument.crc32 = crc;

                    DocumentConvertor.indexDocument(context, oDocument);
                    oPostConversionCommunication.notifyAdminPanel(oDocument);



                    context.SaveChanges();

                 


                }
              


            }
            return true;
        }
    }
}
