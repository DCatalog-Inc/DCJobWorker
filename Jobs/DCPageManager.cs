#nullable disable
using Amazon.Extensions.NETCore.Setup;
using Amazon.SQS;
using Amazon.Util.Internal;
using Core;
using Core.Common;
using Core.Models;
using Core.Services;
using DCatalogAdmin;
using DCatalogCommon;
using DCJobs;
using DCatalogCommon.Data;
using DCCore;
using GifFlippingBook;
using Hangfire;
//using iText.Forms.Form.Element;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections;
using System.IO.Hashing;
using System.Net;
using System.Text;
using System.Xml;


namespace JobWorker.Jobs
{
    public class DCPageManager
    {
        public ApplicationDbContext _context;
        private readonly ISimpleEmailSender _emailSender;
        private readonly ILogger _logger;
        private readonly IAmazonSQS _sqsclient;
        public DCPageManager(
           ApplicationDbContext context,
           ISimpleEmailSender emailSender,
           ILogger<ProductImportJob> logger,
            IAmazonSQS amazonSQS
           )
        {
            _context = context;
            _emailSender = emailSender;
            _logger = logger;
            _sqsclient = amazonSQS;
        }


        public void updatePageSize(document oDocument, int nPageNumber, string sWidth, string sHeight)
        {

            DCS3Services oDCS3Services = new DCS3Services();
            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                oDocument.Id);
            string docname = "document.json";
            string docfilepath = Path.Combine(sOutputDirectory, docname);
            oDCS3Services.downloadFile(sBucketName, sKeyPrefix + "/" + docname, docfilepath);
            JObject docJson = JObject.Parse(System.IO.File.ReadAllText(docfilepath));
            string selectsequence = "$..page[?(@..sequence=='" + nPageNumber.ToString() + "')]";
            JToken jPage = docJson.SelectToken(selectsequence);
            JValue width = (JValue)jPage["@attributes"]["width"];
            width.Value = sWidth;
            JValue height = (JValue)jPage["@attributes"]["height"];
            height.Value = sHeight;

            System.IO.File.WriteAllText(docfilepath, docJson.ToString());
            oDCS3Services.uploadFile(Constants.DEFAULT_DOCS_LOCATION, docfilepath, sKeyPrefix);

        }



        protected async Task<bool> ReplacePages(replacepageinput oReplacePageInput)
        {
            string sBucketName = oReplacePageInput.bucketname;
            job oJob = oReplacePageInput.Job;
            _logger.LogDebug("Processing Job with the ID for replace pages" + oJob.Id);
            

            oJob.Progress = 10;
            oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
            _context.Update(oJob);

            string sFullPDFFileName = "";
            string sFullPDFFileUrl = "";  //New File that we added
            string sOutputDirectory = "";
            bool bReplacePDFForDownload = false;
            DCJobs.DocumentConvertor oDocumentConvertor = new DCJobs.DocumentConvertor(_logger);
            DCS3Services oDCS3Services = new DCS3Services();
            //Create the document repository files
            document oDocument = null;
            string sSourceBucket = Constants.DEFAULT_DOCS_LOCATION;
            var crc32 = new System.IO.Hashing.Crc32();
            uint hash = 0;
            string sNewFileNameUploaded = Guid.NewGuid().ToString() + ".pdf";
            //bool bUsingCopy = false;
            //string sNewFileNameForCompleteFile = "";
            try
            {
                if (oReplacePageInput != null)
                {

                    oDocument = oReplacePageInput.Document;
                    oDocument.DocumentProcessedBy = "DCJobWorker@" + Environment.MachineName;
                    sFullPDFFileName = oReplacePageInput.InputFileName;
                    // input.OutputDirectory is computed by the ADMIN from the shared
                    // serversettings RepositoryLocation (D:\DCatalog\Docs — the legacy
                    // DocProcessor layout). This box has no D: drive; stage everything
                    // under the worker's own repository path instead.
                    sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
                    bReplacePDFForDownload = oReplacePageInput.replacedownloadPDF;
                    sBucketName = oReplacePageInput.bucketname;
                    sFullPDFFileUrl = oReplacePageInput.PDFFileUrl;
                }

                string sDocumentRelativeURL = DocumentUtilBase.getDocumentRelativeURL(oDocument);
                //XmlDocument oDocXML = DocumentUtil.getAsXML(oDocument);
                string sExistingFile = Path.Combine(sOutputDirectory, oDocument.PDFFileName);
                //sNewFileNameForCompleteFile = Path.Combine(sOutputDirectory, Guid.NewGuid().ToString() + ".pdf");

                if (oDocument.Publication == null)
                    _context.Entry(oDocument).Reference(d => d.Publication).Load();

                if (oDocument.Publication != null && oDocument.Publication.Publisher == null)
                    _context.Entry(oDocument.Publication).Reference(p => p.Publisher).Load();

                if (oDocument.Publication != null && oDocument.Publication.PublicationTemplate == null)
                    _context.Entry(oDocument.Publication).Reference(p => p.PublicationTemplate).Load();

                if (!Directory.Exists(sOutputDirectory))
                {
                    _logger.LogDebug("Directory does not exists... creating directory: " + sOutputDirectory);
                    Directory.CreateDirectory(sOutputDirectory);
                }
                else
                {
                    _logger.LogDebug("Directory exists " + sOutputDirectory);
                }



                if (sFullPDFFileUrl != "")
                {
                    // input.InputFileName points at the ADMIN's upload location — also not
                    // valid on this box. The uploaded page PDF is fetched from S3 anyway,
                    // so stage it in the local document directory (created above).
                    string sDownloadDir = sOutputDirectory;
                    if (!Directory.Exists(sDownloadDir))
                    {
                        _logger.LogDebug("Directory does not exists... creating directory: " + sDownloadDir);
                        Directory.CreateDirectory(sDownloadDir);
                    }
                    else
                    {
                        _logger.LogDebug("Directory exists " + sDownloadDir);
                    }
                    sNewFileNameUploaded = Path.Combine(sDownloadDir, sNewFileNameUploaded);

                    if (!string.IsNullOrEmpty(sBucketName))
                    {
                        _logger.LogDebug("Downloading file : " + sBucketName + sFullPDFFileUrl + sNewFileNameUploaded);
                        oDCS3Services.downloadFile(sBucketName, sFullPDFFileUrl, sNewFileNameUploaded);
                    }
                    else
                    {
                        /*
                        using (WebClient webClient = new WebClient())
                        {
                            _logger.LogDebug("Downloading file from " + sFullPDFFileUrl + " " + " to " + sNewFileNameUploaded);
                            webClient.DownloadFile(sFullPDFFileUrl, sNewFileNameUploaded);
                        }
                        */
                        PostSubmitter postClient = new();
                        _logger.LogDebug("Downloading file from " + sFullPDFFileUrl + " " + " to " + sNewFileNameUploaded);
                        await postClient.DownloadFileAsync(sFullPDFFileUrl, sNewFileNameUploaded);
                    }
                }


                if (bReplacePDFForDownload)
                {


                    if (File.Exists(sNewFileNameUploaded))
                    {
                        FileInfo oFileInfo = new FileInfo(sNewFileNameUploaded);
                        //oDocXML.DocumentElement.Attributes["filename"].Value = oFileInfo.Name;
                        //oDocXML.DocumentElement.Attributes["filesize"].Value = oFileInfo.Length.ToString();
                        string sNewPDFFile = Path.Combine(sOutputDirectory, oFileInfo.Name);
                        File.Copy(sNewFileNameUploaded, sNewPDFFile, true);
                        oDocument.PDFFileName = oFileInfo.Name;
                        oDCS3Services.uploadFile(Constants.DEFAULT_DOCS_LOCATION, sNewFileNameUploaded, sDocumentRelativeURL);
                    }

                }
                else
                {
                    string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
                    bool bUseUrlAsProductId = false;
                    if (sPublisherName == "Marine-Warehouse")
                        bUseUrlAsProductId = true;


                    string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
                    string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                        oDocument.Id);

                    string sExistingPDFKeyName = sKeyPrefix + "/" + oDocument.PDFFileName;
                    bool CRCMatch = false;
                    if (File.Exists(sExistingFile) && oDocument.crc32 != 0)
                    {
                        try
                        {
                            hash = DCPageManager.GetCrc32(sExistingFile);
                            if (oDocument.crc32 == hash && hash !=0 )
                                CRCMatch = true;
                        }
                        catch (Exception)
                        {
                            //The file is being used — work on a copy instead.
                            string sNewFileName = sExistingFile.Replace(".pdf", Guid.NewGuid().ToString() + ".pdf");
                            File.Copy(sExistingFile, sNewFileName, true);
                            sExistingFile = sNewFileName;
                            hash = DCPageManager.GetCrc32(sExistingFile);
                            if (oDocument.crc32 == hash && hash != 0)
                                CRCMatch = true;
                            // (legacy fs.Close() removed — fs was never assigned here; GetCrc32 is
                            // self-contained, and the null call killed the job on every file lock)

                        }
                        //Check the file CRC32

                    }
                    if (!CRCMatch)
                        oDCS3Services.downloadFile(sSourceBucket, sExistingPDFKeyName, sExistingFile);

                    //Must be removed!!!!
                    //File.Copy(sNewFileNameUploaded, sExistingFile, true);

                    bool bRet = oDocumentConvertor.init(sNewFileNameUploaded);

                    if (oDocumentConvertor.IsEncrypted())
                    {
                        oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.Encrypted;
                        throw new Exception("The uploaded document is encrypted " + oDocument.Id);
                    }
                    if(bRet == false)
                    {
                        oDocument.DocumentProcessingWarning = (int)Constants.DocumentProcessingWarning.Damaged;
                        throw new Exception("The uploaded document is Damaged " + oDocument.Id);
                    }

                    int nNumberOfPagesUploaded = oDocumentConvertor.getNumberOfPages();
                    bool bUploadedCompletePDF = nNumberOfPagesUploaded == oDocument.NumberOfPages;

                    int nImageQuality = 85;
                    int nResolution = System.Convert.ToInt32(Math.Floor(oDocument.NormalPageResolution));

                    bool bUseMuTools = false;
                    bUseMuTools = _context.serversettings.FirstOrDefault(x => x.Name == "ImageConvertorType").Value != "0";

                    DCJobs.DocumentConvertor oDocumentConvertor2 = new DCJobs.DocumentConvertor(_logger);
                    oDocumentConvertor2.init(sExistingFile);
                    int nTotalNumberOfPages = oDocumentConvertor2.getNumberOfPages();
                    oDocumentConvertor2.release();
                    // &(int)Constants.CompressPDFOptions.ConvertUsingCompressedFile) >1
                    int nNumberOfPagesToReplace = 1;
                    //if ((oDocument.Publication.Publisher.ConvertSettings.CompressPDFOptions &(int)Constants.GeneralConverFlags.REPLACEONLYSELECTEDPAGE) >1)
                    if (bUploadedCompletePDF)
                    {
                        if (!DocumentConvertor.replacePageInPDFHD(oReplacePageInput.pagenumber, sExistingFile, sNewFileNameUploaded))
                            throw new Exception("Replacing page " + oReplacePageInput.pagenumber + " in the PDF failed (cpdf/HD path) — see worker log");
                        nNumberOfPagesToReplace = 1;
                    }
                    else
                    {
                        nNumberOfPagesToReplace = nNumberOfPagesUploaded;
                        if (!DocumentConvertor.replacePageInPDFEx(oReplacePageInput.pagenumber, sExistingFile, sNewFileNameUploaded))
                            throw new Exception("Replacing page " + oReplacePageInput.pagenumber + " in the PDF failed (cpdf) — see worker log");
                    }
                    //End new code.
                    //Also generate the thumbnails
                    for (int i = 0; i < nNumberOfPagesToReplace; i++)
                    {
                        createimagesinput oCreateHiResImagesInput = oDocumentConvertor.createImageInput(nResolution, -1, -1, sNewFileNameUploaded, sOutputDirectory, "ZPage_", nImageQuality, oJob, "Creating High Resolution Images");
                        oCreateHiResImagesInput.Resolution = System.Convert.ToInt32(oDocument.HiPageResolution);
                        int nPageWidth, nPageHeight;
                        if (oReplacePageInput.pagenumber + i > oDocument.NumberOfPages)
                            continue;
                        //We need to use the first page if we have multiple pages.
                        if (bUploadedCompletePDF) //HD Change
                            oDocumentConvertor.createImageReplace(oCreateHiResImagesInput, oReplacePageInput.pagenumber + i, oReplacePageInput.pagenumber + i, out nPageWidth, out nPageHeight);
                        else
                            oDocumentConvertor.createImageReplace(oCreateHiResImagesInput, 1 + i, oReplacePageInput.pagenumber + i, out nPageWidth, out nPageHeight);
                        updatePageSize(oDocument, oReplacePageInput.pagenumber + i, nPageWidth.ToString(), nPageHeight.ToString());




                        oDocument.DocumentProgressingPercent = oJob.Progress = 20;
                        oDocument.DocumentProcessingDescription = oJob.Desctiption = "Generating Images";
                        oJob.Status = Constants.JobProcessingStatus.Processing.ToString();

                        _context.Update(oJob);
                        _context.Update(oDocument);

                        if (oDocument.Publication.PublicationTemplate.usesvghtml5)
                        {
                            PDFRemoveLayers oPdfRemoveLayers = new PDFRemoveLayers();
                            oPdfRemoveLayers.m_nTimeout = 50000;
                            oPdfRemoveLayers.PDFSourceFileName = sNewFileNameUploaded;
                            string sNoImagesPDF = string.Format("{0}\\{1}", sOutputDirectory, "Full_No_Images.pdf");
                            oPdfRemoveLayers.PDFTargetFileName = sNoImagesPDF;
                            oPdfRemoveLayers.LayerFlag = "T";
                            if (i == 0)
                                oPdfRemoveLayers.Execute();
                            oPdfRemoveLayers.NumberOfPages = 1;
                            oPdfRemoveLayers.convertPage(sOutputDirectory, oReplacePageInput.pagenumber + i);
                            if (nNumberOfPagesToReplace == i + 1)
                                oPdfRemoveLayers.deleteOutput();
                        }




                        //string sNewPDFFile = Path.Combine(sOutputDirectory, sFullPDFFileName);

                        //oDocumentConvertor.replacePageInPDF(oReplacePageInput.pagenumber + i, sExistingFile, sNewFileNameUploaded);
                        //oDocumentConvertor.release();

                        string sThumbnailName = string.Format("{0}{1}.jpg", "Thumbnail_", oReplacePageInput.pagenumber + i);
                        string sPageName = string.Format("{0}{1}.jpg", "ZPage_", oReplacePageInput.pagenumber + i);
                        string sSVGPageName = string.Format("{0}{1}.svg", "SPage_", oReplacePageInput.pagenumber + i);

                        string sPageXML = string.Format("{0}{1}.xml", "Page_", oReplacePageInput.pagenumber + i);


                        //string sSWFPageName = string.Format("{0}{1}.swf", "ZPage_", oReplacePageInput.pagenumber);

                        string sThumbnailFullName = Path.Combine(sOutputDirectory, sThumbnailName);
                        string sPageFullName = Path.Combine(sOutputDirectory, sPageName);

                        string sSVgFullName = Path.Combine(sOutputDirectory, sSVGPageName);


                        oDCS3Services.uploadFile(sSourceBucket, sThumbnailFullName, sDocumentRelativeURL);
                        oDCS3Services.uploadFile(sSourceBucket, sPageFullName, sDocumentRelativeURL);
                        if (oDocument.Publication.PublicationTemplate.usesvghtml5)
                            oDCS3Services.uploadFile(sSourceBucket, sSVgFullName, sDocumentRelativeURL);
                        try
                        {
                            hash = DCPageManager.GetCrc32(sExistingFile);
                        }
                        catch (Exception)
                        {
                            string sNewFileName = sExistingFile.Replace(".pdf", Guid.NewGuid().ToString() + ".pdf");
                            File.Copy(sExistingFile, sNewFileName, true);
                            sExistingFile = sNewFileName;
                            hash = DCPageManager.GetCrc32(sExistingFile);
                            if (oDocument.crc32 == hash)
                                CRCMatch = true;
                            // (legacy fs.Close() removed — fs was never assigned here)
                        }
                        oDocument.crc32 = hash;
                        oDocument.DocumentProgressingPercent = oJob.Progress = 50;
                        oDocument.DocumentProcessingDescription = oJob.Desctiption = "Updating images";
                        oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
                        _context.Update(oJob);
                        _context.Update(oDocument);

                        //Todo upload all the other files.
                        if (oReplacePageInput.updatelinks)
                        {
                            oDocumentConvertor2 = new DCJobs.DocumentConvertor(_logger);
                            oDocumentConvertor2.init(sExistingFile);
                            oDocument.DocumentProcessingDescription = "Updating Page With Links";
                            try
                            {

                                string sXMLPageName = string.Format("Page_{0}.xml", oReplacePageInput.pagenumber + i);

                                string sExistingPageXml = sKeyPrefix + "/" + sPageXML;
                                string xXMLPageFullName = Path.Combine(sOutputDirectory, sXMLPageName);


                                oDCS3Services.downloadFile(sSourceBucket, sExistingPageXml, xXMLPageFullName);

                                createpagesxmlinput oCreatePagesXmlInput = new createpagesxmlinput();
                                oCreatePagesXmlInput.ImportLinks = true;
                                if (bUploadedCompletePDF) //HD Change
                                    oCreatePagesXmlInput.InputFileName = sNewFileNameUploaded;
                                else
                                    oCreatePagesXmlInput.InputFileName = sExistingFile;
                                oCreatePagesXmlInput.OutputDirectory = sOutputDirectory;
                                oDocumentConvertor2.createPageXMLReplacePage(oCreatePagesXmlInput, oReplacePageInput.pagenumber + i);





                                // No need for XML File.
                                //oDCS3Services.uploadFile(sSourceBucket, sXMLPageName, sDocumentRelativeURL);

                                string sJsonPage_Name = string.Format("Page_{0}.json", oReplacePageInput.pagenumber + i);
                                publicationtemplate oPublicationTemplate = oDocument.Publication.PublicationTemplate;
                                XmlDocument oXMLResult = new XmlDocument();
                                oXMLResult.Load(Path.Combine(sOutputDirectory, sXMLPageName));
                                string sPageResult = DocumentConvertor.getPageJson(oDocument, oXMLResult, oPublicationTemplate, bUseUrlAsProductId);
                                string sJsonPagePath = Path.Combine(sOutputDirectory, sJsonPage_Name);
                                System.IO.File.WriteAllText(sJsonPagePath, sPageResult);
                                oDCS3Services.uploadFile(sSourceBucket, sJsonPagePath, sDocumentRelativeURL);
                                oDocumentConvertor2.release();
                            }
                            catch (Exception ex)
                            {
                                oJob.Desctiption = oDocument.DocumentProcessingDescription = ex.StackTrace;
                                oJob.Status = oDocument.DocumentStatus = Constants.JobProcessingStatus.Failed.ToString();
                                oDocument.DocumentProgressingPercent = 100;
                                
                                _context.Update(oJob);
                                _context.Update(oDocument);
                                _context.SaveChanges();
                                return false;

                            }
                        }
                        string sDocumentID = oDocument.Id.ToString();

                        //string sDocJsonFilePath = oDocumentConvertor.saveDocumentJsonFile(sDocumentID);
                        await DocumentConvertor.downloadDocumentFile(oDocument, sOutputDirectory, "document.json");
                        string sDocJsonFilePath = DocumentConvertor.updatedocjsonversion(oDocument, oReplacePageInput.pagenumber + i);
                        //oDCS3Services.uploadFile(DCCore.Constants.DEFAULT_DOC_BUCKET, sDocumentXMLFile, sDocumentRelativeURL);
                        oDCS3Services.uploadFile(Constants.DEFAULT_DOCS_LOCATION, sDocJsonFilePath, sDocumentRelativeURL);
                        PDF2HTML.convertPagesCommandLine(sExistingFile, sOutputDirectory, "html", oReplacePageInput.pagenumber + i);
                        indexPageInDocument(oDocument, oReplacePageInput.pagenumber + i);
                    }

                }

                oDCS3Services.uploadFile(sSourceBucket, sExistingFile, sDocumentRelativeURL, oDocument.PDFFileName);
                oDocument.DocumentProgressingPercent = oJob.Progress = 70;
                oDocument.DocumentProcessingDescription = oJob.Desctiption = "Indexing edition";
                oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
                _context.Update(oJob);
                _context.Update(oDocument);
                _context.SaveChanges();



                oDocument.DocumentProgressingPercent = oJob.Progress = 100;
                oDocument.DocumentStatus = oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                oJob.Desctiption = "Finished Successfully";
                oDocument.DocumentProcessingDescription = "Finished Successfully";
                _context.Update(oJob);
                _context.Update(oDocument);
                _context.SaveChanges();

                /*
                 _pub = _context.publication
                    .Include(p => p.Publisher)
                    .Include(p => p.PublicationTemplate)
                    // .Include(p => p.RemoteAuthentication)
                    .Where(p => string.Equals(p.Publisher.Name, sPublisherFromTheDomain, StringComparison.CurrentCultureIgnoreCase))
                    .Where(p => string.Equals(p.Name, sPublicationName, StringComparison.CurrentCultureIgnoreCase))
                    .FirstOrDefault();

                */
                DownloadAllPDFFlag oDownloadAllPDFFlag = _context.DownloadAllPDFFlag
                    .Where(p => p.Publication.Id == oDocument.Publication.Id)
                    .FirstOrDefault();
                /*
                DownloadAllPDFFlag oDownloadAllPDFFlag = session.CreateCriteria<DownloadAllPDFFlag>()
                .CreateCriteria("Publication")
                .Add(Restrictions.Eq("Id", oDocument.Publication.Id))
                .UniqueResult<DownloadAllPDFFlag>();
                if (oDownloadAllPDFFlag != null)
                {
                    oDownloadAllPDFFlag.generateAllPDF = true;
                    session.SaveOrUpdate(oDownloadAllPDFFlag);
                    session.Flush();

                }
                oDocumentConvertor.release();
                if (bUsingCopy)
                    File.Delete(sExistingFile);
                */
                //UpdateDownloadAllPDF oUpdateDownloadAllPDF = new UpdateDownloadAllPDF();
                //oUpdateDownloadAllPDF.generateDownloadPDF(oDocument.Publication);

            }
            catch (Exception e)
            {
                //BIG NUG WE HAVE ISSUE AND WE UPDATING THE NUMBER OF PAGES!!!!!!!!!!
                oDocument.DocumentProgressingPercent = oJob.Progress = 100;
                oDocument.DocumentStatus = Constants.JobProcessingStatus.Completed.ToString();
                oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                oJob.Desctiption = oDocument.DocumentProcessingDescription = e.Message;
                _context.Update(oJob);
                _context.Update(oDocument);
                _context.SaveChanges();

                _logger.LogError(e, "ReplacePages failed for job {JobId}", oJob?.Id);
                // _logger may be a NullLogger depending on the caller — stdout is always captured.
                Console.WriteLine("ReplacePages failed for job " + oJob?.Id + ": " + e);

            }



            return true;

        }

    

        public void indexPageInDocument(document oDocument, int nPageNumber)
        {
            string sIndexName = _context.serversettings.FirstOrDefault(x => x.Name == "esdefaultindex")?.Value;
            if (!string.IsNullOrEmpty(oDocument?.Publication?.searchindexname))
                sIndexName = oDocument.Publication.searchindexname;


            //AWSOptions awsOptions = _configuration.GetAWSOptions();
            ElasticSearchEngine oElasticSearchEngine = new ElasticSearchEngine(sIndexName);
            oElasticSearchEngine.createIndex(sIndexName);

            //Delete the document if already exists.

            oElasticSearchEngine.deletePageFromDocument(sIndexName, oDocument.Id.ToString(), nPageNumber);
            oElasticSearchEngine.addPage(sIndexName, oDocument,nPageNumber);
        }

        protected bool AddPages(addpagesinput oaddpagesinput)
        {
            document oDocument = oaddpagesinput.Document;

            if (oDocument.Publication == null)
                _context.Entry(oDocument).Reference(d => d.Publication).Load();

            if (oDocument.Publication != null && oDocument.Publication.Publisher == null)
                _context.Entry(oDocument.Publication).Reference(p => p.Publisher).Load();

            if (oDocument.Publication != null && oDocument.Publication.PublicationTemplate == null)
                _context.Entry(oDocument.Publication).Reference(p => p.PublicationTemplate).Load();

            int nPageNumber = oaddpagesinput.pagenumber;
            bool bAddafter = oaddpagesinput.addafter;
            string sBucketName = oaddpagesinput.bucketname;
            string sFileToAdd = oaddpagesinput.filename;

            // The EXISTING document files (PDF, document.json, page files) must be present locally for
            // init()/RenameFiles below. On the SQS worker the per-job local dir is empty, so download the
            // whole doc first (delete does this via downloadFilesForDelete). downloadFiles() cleans +
            // repopulates the dir, so it MUST run before the file-to-add is downloaded into it.
            UpdateProgress(oaddpagesinput.Job, 10);
            string sDocBucket = string.IsNullOrEmpty(oDocument.Publication?.Publisher?.BucketName)
                ? Constants.DEFAULT_DOCS_LOCATION : oDocument.Publication.Publisher.BucketName;
            downloadFiles(oDocument, sDocBucket);
            UpdateProgress(oaddpagesinput.Job, 20);

            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);


            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
            if (!File.Exists(sPDFFileName))
                throw new Exception("Existing document PDF missing after download: " + sPDFFileName);
            string sFileToAddName = Guid.NewGuid().ToString() + ".pdf";
            string sNewPDFFile = Path.Combine(sDocumentPath, sFileToAddName);
            DCS3Services oDCS3Services = new DCS3Services();
            bool bFileDownloaded = oDCS3Services.downloadFile(sBucketName, sFileToAdd, sNewPDFFile);
            if (bFileDownloaded == false)
            {
                _logger.LogError("Cannot Download the file " + sBucketName + " " + sFileToAdd);
                throw new Exception("Cannot Download the file " + sBucketName + " " + sFileToAdd);
            }
            DCJobs.DocumentConvertor oDocumentConvertor2 = new DCJobs.DocumentConvertor(_logger);
            oDocumentConvertor2.init(sNewPDFFile);
            int nPageToAddCount = oDocumentConvertor2.getNumberOfPages();
            oDocumentConvertor2.release();   // free the file-to-add handle BEFORE the merge re-opens/deletes it
            if (nPageToAddCount < 1)
                throw new Exception("Uploaded PDF unreadable (encrypted/damaged?): " + sFileToAdd);
            DCJobs.DocumentConvertor oDocumentConvertor = new DCJobs.DocumentConvertor(_logger);
            oDocumentConvertor.init(sPDFFileName);
            int nPageCount = oDocumentConvertor.getNumberOfPages();
            oDocumentConvertor.release();    // free the existing-PDF handle BEFORE the merge deletes/replaces it
            if (nPageCount < 1)
                throw new Exception("Existing document PDF unreadable: " + sPDFFileName);
            // Hold NO PDF file handles here: AddPagesToPDF re-opens both PDFs and replaces the
            // existing one (release() above now truly closes the iText readers).
            // Merge via the worker's cpdf-backed AddPagesToPDF (JobWorker.DocumentConvertor in
            // PDFConverter.cs) — the DCJobs iText PdfPageFormCopier merge throws a contentless
            // "Unknown PdfException" on catalog PDFs with AcroForms. The cpdf rewrite previously
            // existed only in JobWorker.DocumentConvertor, which this method never called.
            JobWorker.DocumentConvertor oMergeConvertor = new JobWorker.DocumentConvertor();
            if (!oMergeConvertor.AddPagesToPDF(nPageNumber, sPDFFileName, sNewPDFFile, bAddafter))
                throw new Exception("AddPagesToPDF failed merging '" + sFileToAdd + "' into " + oDocument.PDFFileName);
            try { File.Delete(sNewPDFFile); } catch { }
            UpdateProgress(oaddpagesinput.Job, 35);   // pages merged into PDF
            if (bAddafter)
                nPageNumber++;
            oDocumentConvertor.RenameFiles(nPageNumber, oDocument, sOutputDirectory, nPageToAddCount, bAddafter);
            UpdateProgress(oaddpagesinput.Job, 50);   // existing page files shifted
            DCJobs.DocumentConvertor oDocumentConvertor3 = new DCJobs.DocumentConvertor(_logger); //Todo reopen document instead
            oDocumentConvertor3.init(sPDFFileName);
            string docfilepath = Path.Combine(sOutputDirectory, "document.json");
            JObject docJson = JObject.Parse(System.IO.File.ReadAllText(docfilepath));
            //string selectpagessequence = "$..page";
            JArray allpages = docJson["issue"]["page"].Value<JArray>();


            bool bIntro = false;
            string selectintrosequence = "$..page[?(@..isintropage=='" + "1" + "')]";
            IEnumerable<JToken> intropagesection = docJson.SelectTokens(selectintrosequence);
            if (intropagesection.Count() > 0)
                bIntro = true;

            ArrayList arrVersions = new ArrayList();
            string selectsequence = "";
            for (int i = nPageCount; i >= nPageNumber; i--)
            {

                selectsequence = "$..page[?(@..sequence=='" + i.ToString() + "')]";
                int nCurrentPage = i + nPageToAddCount;
                string selectsequenceNext = "$..page[?(@..sequence=='" + i.ToString() + "')]";
                IEnumerable<JToken> pagesection = docJson.SelectTokens(selectsequence);
                IEnumerable<JToken> pagesectionNext = docJson.SelectTokens(selectsequenceNext);
                JToken pagetokenindoc = pagesection.First();
                JToken pagetokenindocnext = pagesectionNext.First();
                JValue jpageversion = (JValue)pagetokenindoc["@attributes"]["version"];

                JValue thumb = (JValue)pagetokenindoc["@attributes"]["thumb"];
                thumb.Value = string.Format("Thumbnail_{0}.jpg", nCurrentPage);

                JValue iphoneImage = (JValue)pagetokenindoc["@attributes"]["iphoneImage"];
                iphoneImage.Value = string.Format("ZPage_{0}.jpg", nCurrentPage);

                if (pagetokenindoc["@attributes"]["svgimg"] != null)
                {
                    JValue svgImage = (JValue)pagetokenindoc["@attributes"]["svgimg"];
                    svgImage.Value = string.Format("SPage_{0}.svg", nCurrentPage);
                }

                JValue sequence = (JValue)pagetokenindoc["@attributes"]["sequence"];
                sequence.Value = nCurrentPage.ToString();

                JValue name = (JValue)pagetokenindoc["@attributes"]["name"];
                name.Value = nCurrentPage.ToString();
                if (pagesectionNext != null)
                {
                    arrVersions.Add(jpageversion.Value);
                    JValue jpageversionNext = (JValue)pagetokenindocnext["@attributes"]["version"];
                    // version strings are not always Int32 (older docs carry tick-derived or
                    // empty values) — Convert.ToInt32 threw and failed the whole job. Bump when
                    // numeric, otherwise restamp tick-based like the old processor.
                    jpageversion.Value = long.TryParse(System.Convert.ToString(jpageversionNext.Value), out long nVer)
                        ? (nVer + 1).ToString()
                        : (DateTime.Now.Ticks % int.MaxValue).ToString();
                }

            }
            PDFRemoveLayers oPdfRemoveLayers = new PDFRemoveLayers();
            if (oDocument.Publication.PublicationTemplate.usesvghtml5)
            {

                oPdfRemoveLayers.m_nTimeout = 50000;
                oPdfRemoveLayers.PDFSourceFileName = sPDFFileName;
                string sNoImagesPDF = string.Format("{0}\\{1}", sOutputDirectory, "Full_No_Images.pdf");
                oPdfRemoveLayers.PDFTargetFileName = sNoImagesPDF;
                oPdfRemoveLayers.LayerFlag = "T";
                oPdfRemoveLayers.Execute();
                oPdfRemoveLayers.NumberOfPages = oDocument.NumberOfPages;
            }

            for (int i = 0; i < nPageToAddCount; i++)
            {
                string sPageID = Guid.NewGuid().ToString();
                int nCurrentPage = nPageNumber + i;



                oDocumentConvertor3.createPageXML(sOutputDirectory, nCurrentPage, sPageID);
                XmlDocument xmlPage = new XmlDocument();
                string sPageName = string.Format("Page_{0}.xml", nCurrentPage);
                string sPageFileName = Path.Combine(sOutputDirectory, sPageName);
                xmlPage.Load(sPageFileName);
                //xmlPage.Save(sPageFileName);
                string sJsonPageName = Path.Combine(sOutputDirectory, string.Format("Page_{0}.json", nCurrentPage));
                string sPageResult = DocumentConvertor.getPageJson(oDocument, xmlPage, oDocument.Publication.PublicationTemplate);
                System.IO.File.WriteAllText(sJsonPageName, sPageResult);

                //Generate images.
                int nResolution = System.Convert.ToInt32(oDocument.HiPageResolution);
                int nImageQuality = 85;
                createimagesinput oCreateHiResImagesInput = oDocumentConvertor3.createImageInput(nResolution, -1, -1, sPDFFileName, sOutputDirectory, "ZPage_", nImageQuality, null, "Creating High Resolution Images");
                oCreateHiResImagesInput.Resolution = System.Convert.ToInt32(oDocument.HiPageResolution);
                int nPageWidth, nPageHeight;
                oDocumentConvertor3.createImageEx(oCreateHiResImagesInput, 1, nCurrentPage, out nPageWidth, out nPageHeight);

                //Create SVG
                if (oDocument.Publication.PublicationTemplate.usesvghtml5)
                {
                    oPdfRemoveLayers.convertPage(sOutputDirectory, nCurrentPage);
                }

                JObject new_page = new JObject();
                Random rnd = new Random();
                int v = rnd.Next(1, 10000000); // creates a number between 1 and 12

                new_page["@attributes"] = new JObject();
                new_page["@attributes"]["id"] = Guid.NewGuid().ToString();
                // same non-numeric-version tolerance as the shift loop above
                if (arrVersions.Count > i && long.TryParse(System.Convert.ToString(arrVersions[i]), out long nPrevVer))
                {
                    new_page["@attributes"]["version"] = (nPrevVer + 1).ToString();
                }
                else
                {
                    new_page["@attributes"]["version"] = "1"; //Need to check
                }
                new_page["@attributes"]["sequence"] = nCurrentPage.ToString();
                new_page["@attributes"]["thumb"] = string.Format("Thumbnail_{0}.jpg", nCurrentPage);
                new_page["@attributes"]["name"] = nCurrentPage.ToString();
                new_page["@attributes"]["width"] = nPageWidth.ToString();
                new_page["@attributes"]["height"] = nPageHeight.ToString();
                new_page["@attributes"]["contentType"] = "";
                new_page["@attributes"]["iphoneImage"] = string.Format("ZPage_{0}.jpg", nCurrentPage);
                if (oDocument.Publication.PublicationTemplate.usesvghtml5)
                {
                    new_page["@attributes"]["svgimg"] = string.Format("SPage_{0}.svg", nCurrentPage);
                }
                new_page["@attributes"]["isintropage"] = "0";
                new_page["@attributes"]["recolor"] = 0;
                if (bIntro)
                    allpages.Insert(nCurrentPage, new_page);
                else
                    allpages.Insert(nCurrentPage - 1, new_page);

            }
            if (oDocument.Publication.PublicationTemplate.usesvghtml5)
            {
                oPdfRemoveLayers.deleteOutput();
            }
            ////Update TOC.
            selectsequence = "$..TOC.[*]";
            IEnumerable<JToken> tocitems = docJson.SelectTokens(selectsequence);
            updateTOCItems(tocitems, nPageNumber, nPageToAddCount);

            selectsequence = "$..bookmark.[*].@attributes";
            IEnumerable<JToken> bookmarksitems = docJson.SelectTokens(selectsequence);
            updateBookmarks(bookmarksitems, nPageNumber, nPageToAddCount);

            //Update goto page links.
            System.IO.File.WriteAllText(docfilepath, docJson.ToString());
            string sHTMLOutputDirectory = Path.Combine(sOutputDirectory, "html");
            SearchHighligh.deleteDocument(_context,oDocument.Id.ToString());
            if (Directory.Exists(sHTMLOutputDirectory))
                Directory.Delete(sHTMLOutputDirectory, true);
            oDocumentConvertor3.release();

            bool bTextExtractor = _context.serversettings.FirstOrDefault(x => x.Name == "TextExtractor").Value != "0";
            bool bres;
            if (bTextExtractor)
            {
                bres = PDF2HTML.convertAllPagesCommandLine(sPDFFileName, sDocumentPath, sHTMLOutputDirectory);
                if (!bres)
                {
                    PDF2HTML.convertAllPagesMuPDF(sPDFFileName, sDocumentPath, sHTMLOutputDirectory);
                }
            }
            else
            {
                bres = PDF2HTML.convertAllPagesMuPDF(sPDFFileName, sDocumentPath, sHTMLOutputDirectory);
                if (!bres)
                {
                    PDF2HTML.convertAllPagesCommandLine(sPDFFileName, sDocumentPath, sHTMLOutputDirectory);
                }

            }
            generateSelectableText(oDocument);
            //PDF2HTML.convertAllPagesCommandLine(sPDFFileName, sDocumentPath, sHTMLOutputDirectory);
            oDocument.NumberOfPages = oDocument.NumberOfPages + nPageToAddCount;
            updateTOCLinks(oDocument, nPageNumber, nPageToAddCount);
            UpdateProgress(oaddpagesinput.Job, 75);   // new-page images + HTML regenerated

            return true;
        }

        protected bool generateSelectableText(document doc)
        {
            try
            {
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
            catch (Exception)
            {
                return false;
            }
            return true;
        }


        private void updateTOCLinks(document oSourceDocument, int nStartPage, int offset)
        {


            int nPageCount = oSourceDocument.NumberOfPages;

            string sSourceDoc = DocumentUtilBase.getDocumentPath(oSourceDocument);
            for (int i = 0; i < nPageCount; i++)
            {
                bool bDirty = false;
                int nPageNumber = i + 1;
                string sPageName = "Page_" + nPageNumber.ToString() + ".json";
                string sPagePathSource = Path.Combine(sSourceDoc, sPageName);
                if (!File.Exists(sPagePathSource))
                    continue;
                JObject pageJson = JObject.Parse(System.IO.File.ReadAllText(sPagePathSource));
                string selectsequence = "$..link[?(@..type=='" + "2" + "')]";
                IEnumerable<JToken> jLinks = pageJson.SelectTokens(selectsequence);
                foreach (JToken item in jLinks)
                {

                    JValue pageVal = (JValue)item["@attributes"]["url"];
                    try
                    {
                        int nPage = System.Convert.ToInt32(pageVal.Value);
                        if (nPage >= nStartPage)
                        {
                            nPage += offset;
                            pageVal.Value = nPage;
                            bDirty = true;
                        }
                    }
                    catch (Exception)
                    {

                    }
                }
                if (bDirty)
                    System.IO.File.WriteAllText(sPagePathSource, pageJson.ToString());
            }
        }



        private void updateBookmarks(IEnumerable<JToken> bookmarkitems, int nStartPage, int offset)
        {
            foreach (JToken item in bookmarkitems)
            {
                if (item["pageIndex"] != null)
                {
                    JValue pageVal = (JValue)item["pageIndex"];
                    int nPage = System.Convert.ToInt32(pageVal.Value);
                    if (nPage >= nStartPage)
                    {
                        nPage += offset;
                        pageVal.Value = nPage;

                    }
                }
            }
        }

        [Queue("mikitest")]
        public bool FlippingGif(string sreplaceinput)
        {

            job oJob = _context.job
                            .Where(j => j.Id == sreplaceinput)
                            .FirstOrDefault();
            var ogenerategifflipbookinput = _context.creategifflippingbookinput
              .Include(r => r.Document)
              .Include(r => r.Document.Publication)
              .Include(r => r.Document.Publication.Publisher)
              .Include(r => r.Job)
              .Where(p => p.Job.Id == oJob.Id)
              .FirstOrDefault();
            FlippingGif(ogenerategifflipbookinput);
            return true;
        }

        public void FlippingGif(creategifflippingbookinput ocreateflipbookinput)
        {
            
            int nNumberOfPages = ocreateflipbookinput.NumberOfImages;
            int width  = ocreateflipbookinput.Width;
            int height = ocreateflipbookinput.Height;
            double dRatio = ocreateflipbookinput.Ratio;
            string sbackgroundColor = ocreateflipbookinput.BackGroundColor;
            int nInterval = ocreateflipbookinput.FlipInterval;
            string gifFileName = ocreateflipbookinput.GifFileName;

            document oDocument = ocreateflipbookinput.Document;
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);
            string sFlippingBookGif = Path.Combine(sDocumentPath, "MiniFlipper");
            if (!Directory.Exists(sFlippingBookGif))
                Directory.CreateDirectory(sFlippingBookGif);
          
            string sPrefixName = "Thumbnail_";
            if (width > 300 || height > 300)
                sPrefixName = "ZPage_";
            downloadFiles(oDocument, nNumberOfPages, sPrefixName);
            GifFlippingBookCreator oGifFlippingBookCreator = new GifFlippingBookCreator();
            oGifFlippingBookCreator.CreateGifFlippingBook(width, height, dRatio, sbackgroundColor, nInterval, sPrefixName, ".jpg",
                       nNumberOfPages,
                       sDocumentPath,
                       sFlippingBookGif,
                       gifFileName);
            string sOutputName = gifFileName + ".gif";
            string sOutputFile = Path.Combine(sFlippingBookGif, sOutputName);
            if (File.Exists(sOutputFile))
            {
                string sKeyPrefix = string.Format("{0}/{1}/{2}/MiniFlipper", oDocument.PublisherFolderName, oDocument.TemplateFolderName, oDocument.Id);
                DCS3Services oDCS3Services = new DCS3Services();
                oDCS3Services.uploadFile(Constants.DEFAULT_DOCS_LOCATION, sOutputFile, sKeyPrefix);
            }

        }

        public bool downloadFiles(document oDocument, int nNumberOfPages, string sPrefix)
        {
            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
            //Download the PDF File.
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                oDocument.Id);
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            DCS3Services oDCS3Services = new DCS3Services();
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);
            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
            int nPageCount = oDocument.NumberOfPages;
            nNumberOfPages = Math.Min(nPageCount, nNumberOfPages);
            for (int i = 0; i < nNumberOfPages; i++)
            {
                int nCurrentPage = i + 1;
                string sName = string.Format("{0}{1}.jpg", sPrefix, nCurrentPage);
                string sThumbFileToDownload = Path.Combine(sDocumentPath, sName);
                string sThumbDownloadKey = string.Format("{0}/{1}", sKeyPrefix, sName);
                oDCS3Services.downloadFile(sBucketName, sThumbDownloadKey, sThumbFileToDownload);

            }
            return true;
        }


        [Queue("mikitest")]
        public async Task<bool> ReplacePages(string sreplaceinput)
        {
            var oaddpagesinput = _context.replacepageinput
              .Include(r => r.Document)
              .Include(r => r.Document.Publication)
              .Include(r => r.Document.Publication.Publisher)
              .Include(r => r.Job)
              .Where(p => p.Id == sreplaceinput)
              .FirstOrDefault();
            await ReplacePages(oaddpagesinput);
            return true;
        }


        [Queue("mikitest")]
        public bool AddPages(string saddpagesinput)
        {
            var oaddpagesinput = _context.addpagesinput
               .Include(r => r.Document)
               .Include(r => r.Document.Publication)
               .Include(r => r.Document.Publication.Publisher)
               .Include(r => r.Job)
               .Where(p => p.Id == saddpagesinput)
               .FirstOrDefault();
            AddPages(oaddpagesinput);
            return true;
        }

        [Queue("mikitest")]
        public bool DelPage(string sdeletepagesinput)
        {

            try
            {
                var odeletepagesinput = _context.deletepagesinput
                 .Include(r => r.Document)
                 .Include(r => r.Document.Publication)
                 .Include(r => r.Document.Publication.Publisher)
                 .Include(r => r.Job)
                 .Where(p => p.Id == sdeletepagesinput)
                 .FirstOrDefault();
                bool delOk = DeletePages(odeletepagesinput);

                document oDocumentDiag = odeletepagesinput.Document;
                // DIAGNOSTIC (readable via DB; JobProcessor overwrites job.Desctiption but not the
                // input row): record what the worker actually produced locally — the renumbered page
                // count + the id now at sequence 2. Lets us tell an internal renumber failure apart
                // from an external overwrite of the corrected document.json.
                try
                {
                    string djp = Path.Combine(DocumentUtilBase.getDocumentPath(oDocumentDiag), "document.json");
                    if (File.Exists(djp))
                    {
                        JObject djj = JObject.Parse(File.ReadAllText(djp));
                        JArray djpages = (JArray)djj["issue"]["page"];
                        JToken p2 = djpages.FirstOrDefault(x => (string)x["@attributes"]["sequence"] == "2");
                        string seq2id = p2 != null ? (string)p2["@attributes"]["id"] : "missing";
                        odeletepagesinput.Description = (odeletepagesinput.Description ?? "") + $" || POST delOk={delOk} npages={oDocumentDiag.NumberOfPages} jsonpages={djpages.Count} seq2id={seq2id}";
                    }
                    else
                        odeletepagesinput.Description = $"DEL delOk={delOk} LOCAL document.json MISSING";
                    _context.Update(odeletepagesinput);
                    _context.SaveChanges();
                }
                catch (Exception dex)
                {
                    odeletepagesinput.Description = "DEL diag-err: " + dex.Message;
                    _context.Update(odeletepagesinput);
                    _context.SaveChanges();
                }

                if (!delOk)
                {
                    job oJobFail = odeletepagesinput.Job;
                    oJobFail.Status = Constants.JobProcessingStatus.Failed.ToString();
                    oJobFail.Desctiption = "Delete pages failed (PDF edit returned false / aborted)";
                    _context.Update(oJobFail);
                    _context.SaveChanges();
                    return false;
                }

                job oJob =  odeletepagesinput.Job;
                //3. Upload files
                document oDocument = odeletepagesinput.Document;
                updateDocument(oDocument);
                UpdateProgress(oJob, 85);   // uploaded to S3

                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
                if (File.Exists(sPDFFileName) )
                {

                    var crc32 = new System.IO.Hashing.Crc32();
                    var fs = File.Open(sPDFFileName, FileMode.Open);
                    crc32.Append(fs);
                    byte[] hashBytes = crc32.GetCurrentHash();
                    oDocument.crc32 = BitConverter.ToUInt32(hashBytes, 0);
                    fs.Close();
                }
                

                DCJobs.DocumentConvertor.indexDocument(_context, oDocument);
                UpdateProgress(oJob, 95);   // re-indexed in OpenSearch
                // NOTE: DeletePages already regenerated the page HTML (convertAllPagesMuPDF) and that
                // output was what updateDocument uploaded and indexDocument indexed. Calling
                // createTextFiles here re-rendered the ENTIRE document's HTML a second time AFTER the
                // upload+index — wasted work that doubled the delete time. Removed.

                oJob.Progress = 100;
                oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                _context.Update(oJob);
                _context.Update(oDocument);
                _context.SaveChanges();

            }
            catch (Exception ex)
            {
                Console.WriteLine("# BackgroundJob.Schedule err:" + ex.Message);
                return false;
            }
            return true;
        }

        public static UInt32 GetCrc32(string sInputFile)
        {
            UInt32 ret = 0;
            if (File.Exists(sInputFile))
            {

                var crc32 = new Crc32();
                var fs = File.Open(sInputFile, FileMode.Open);
                crc32.Append(fs);
                byte[] hashBytes = crc32.GetCurrentHash();
                ret = BitConverter.ToUInt32(hashBytes, 0);
                fs.Close();
            }
            return ret;

        }

        protected async Task indexv1documentAsync(document doc)
        {
            string sJobTypeName = Constants.EJobType.JobExecutionIndexDocument.ToString();
            jobtype oJobType = _context.jobtype.FirstOrDefault(x => x.Name == sJobTypeName);
            if (oJobType == null) { oJobType = new jobtype(); oJobType.Name = sJobTypeName; }
            job oCurrentJob = new job(oJobType);
            _context.job.Add(oCurrentJob);
            _context.SaveChanges();

            indexdocumentinput oIndexDocumentInput = new indexdocumentinput();
            oIndexDocumentInput.Job = oCurrentJob;
            oIndexDocumentInput.Document = doc;
            _context.indexdocumentinput.Add(oIndexDocumentInput);

            _context.SaveChanges();
            DCSQS oDCSQS = new DCSQS(_context, _sqsclient);
            await oDCSQS.addJobToQueue(oCurrentJob, Constants.JobQueueName.DistributedHPClientQueue);

        }


        private void updateTOCLinksRemove(document oSourceDocument, int nStartPage, int offset)
        {
            int nPageCount = oSourceDocument.NumberOfPages;

            string sSourceDoc = DocumentUtilBase.getDocumentPath(oSourceDocument);
            for (int i = 0; i < nPageCount; i++)
            {
                bool bDirty = false;
                int nPageNumber = i + 1;
                string sPageName = "Page_" + nPageNumber.ToString() + ".json";
                string sPagePathSource = Path.Combine(sSourceDoc, sPageName);
                if (!File.Exists(sPagePathSource))
                    continue;
                JObject pageJson = JObject.Parse(System.IO.File.ReadAllText(sPagePathSource));
                string selectsequence = "$..link[?(@..type=='" + "2" + "')]";
                IEnumerable<JToken> jLinks = pageJson.SelectTokens(selectsequence);
                foreach (JToken item in jLinks)
                {

                    JValue pageVal = (JValue)item["@attributes"]["url"];
                    try
                    {
                        int nPage = System.Convert.ToInt32(pageVal.Value);
                        if (nPage >= nStartPage && nPage < nStartPage + offset)
                        {
                            //We have removed the page
                            item.Parent.Remove();
                            bDirty = true;
                        }
                        else if (nPage >= nStartPage)
                        {
                            nPage -= offset;
                            pageVal.Value = nPage;
                            bDirty = true;
                        }
                    }
                    catch (Exception)
                    {

                    }
                }
                if (bDirty)
                    System.IO.File.WriteAllText(sPagePathSource, pageJson.ToString());

            }



        }

       


        private bool DeletePages(deletepagesinput odeletepagesinput)
        {
            document oDocument = odeletepagesinput.Document;
            int nFromPage = odeletepagesinput.frompage;
            int nToPage = odeletepagesinput.topage;

            bool bRet = false;
            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);

            //1. Download files.
            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
            UpdateProgress(odeletepagesinput.Job, 5);    // started — move off 0 immediately
            downloadFilesForDelete(oDocument, sBucketName, nFromPage, nToPage, odeletepagesinput.Job);
            UpdateProgress(odeletepagesinput.Job, 15);   // files downloaded
            if (checkJobStatus(odeletepagesinput.Job, "Download Files") == false)
                return false;

            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
            int nPageNumber = nFromPage;
            int nNumberOfPagesToRemove = nToPage - nPageNumber + 1;

            int nPDFPageCount = oDocument.NumberOfPages;

            // PDFTron in-place first: removes the pages without re-extracting the whole document
            // like the cpdf range-cut below does, so it scales to very large docs and preserves the
            // AcroForm. cpdf fallback if the tool is unavailable or the in-place remove fails.
            bRet = JobWorker.DocumentConvertor.RemovePagesViaPdfUtils(sPDFFileName, nPageNumber, nNumberOfPagesToRemove);
            if (!bRet)
            {
                Console.WriteLine("DeletePages: PDFUtils path unavailable/failed, falling back to cpdf");
                PDFExtractPagesCPDF e2 = new PDFExtractPagesCPDF(_logger);
                string sRange = PDFExtractPagesCPDF.getDeleteCommand(nPageNumber, nNumberOfPagesToRemove, nPDFPageCount);

                e2.PDFSourceFileName = sPDFFileName;
                e2.Range = sRange;
                e2.OutputDir = sDocumentPath;
                //string sFileName = string.Format("Page_{0}.pdf", e2.Range);
                string outputfile = Path.Combine(e2.OutputDir, oDocument.PDFFileName);
                e2.PDFTargetFileName = outputfile;
                bRet = e2.Execute();
            }
            UpdateProgress(odeletepagesinput.Job, 25);   // PDF cut done

            DCJobs.DocumentConvertor oDocumentConvertor = new DCJobs.DocumentConvertor(_logger);
            oDocumentConvertor.RenameFilesDelete(nPageNumber, oDocument, sOutputDirectory, nNumberOfPagesToRemove);
            UpdateProgress(odeletepagesinput.Job, 40);   // page files shifted

            string docfilepath = Path.Combine(sOutputDirectory, "document.json");
            JObject docJson = JObject.Parse(System.IO.File.ReadAllText(docfilepath));
            //string selectpagessequence = "$..page";
            JArray allpages = docJson["issue"]["page"].Value<JArray>();
            int nPageCount = oDocument.NumberOfPages;

            Hashtable hashVersions = new Hashtable();
            ArrayList arrVersions = new ArrayList();
            string selectsequence = "";
            for (int i = 0; i < nPageCount - nPageNumber + 1; i++)
            {
                int nCurrentPage = nPageNumber + i;
                selectsequence = "$..page[?(@..sequence=='" + nCurrentPage.ToString() + "')]";
                int nNextPagePage = nCurrentPage + nNumberOfPagesToRemove;
                string selectsequenceNext = "$..page[?(@..sequence=='" + nNextPagePage.ToString() + "')]";
                IEnumerable<JToken> pagesection = docJson.SelectTokens(selectsequence);
                IEnumerable<JToken> pagesectionNext = docJson.SelectTokens(selectsequenceNext);
                JToken pagetokenindocnext = null;
                JValue jpagenextversion = null;
                if (pagesectionNext.Count() > 0)
                {
                    pagetokenindocnext = pagesectionNext.First();

                    jpagenextversion = (JValue)pagetokenindocnext["@attributes"]["version"];
                }
                if (pagesection.Count() > 0)
                {
                    JToken pagetokenindoc = pagesection.First();
                    JValue jpageversion = (JValue)pagetokenindoc["@attributes"]["version"];

                    if (nCurrentPage < nPageNumber + nNumberOfPagesToRemove)
                    {
                        //Need to remove from json.
                        pagetokenindoc.Remove();
                    }
                    if (jpagenextversion != null)
                    {
                        hashVersions.Add(nNextPagePage, jpagenextversion.Value);
                        jpagenextversion.Value = string.Format("{0}", System.Convert.ToInt32(jpageversion.Value) + 1);
                    }
                }
                else
                {
                    if (hashVersions.Contains(nCurrentPage) && jpagenextversion != null)
                    {
                        hashVersions.Add(nNextPagePage, jpagenextversion.Value);
                        jpagenextversion.Value = string.Format("{0}", System.Convert.ToInt32(hashVersions[nCurrentPage]) + 1);
                    }

                }

                if (pagetokenindocnext != null)
                {
                    JValue thumb = (JValue)pagetokenindocnext["@attributes"]["thumb"];
                    thumb.Value = string.Format("Thumbnail_{0}.jpg", nCurrentPage);

                    JValue iphoneImage = (JValue)pagetokenindocnext["@attributes"]["iphoneImage"];
                    iphoneImage.Value = string.Format("ZPage_{0}.jpg", nCurrentPage);
                    if (pagetokenindocnext["@attributes"]["svgimg"] != null)
                    {
                        JValue svgimg = (JValue)pagetokenindocnext["@attributes"]["svgimg"];
                        svgimg.Value = string.Format("SPage_{0}.svg", nCurrentPage);
                    }

                    JValue sequence = (JValue)pagetokenindocnext["@attributes"]["sequence"];
                    sequence.Value = nCurrentPage.ToString();

                    JValue name = (JValue)pagetokenindocnext["@attributes"]["name"];
                    name.Value = nCurrentPage.ToString();
                }
            }
            ////Update TOC.
            selectsequence = "$..TOC.[*]";
            IEnumerable<JToken> tocitems = docJson.SelectTokens(selectsequence);
            updateTOCItemsRemove(tocitems, nPageNumber, nNumberOfPagesToRemove);

            selectsequence = "$..bookmark.[*].@attributes";
            IEnumerable<JToken> bookmarksitems = docJson.SelectTokens(selectsequence);
            updateBookmarksRemove(bookmarksitems, nPageNumber, nNumberOfPagesToRemove);

            // Persist the renumbered document.json + page count IMMEDIATELY (the page files were
            // already renamed by RenameFilesDelete). Doing this before any early-exit guarantees the
            // document structure/labels always match the shifted files — bailing here previously left
            // the doc half-edited (files shifted, document.json/labels not), which is the page-number
            // + search-highlight mismatch we saw.
            int afterLoopPageCount = ((JArray)docJson["issue"]["page"]).Count;
            System.IO.File.WriteAllText(docfilepath, docJson.ToString());
            // INTERNAL instrumentation (read via deletepagesinput.Description): pinpoints whether the
            // renumber loop actually shifted. loopBound = oDocument.NumberOfPages at loop start.
            try
            {
                odeletepagesinput.Description = $"INT loopBound={nPageCount} pdfPageCount={nPDFPageCount} fromPg={nPageNumber} remove={nNumberOfPagesToRemove} afterLoopPages={afterLoopPageCount} cpdfOk={bRet}";
                _context.Update(odeletepagesinput);
                _context.SaveChanges();
            }
            catch { }
            oDocument.NumberOfPages = oDocument.NumberOfPages - nNumberOfPagesToRemove;
            updateTOCLinksRemove(oDocument, nPageNumber, nNumberOfPagesToRemove);
            _context.Update(oDocument);

            if (checkJobStatus(odeletepagesinput.Job, "Before index document") == false)
                return false;

            //Update goto page links.
            UpdateProgress(odeletepagesinput.Job, 50);   // document.json renumbered + saved
            string sHTMLOutputDirectory = Path.Combine(sOutputDirectory, "html");
            SearchHighligh.deleteDocument(_context,oDocument.Id.ToString());
            PDF2HTML.convertAllPagesMuPDF(sPDFFileName, sOutputDirectory, sHTMLOutputDirectory);
            UpdateProgress(odeletepagesinput.Job, 70);   // page HTML/images regenerated
            return bRet;
        }




        private void updateBookmarksRemove(IEnumerable<JToken> bookmarkitems, int nStartPage, int offset)
        {
            foreach (JToken item in bookmarkitems)
            {
                if (item["pageIndex"] != null)
                {
                    JValue pageVal = (JValue)item["pageIndex"];
                    int nPage = System.Convert.ToInt32(pageVal.Value);
                    if (nPage >= nStartPage && nPage < nStartPage + offset)
                    {
                        //We have removed the page
                        item.Parent.Remove();

                    }
                    else if (nPage >= nStartPage)
                    {
                        nPage -= offset;
                        pageVal.Value = nPage;

                    }
                }
            }
        }

        private void updateTOCItemsRemove(IEnumerable<JToken> tocitems, int nStartPage, int offset)
        {
            foreach (JToken item in tocitems.ToList())
            {
                if (item["page"] != null)
                {
                    JValue pageVal = (JValue)item["page"];
                    int nPage = System.Convert.ToInt32(pageVal.Value);
                    if (nPage >= nStartPage && nPage < nStartPage + offset)
                    {
                        //We have removed the page
                        item.Remove();
                    }
                    else if (nPage >= nStartPage)
                    {
                        nPage -= offset;
                        pageVal.Value = nPage;
                    }

                }
                if (item["children"] != null)
                {
                    IEnumerable<JToken> tocchildren = item["children"];
                    updateTOCItemsRemove(tocchildren, nStartPage, offset);
                }

            }
        }


        private void updateTOCItems(IEnumerable<JToken> tocitems, int nStartPage, int offset)
        {
            foreach (JToken item in tocitems)
            {
                if (item["page"] != null)
                {
                    JValue pageVal = (JValue)item["page"];
                    int nPage = System.Convert.ToInt32(pageVal.Value);
                    if (nPage >= nStartPage)
                    {
                        nPage += offset;
                        pageVal.Value = nPage;
                    }

                }
                if (item["children"] != null)
                {
                    IEnumerable<JToken> tocchildren = item["children"];
                    updateTOCItems(tocchildren, nStartPage, offset);
                }

            }
        }

        // Persists incremental job progress so the admin progress modal (polling
        // /Progress/checkJobStatus) advances during the multi-step delete instead of sitting at one %.
        private void UpdateProgress(job oJob, int pct)
        {
            try
            {
                if (oJob == null) return;
                oJob.Progress = pct;
                oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
                _context.Update(oJob);
                _context.SaveChanges();
            }
            catch { /* progress is best-effort; never fail the job over it */ }
        }

        public bool checkJobStatus(job oJob,string step)
        {
            if (oJob.Status == Constants.JobProcessingStatus.Aborted.ToString())
            {
                oJob.Desctiption = "Aborted after step " + step;
                oJob.Progress = 100;
                _context.Update(oJob);
                _context.SaveChanges();
                return false;

            }
            return true;
        }
        /*
        public bool executeJob(job oJob)
        {
            _logger.LogDebug("Processing Job with the ID for remove pages" + oJob.Id);

            oJob.Progress = 10;
            oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
            _context.Update(oJob);


            string sFullPDFFileName = "";
            string sOutputDirectory = "";
            int nfrompage = 1;
            int ntopage = 1;
            DocumentConvertor oDocumentConvertor = new DocumentConvertor();
            DCS3Services oDCS3Services = new DCS3Services();
            //Create the document repository files
            document oDocument = null;

            string sFilename = "";
            int nPageNumber = 1;

            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;

            //try
            {


                var oDeletePagesInput = _context.deletepagesinput
                    .Where(p => p.Job.Id == oJob.Id)
                    .FirstOrDefault();
                if (oDeletePagesInput != null)
                {
                    oDocument = oDeletePagesInput.Document;
                    sFullPDFFileName = oDeletePagesInput.InputFileName;
                    sOutputDirectory = oDeletePagesInput.OutputDirectory;
                    nfrompage = oDeletePagesInput.frompage;
                    ntopage = oDeletePagesInput.topage;
                }

                if (checkJobStatus(oJob, "Initializing Job") == false)
                    return false;

                oJob.Progress = 10;
                oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
                _context.Update(oJob);
                _context.SaveChanges();
                //1. Download files.
                downloadFilesForDelete(oDocument, sBucketName, nfrompage, ntopage);
                if (checkJobStatus(oJob,  "Download Files") == false)
                    return false;
                oJob.Progress = 30;
                oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
                _context.Update(oJob);
                _context.SaveChanges();
                //2. Add the pages.
                bool bRet = DeletePages(oDocument, nfrompage, ntopage);
                if (!bRet)
                {
                    oJob.Progress = 100;
                    oJob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    _context.Update(oJob);
                    _context.SaveChanges();
                    return false;
                }

                if (checkJobStatus(oJob, "Delete Pages from PDF and json") == false)
                    return false;


                oJob.Progress = 60;
                oJob.Status = Constants.JobProcessingStatus.Processing.ToString();
                _context.Update(oJob);
                _context.SaveChanges();

                //3. Upload files
                updateDocument(oDocument);

                string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
                string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
                if (File.Exists(sPDFFileName) && oDocument.crc32 != 0)
                {
                    var crc32 = new Crc32();
                    var fs = File.Open(sPDFFileName, FileMode.Open);
                    crc32.Append(fs);
                    byte []hashBytes = crc32.GetCurrentHash();
                    oDocument.crc32 = BitConverter.ToUInt32(hashBytes, 0); 
                    fs.Close();
                }


                //DocumentConvertor.indexDocument(oDocument);

                oJob.Progress = 100;
                oJob.Status = Constants.JobProcessingStatus.Completed.ToString();

                _context.Update(oDocument);
                _context.Update(oJob);
                _context.SaveChanges();




            }

            return true;

        }
        */

        public bool updateDocument(document oDocument)
        {
            DCS3Services oDCS3Services = new DCS3Services();
            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);
            string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("/{0}/{1}/{2}", sPublisherName, sPublicationName,
                oDocument.Id);
            oDCS3Services.uploadDirectory(sBucketName, sOutputDirectory, sKeyPrefix);
            return true;
        }
        public bool downloadFilesForDelete(document oDocument, string sBucketName, int nFromPage, int nToPage, job oProgressJob = null)
        {
            //PDF,Image,Jpg,json,Svg?

            //string sS3PDFFileLocation;
            //Download all the XML's and the PDF Files.

            //Download the PDF File.
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                oDocument.Id);

            //PDF
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            string sHTMLDocumentPath = Path.Combine(sDocumentPath, "html");
            string sPDFKeyName = string.Format("{0}/{1}", sKeyPrefix, oDocument.PDFFileName);
            string sPDFFileName = Path.Combine(sDocumentPath, oDocument.PDFFileName);
            bool CRCMatch = false;
            if (File.Exists(sPDFFileName) && oDocument.crc32 != 0)
            {
                var crc32 = new Crc32();
                var fs = File.Open(sPDFFileName, FileMode.Open);
                crc32.Append(fs);
                byte[] hashBytes = crc32.GetCurrentHash();
                uint hash = BitConverter.ToUInt32(hashBytes, 0); 
                if (oDocument.crc32 == hash)
                {
                    CRCMatch = true;

                }
                fs.Close();
                //Check the file CRC32 

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

            DCS3Services oDCS3Services = new DCS3Services();
            if (!File.Exists(sPDFFileName))
                oDCS3Services.downloadFile(sBucketName, sPDFKeyName, sPDFFileName);

            //Json
            string sDocumentJsonKey = string.Format("{0}/{1}", sKeyPrefix, "document.json");
            string sDocumentJsonFile = Path.Combine(sDocumentPath, "document.json");

            oDCS3Services.downloadFile(sBucketName, sDocumentJsonKey, sDocumentJsonFile);

            //Images,SVG and Thumbnail.

            string sOutputDirectory = DocumentUtilBase.getDocumentPath(oDocument);


            int nPageNumber = nFromPage;
            int nNumberOfPagesToRemove = nToPage - nPageNumber + 1;

            int nPageCount = oDocument.NumberOfPages;
            int nDownloadTotal = Math.Max(1, nPageCount - nPageNumber);
            int nProgressStep = Math.Max(1, nDownloadTotal / 10);
            for (int i = 0; i < nPageCount - nPageNumber; i++)
            {
                // Crawl the bar from 5..14 across the (potentially long) per-page download so it
                // doesn't sit at 0 while ~4 files/page are pulled from S3.
                if (oProgressJob != null && i % nProgressStep == 0)
                    UpdateProgress(oProgressJob, 5 + (int)((double)i / nDownloadTotal * 9));
                int nCurrentPage = nPageNumber + nNumberOfPagesToRemove + i;
                int nNewCurrentPage = nPageNumber + i;
                string sName = string.Format("Page_{0}.json", nCurrentPage);
                string sJsonFileToDownload = Path.Combine(sDocumentPath, sName);
                string sJsonFileToDownloadKey = string.Format("{0}/{1}", sKeyPrefix, sName);
                oDCS3Services.downloadFile(sBucketName, sJsonFileToDownloadKey, sJsonFileToDownload);



                sName = string.Format("Thumbnail_{0}.jpg", nCurrentPage);
                string sThumbFileToDownload = Path.Combine(sDocumentPath, sName);
                string sThumbDownloadKey = string.Format("{0}/{1}", sKeyPrefix, sName);
                oDCS3Services.downloadFile(sBucketName, sThumbDownloadKey, sThumbFileToDownload);


                sName = string.Format("ZPage_{0}.jpg", nCurrentPage);
                string sImageFileToDownload = Path.Combine(sDocumentPath, sName);
                string sImageDownloadKey = string.Format("{0}/{1}", sKeyPrefix, sName);
                oDCS3Services.downloadFile(sBucketName, sImageDownloadKey, sImageFileToDownload);


                sName = string.Format("SPage_{0}.svg", nCurrentPage);
                string sSVGImageFileToDownload = Path.Combine(sDocumentPath, sName);
                string sSVGImageDownloadKey = string.Format("{0}/{1}", sKeyPrefix, sName);
                oDCS3Services.downloadFile(sBucketName, sSVGImageDownloadKey, sSVGImageFileToDownload);



            }




            return true;
        }

        public bool downloadFiles(document oDocument, string sBucketName)
        {
            //string sS3PDFFileLocation;
            //Download all the XML's and the PDF Files.


            //Download the PDF File.
            string sPublisherName = Utility.GenerateFriendlyURL(oDocument.Publication.Publisher.Name);
            string sPublicationName = Utility.GenerateFriendlyURL(oDocument.Publication.Name);
            string sKeyPrefix = string.Format("{0}/{1}/{2}", sPublisherName, sPublicationName,
                oDocument.Id);
            string sDocumentPath = DocumentUtilBase.getDocumentPath(oDocument);
            if (!Directory.Exists(sDocumentPath))
                Directory.CreateDirectory(sDocumentPath);
            else
            {
                Directory.Delete(sDocumentPath, true);
                Directory.CreateDirectory(sDocumentPath);

            }

            string sPDFKeyName = string.Format("{0}/{1}", sKeyPrefix, oDocument.PDFFileName);
            DCS3Services oDCS3Services = new DCS3Services();
            oDCS3Services.downloadDirectory(sBucketName, sKeyPrefix, sDocumentPath);
            return true;

        }


    }

}
