#pragma warning disable 0436
using Core;
using DCatalogCommon.Data;
using Core.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using core.Models.Convertor;
using Microsoft.EntityFrameworkCore;

namespace core.Common
{
    public class PublicationUtil
    {
        //make sure to load publihser also. 
        public static string getObjectKey(publication pub, string key)
        {
            string templatename = Utility.GenerateFriendlyURL(pub.Name);
            string PublisherFolderName = Utility.GenerateFriendlyURL(pub.Publisher.Name);
            string objectKey = PublisherFolderName + "/" + templatename + "/" + key;
            return objectKey;
        }

        public static int GetActiveDocumentCountAsync(
        ApplicationDbContext db,
        string publicationId,
        CancellationToken ct = default)
        {
            // If your 'document' entity has a FK property like 'PublicationId' (recommended):
            var q = db.document
                .AsNoTracking()
                .Where(d =>
                    d.IsActive &&
                    d.ShowInArchive &&
                    !d.Deleted &&
                    d.DocumentStatus == Constants.JobProcessingStatus.Completed.ToString() &&
                    d.Publication.Id == publicationId);

            // If you DON'T have a FK scalar (less efficient), use this instead:
            // var q = db.document
            //     .AsNoTracking()
            //     .Where(d =>
            //         d.IsActive &&
            //         d.ShowInArchive &&
            //         !d.Deleted &&
            //         d.DocumentStatus == Constants.JobProcessingStatus.Completed &&
            //         d.Publication.Id == pubId);
            return  q.Count();
        }

        public static string getPreloaderByPublication(publication pub)
        {
            publicationtemplate oPublicationTemplate = pub.PublicationTemplate;
            string sPreloaderText = "{\"preloader\": {\"@attributes\": $sPreloaderAttributes}}";
            //sDocumentID = getParam("docid", "", false, "Missing docid paramter");

            JSONPreloaderResult oJSONPreloaderResult = new JSONPreloaderResult();
            oJSONPreloaderResult.preloader_effect = "blind";
            if (oPublicationTemplate.logoImgFile != null && oPublicationTemplate.logoImgFile != "")
                oJSONPreloaderResult.preloader_logourl = oPublicationTemplate.logoImgFile.Replace("_toolbar", "");
            oJSONPreloaderResult.preloader_type = "loader1";
            oJSONPreloaderResult.preloader_startcolor = oPublicationTemplate.preloaderstartcolor != null ? oPublicationTemplate.preloaderstartcolor.PadLeft(6, '0') : "FFFFFF";
            oJSONPreloaderResult.preloader_endcolor = oPublicationTemplate.preloaderendcolor != null ? oPublicationTemplate.preloaderendcolor.PadLeft(6, '0') : "FFFFFF";
            oJSONPreloaderResult.preloader_footer1 = oPublicationTemplate.footerText;
            oJSONPreloaderResult.preloader_footer2 = "";
            oJSONPreloaderResult.preloader_showfooter = oPublicationTemplate.showfooter ? "1" : "0";
            var sPreloaderAttributes = JsonConvert.SerializeObject(oJSONPreloaderResult);
            sPreloaderText = sPreloaderText.Replace("$sPreloaderAttributes", sPreloaderAttributes);
            return sPreloaderText;

        }
        public static string getPreloader(document doc)
        {
            return getPreloaderByPublication(doc.Publication);

        }

        public static string getObjectKey(publication pub)
        {
            string templatename = Utility.GenerateFriendlyURL(pub.Name);
            string PublisherFolderName = Utility.GenerateFriendlyURL(pub.Publisher.Name);
            string objectKey = PublisherFolderName + "/" + templatename;
            return objectKey;
        }

        public static string getPublicationPath(publication pub)
        {
            string sDocumentPath;
            string sRepository = DCCommon.Instance.RepositoryLocation;
            string templatename = Utility.GenerateFriendlyURL(pub.Name);
            string PublisherFolderName = Utility.GenerateFriendlyURL(pub.Publisher.Name);
            sDocumentPath = System.IO.Path.Combine(sRepository, PublisherFolderName, templatename);
            return sDocumentPath;
        }

        public static string getPublicationPath(publication pub,string sRepository)
        {
            string sDocumentPath;

            string templatename = Utility.GenerateFriendlyURL(pub.Name);
            string PublisherFolderName = Utility.GenerateFriendlyURL(pub.Publisher.Name);
            sDocumentPath = System.IO.Path.Combine(sRepository, PublisherFolderName, templatename);
            return sDocumentPath;
        }

        public static string DownloadSettingsFile(publication pub, ApplicationDbContext context, DCS3Services dcs3services, string bucketName)
        {
            string objectkey = getObjectKey(pub, "settings.json");
            string sRepository =  DCCommon.Instance.RepositoryLocation;
            string TemplatePath = getPublicationPath(pub, sRepository);
            string localfile = Path.Combine(TemplatePath, "settings.json");
            dcs3services.downloadFile(bucketName, objectkey, localfile);
            return localfile;
        }

        public static string DownloadPreloaderFile(publication pub, ApplicationDbContext context, DCS3Services dcs3services, string bucketName)
        {
            string objectkey = getObjectKey(pub, "preloader.json");
            string sRepository = DCCommon.Instance.RepositoryLocation;
            string TemplatePath = getPublicationPath(pub, sRepository);
            string localfile = Path.Combine(TemplatePath, "preloader.json");
            dcs3services.downloadFile(bucketName, objectkey, localfile);
            return localfile;
        }




    }
}
