using Core;
using Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobWorker
{
    public class PostConversionCommunication
    {
        protected string _sCallbackURL ="";
        protected string _sErrorString ="";
        protected string _sOutputURL = "";
        protected string _sOrderID = "";
        protected string _sPublicationID = "";
        protected string _sDocumentId = "";
        public PostConversionCommunication(){
        }
        public async Task<string> notifyAdminPanel(document doc)
        {
            PostSubmitter post = new PostSubmitter();
            string CallbackURL = "https://admin.dcatalog.com/api/DocumentCompleted";
            post.Url = CallbackURL;
            post.PostItems.Add("docid", doc.Id.ToString());
            post.PostItems.Add("docstatus", doc.DocumentStatus.ToString());
            post.PostItems.Add("docstatusdesc", doc.DocumentProcessingDescription.ToString());
            post.Type = PostSubmitter.PostTypeEnum.Post;
            string result = await post.Post();
            return result;
        }



        public async Task<string> invokeCallbackMethod()
        {
            PostSubmitter post = new PostSubmitter();

            post.Url = CallbackURL;
            post.PostItems.Add("ErrorString", ErrorString);
            post.PostItems.Add("OutputURL", OutputURL);
            post.PostItems.Add("OrderID", OrderID);
            post.PostItems.Add("PublicationID", PublicationID);
            post.PostItems.Add("DocumentID", DocumentID);
            post.Type = PostSubmitter.PostTypeEnum.Post;
            string result = await post.Post();
            return result;
        }

        public string CallbackURL
        {
            get
            {
                return _sCallbackURL;
            }
            set
            {
                _sCallbackURL = value;
            }
        }
        public string ErrorString
        {
            get
            {
                return _sErrorString;
            }
            set
            {
                _sErrorString = value;
            }
        }
        public string OutputURL
        {
            get
            {
                return _sOutputURL;
            }
            set
            {
                _sOutputURL = value;
            }
        }
        public string OrderID
        {
            get
            {
                return _sOrderID;
            }
            set
            {
                _sOrderID = value;
            }
        }
        public string PublicationID
        {
            get
            {
                return _sPublicationID;
            }
            set
            {
                _sPublicationID = value;
            }
        }

        public string DocumentID
        {
            get
            {
                return _sDocumentId;
            }
            set
            {
                _sDocumentId = value;
            }

        }
    }
}
