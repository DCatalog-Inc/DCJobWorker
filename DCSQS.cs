using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Xml;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System.Threading;
using System.Configuration;
using DCCore;
using Core.Models;
using Core;
using Amazon.SQS;
using Amazon.SQS.Model;
using System.Threading.Tasks;
using System.Linq;
using Amazon.Runtime;
using DCatalogCommon.Data;

namespace DCatalogCommon
{
    public class DCSQS
    {

        //For now we must call init to set the queue from the main thread!
        //Need to fix it in the future!
        private readonly ApplicationDbContext _context;
        private readonly IAmazonSQS _sqsclient;
        protected static string _sDemoQueueUrl = "";
        protected static string _sClientsQueueName = "";
        protected static string _sDistributedQueueName = "";
        protected static string _sDistributedHPQueueName = "";

        public DCSQS(ApplicationDbContext context,IAmazonSQS amazonSQS)
        {
            _context = context;
            _sqsclient = amazonSQS;
            
        }

        public  IAmazonSQS getDefaultSQS()
        {
            
            return _sqsclient;
        }
        public async Task<string> getURLByName(string sQueueName)
        {
            IAmazonSQS sqs = getDefaultSQS();
            GetQueueUrlRequest oQueueURLRequest = new GetQueueUrlRequest();
            oQueueURLRequest.QueueName = sQueueName;
            GetQueueUrlResponse oQueueUrlResponse = await sqs.GetQueueUrlAsync(oQueueURLRequest);
            string sClientsQueueUrl = oQueueUrlResponse.QueueUrl;
            return sClientsQueueUrl;
        }

        //_sDistributedQueueName


        public async Task<string> getDistributedQueueNewUrl()
        {
            if (_sDistributedQueueName == "")
            {
                string sDistributedQueueName = _context.serversettings.FirstOrDefault(x => x.Name == "Distributed_Jobs_Queue_Testing").Value;
                _sDistributedQueueName = await getURLByName(sDistributedQueueName);
            }
            return _sDistributedQueueName;
        }
        public async Task<string> getDistributedQueueUrl()
        {
            if (_sDistributedQueueName == "")
            {
                string sDistributedQueueName = _context.serversettings.FirstOrDefault(x => x.Name == "Distributed_Jobs_Queue").Value;
                _sDistributedQueueName = await getURLByName(sDistributedQueueName);
            }
            return _sDistributedQueueName;
        }

        public async Task<string> getDistributedHPQueueUrl()
        {
            if (_sDistributedHPQueueName == ""){
                string sDistributedHPQueueName = _context.serversettings.FirstOrDefault(x => x.Name == "Distributed_Jobs_Queue_HP").Value;
                _sDistributedHPQueueName = await getURLByName(sDistributedHPQueueName);
            }
            return _sDistributedHPQueueName;
        }


        public async Task<string> getDemosURL()
        {
            if (_sDemoQueueUrl == "")
            {
                string sDemoQueueName = _context.serversettings.FirstOrDefault(x => x.Name == "DemoQueueName").Value; 
                _sDemoQueueUrl = await getURLByName(sDemoQueueName);
            }
            return _sDemoQueueUrl;
        }
        public async Task<string> getClientsURL()
        {
            if (_sClientsQueueName == "")
            {
                string sClientsQueueName = _context.serversettings.FirstOrDefault(x => x.Name == "ClientsQueueName").Value;
                _sClientsQueueName = await getURLByName(sClientsQueueName);
            }
            return _sClientsQueueName;
        }

        //When use s3 is on we can process the files on any server.
        public async void addJob(string sQueURL, string sJobID)
        {
         
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.QueueUrl = sQueURL; //URL from initial queue creation
            string sMessage = string.Format("<pr><job><id>{0}</id><timestamp>{1}</timestamp></job></pr>", sJobID, DateTime.UtcNow.ToLongTimeString());
            sendMessageRequest.MessageBody = sMessage; //Todo build an XML message
            //_sqsclient.Config.
            SendMessageResponse smr = await _sqsclient.SendMessageAsync(sendMessageRequest);
            //smr.SendMessageResult
        }

        public async Task addJobToQueue(job oJob,Constants.JobQueueName eJobQueueName)
        {
            string sQueueURL;
            switch (eJobQueueName)
            {
                case Constants.JobQueueName.DemoQueue:
                    sQueueURL = await getDemosURL();
                    break;
                case Constants.JobQueueName.ClientQueue:
                    sQueueURL = await getClientsURL();
                    break;
                case Constants.JobQueueName.DistributedClientQueue:
                    sQueueURL = await getDistributedQueueUrl();
                    break;
                case Constants.JobQueueName.DistributedHPClientQueue:
                    sQueueURL = await getDistributedHPQueueUrl();
                    break;
                default:
                    sQueueURL = await getDistributedQueueUrl();
                    break;
            }
            addJob(sQueueURL, oJob.Id.ToString());
        }



        public async void deleteMessage(String messageRecieptHandle, string sQueueURL)
        {
            //Deleting a message
            Console.WriteLine("Deleting the message.\n");
            DeleteMessageRequest deleteRequest = new DeleteMessageRequest();
            deleteRequest.QueueUrl = sQueueURL;
            deleteRequest.ReceiptHandle = messageRecieptHandle;
            await _sqsclient.DeleteMessageAsync(deleteRequest);

        }
        public void listMessages()
        {
        }
        
    };
}