using Amazon.Util.Internal;
using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using DocumentFormat.OpenXml.Wordprocessing;
using Hangfire.Logging;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Asn1.IsisMtt.X509;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace JobWorker.Jobs
{
    public class JobExecutionRecognizeLinks : IJobExecution
    {
        private readonly ApplicationDbContext _context;
        public JobExecutionRecognizeLinks(ApplicationDbContext context)
        {
            _context = context;
        }
        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {

            return true;
        }


        public void recognizeLinksFromLocal(recognizelinksinput oRecognizeLinksInput)
        {
           
            //bool bRet = false;
            int nProcessID = 0;
            try
            {

                DCS3Services oDCS3Services = new DCS3Services();
                string sTempPath = _context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;
                string sLocalJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");
                oDCS3Services.downloadFileByURL(oRecognizeLinksInput.RecognizeLinkXml, sLocalJobFile);

                string sProcessName = System.IO.Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
                Process oPDFProcess = new Process();
                ProcessStartInfo startInfo = new ProcessStartInfo(sProcessName);

                startInfo.Arguments = sLocalJobFile;
                oPDFProcess.StartInfo = startInfo;
                startInfo.WindowStyle = ProcessWindowStyle.Hidden;
                oPDFProcess.Start();
                int nTimeout = 60000;
                oPDFProcess.WaitForExit(nTimeout);
                int nExitCode = oPDFProcess.ExitCode;

                //oPDFProcess.OutputDataReceived += new DataReceivedEventHandler(OutputDataHandler);
                //oPDFProcess.ErrorDataReceived += new DataReceivedEventHandler(ErrorDataHandler);

                oPDFProcess.Start();

                // Start the asynchronous read of the standard output stream.
                //oPDFProcess.BeginOutputReadLine();
                //oPDFProcess.BeginErrorReadLine();
                nProcessID = oPDFProcess.Id;
                oPDFProcess.WaitForExit(nTimeout);
            }
            catch (Exception ex)
            {
                
                if (oRecognizeLinksInput.Job != null)
                {
                    //oJob.Status = Constants.JobProcessingStatus.Failed;
                    oRecognizeLinksInput.Job.Desctiption = ex.Message;
                }
                //Logger.log.Debug("Exception when recognizing links", ex);
            }

            //return output.ToString();
        }

   


        protected string sDocumentId = "";

      

        
    }
}
