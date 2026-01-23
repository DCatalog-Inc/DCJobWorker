using Amazon.Util.Internal;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace JobWorker.Jobs
{
    public class SearchProductsInDocument
    {
        protected string sDocumentId = "";
        protected string sPubliactionId = "";
        protected int m_nTimeout = 7200000;
        protected string m_sProcessName = "dcproxy.exe";
        private static StringBuilder output;
        protected int nFromPage = 1;
        protected int nToPage = 0;
        protected bool bUseRange = false;
        protected string sBucketName = Constants.DEFAULT_DOCS_LOCATION;
        protected string sKeyPrefix = "";
        protected string sDocumentPath = "";
        protected bool usejson = true;
        protected bool importproductstodb = false;
        protected bool updatecatalogs = false;

        protected string generateJobFile(string sTempPath,searchproductsindocumentinput oSearchProductsInDocumentInput)
        {
            string sJobFile = Path.Combine(sTempPath, Guid.NewGuid().ToString() + ".xml");


            XmlDocument oLinksParams = new XmlDocument();
            XmlDeclaration xmlDeclaration = oLinksParams.CreateXmlDeclaration("1.0", "utf-8", null);
            //Create the root element
            XmlElement rootNode = oLinksParams.CreateElement("job");
            rootNode.SetAttribute("name", "SearchProductsInDocument");

            oLinksParams.InsertBefore(xmlDeclaration, oLinksParams.DocumentElement);
            oLinksParams.AppendChild(rootNode);

            XmlElement inputfilexml = oLinksParams.CreateElement("inputfile");
            inputfilexml.InnerText = "";
            rootNode.AppendChild(inputfilexml);

            XmlElement outputdir = oLinksParams.CreateElement("outputdir");
            outputdir.InnerText = oSearchProductsInDocumentInput.OutputDirectory;
            rootNode.AppendChild(outputdir);

            XmlElement documentidxml = oLinksParams.CreateElement("docid");
            documentidxml.InnerText = oSearchProductsInDocumentInput.Document.Id.ToString();
            rootNode.AppendChild(documentidxml);


            XmlElement usejson = oLinksParams.CreateElement("usejson");
            usejson.InnerText = "1";
            rootNode.AppendChild(usejson);

            Guid oNewJobId = Guid.NewGuid();

            rootNode.SetAttribute("id", oNewJobId.ToString());

            oLinksParams.Save(sJobFile);
            return sJobFile;
        }

        private static void OutputDataHandler(object sendingProcess, DataReceivedEventArgs outLine)
        {
            if (!String.IsNullOrEmpty(outLine.Data))
            {
                output.Append("\n" + outLine.Data);
            }
        }

        private static void ErrorDataHandler(object sendingProcess, DataReceivedEventArgs errLine)
        {
        }

        public string executeSearchProducts(ApplicationDbContext context,searchproductsindocumentinput osearchproductsindocumentinput)
        {
            output = new StringBuilder();

            //bool bRet = false;
            int nProcessID = 0;
            try
            {
                jobtype oJobType = new jobtype();
                oJobType.Name = "JobSearchProductsInDocument";
                job oCurrentJob = new job(oJobType);
                oCurrentJob.CreationTime = DateTime.Now;
                oCurrentJob.Desctiption = "Waiting For Processing";
                oCurrentJob.Priority = 1;
                oCurrentJob.Progress = 0;
                oCurrentJob.Status = Constants.JobProcessingStatus.Waiting.ToString();

                context.Update(oCurrentJob);

                string sTempPath = context.serversettings.FirstOrDefault(x => x.Name == "TempPath").Value;

                string sFileName = generateJobFile(sTempPath,osearchproductsindocumentinput);
                Process oPDFProcess = new Process();
                string exePath = System.IO.Path.Combine(AppContext.BaseDirectory, "Tools", "dcproxy", "dcproxy.exe");
                ProcessStartInfo startInfo = new ProcessStartInfo(exePath);
                startInfo.Arguments = sFileName;
                startInfo.CreateNoWindow = true;
                startInfo.RedirectStandardError = false;
                startInfo.RedirectStandardOutput = false;
                startInfo.CreateNoWindow = false;
                startInfo.UseShellExecute = false;
                oPDFProcess.StartInfo = startInfo;
                oPDFProcess.EnableRaisingEvents = false;

                startInfo.WindowStyle = ProcessWindowStyle.Hidden;

                oPDFProcess.OutputDataReceived += new DataReceivedEventHandler(OutputDataHandler);
                oPDFProcess.ErrorDataReceived += new DataReceivedEventHandler(ErrorDataHandler);

                oPDFProcess.Start();

                // Start the asynchronous read of the standard output stream.
                //oPDFProcess.BeginOutputReadLine();
                //oPDFProcess.BeginErrorReadLine();
                nProcessID = oPDFProcess.Id;
                oPDFProcess.WaitForExit(m_nTimeout);
                if (oPDFProcess.HasExited == false)
                {
                    //oPDFProcess.CancelErrorRead();
                    //oPDFProcess.CancelOutputRead();
                    oPDFProcess.CloseMainWindow();
                    // Free resources associated with process.

                    oPDFProcess.Close();
                    return output.ToString();
                }
                else if (oPDFProcess.ExitCode == 0)
                {

                    return output.ToString();
                }
                else
                {


                    oPDFProcess.CloseMainWindow();
                    //oPDFProcess.Close();
                    return output.ToString();
                }
            }
            catch (Exception ex)
            {
                //Logger.log.Error("Exception when recognizing links", ex);
            }

            return output.ToString();
        }

    }
}
