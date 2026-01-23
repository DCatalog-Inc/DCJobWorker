using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Hangfire.Logging;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Org.BouncyCastle.Asn1.IsisMtt.X509;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    public class JobExecutionImportNotes : IJobExecution
    {
        private readonly ApplicationDbContext _context;
        public JobExecutionImportNotes(ApplicationDbContext context)
        {
            _context = context;
        }
        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {

            try
            {



                var importnotes = await _context.importnotesinput
                                 .Include(c => c.Job)
                                 .Where(c => c.Job.Id == oJob.Id)
                                 .FirstOrDefaultAsync();
              
                importNotes(importnotes);
                oJob.Progress = 100;
                oJob.Status = Constants.JobProcessingStatus.Completed.ToString();
                _context.Update(oJob);

            }
            catch (Exception e)
            {
                //_log.LogError("Error when adding bookmarks " + e.Message.ToString());
                //_log.LogError("Job id  " + oJob.Id);

            }


            return true;
        }

        protected string sDocumentId = "";

        public void importNotes(importnotesinput oImportNotesInput)
        {
            if (!File.Exists(oImportNotesInput.InputFileName))
                return;

            string output_path = oImportNotesInput.OutputDirectory;
            if (output_path.EndsWith("\\") == false)
                output_path += "\\";

            String command = string.Format("show \"{0}\" importnotes \"{1}document.json\"", oImportNotesInput.InputFileName, output_path);
            string exePath = System.IO.Path.Combine(AppContext.BaseDirectory, "Tools", "dcmutool", "dcmutool.exe");

            ProcessStartInfo cmdsi = new ProcessStartInfo(exePath);
            cmdsi.Arguments = command;
            Process cmd = Process.Start(cmdsi);
            cmd.WaitForExit();

        }
      

        public static void UpdateTOCType(string output_path)
        {
            string sDocumentJson = Path.Combine(output_path, "document.json");
            DocumentConvertor dc = new DocumentConvertor();
            if (sDocumentJson != "" && File.Exists(sDocumentJson))
            {
                using (StreamReader r = new StreamReader(sDocumentJson))
                {
                    string json = r.ReadToEnd();
                    try
                    {
                        var result = JsonConvert.DeserializeObject<dynamic>(json);
                        if (result["issue"]["TOC"] != null && result["issue"]["TOC"].Count > 0)
                        {
                            if (result["issue"]["@attributes"]["toc_type"] == null)
                            {
                                result["issue"]["@attributes"]["toc_type"] = "2";
                            }
                            else
                            {
                                result["issue"]["@attributes"]["toc_type"].Value = "2";
                            }
                            r.Close();
                            System.IO.File.WriteAllText(sDocumentJson, result.ToString());
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                }
            }
        }

        
    }
}
