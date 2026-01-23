using core;
using Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    public class ReplacePagesJob : IJobExecution
    {
        private readonly ILogger<ReplacePagesJob> _log;

        public ReplacePagesJob(ILogger<ReplacePagesJob> log)
        {
            _log = log;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            _log.LogInformation("ReplacePages job {JobId}", oJob.Id);

            // Your real processing logic here
            await Task.Delay(200, ct);

            return true;
        }
    }
}
