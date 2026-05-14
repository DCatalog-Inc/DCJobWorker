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
using Serilog;
using Hangfire.Logging;
using Microsoft.AspNetCore.Http;
using System.Collections;

namespace JobWorker.Jobs
{
    public class JobExecutionAddPages : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<JobExecutionAddPages> _log;
        public JobExecutionAddPages(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<JobExecutionAddPages> log)
        {
            _dbFactory = dbFactory;
            _log = log;
        }
        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var context = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("JobExecutionAddPages job {JobId}", oJob.Id);
            return true;
        }
    }
}
