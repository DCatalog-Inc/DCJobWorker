using Amazon.S3;
using core;
using Core.Models;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace JobWorker.Jobs
{
    public class CopyLinksJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<CopyLinksJobWorker> _log;
        private readonly IConfiguration _config;
        private readonly IAmazonS3 _s3Client;
        private readonly ILoggerFactory _loggerFactory;

        public CopyLinksJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<CopyLinksJobWorker> log,
            IConfiguration config,
            IAmazonS3 s3Client,
            ILoggerFactory loggerFactory)
        {
            _dbFactory = dbFactory;
            _log = log;
            _config = config;
            _s3Client = s3Client;
            _loggerFactory = loggerFactory;
        }

        private class CopyLinksParams
        {
            [JsonProperty("srcDocId")]
            public string SrcDocId { get; set; }
            [JsonProperty("dstDocId")]
            public string DstDocId { get; set; }
            [JsonProperty("srcPageStart")]
            public int SrcPageStart { get; set; }
            [JsonProperty("srcPageEnd")]
            public int SrcPageEnd { get; set; }
            [JsonProperty("dstPageStart")]
            public int DstPageStart { get; set; }
            [JsonProperty("linkTypesStr")]
            public string LinkTypesStr { get; set; }
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("CopyLinksJobWorker job {JobId}", oJob.Id);

            if (string.IsNullOrWhiteSpace(oJob.Desctiption))
            {
                _log.LogWarning("CopyLinksJobWorker: Desctiption is null/empty for job {JobId}", oJob.Id);
                return false;
            }

            CopyLinksParams parms;
            try
            {
                parms = JsonConvert.DeserializeObject<CopyLinksParams>(oJob.Desctiption);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "CopyLinksJobWorker: failed to parse Desctiption JSON for job {JobId}", oJob.Id);
                return false;
            }

            if (parms == null || string.IsNullOrEmpty(parms.SrcDocId) || string.IsNullOrEmpty(parms.DstDocId))
            {
                _log.LogWarning("CopyLinksJobWorker: invalid parameters in Desctiption for job {JobId}", oJob.Id);
                return false;
            }

            ILogger<CopyLinksJob> copyLinksLogger = _loggerFactory.CreateLogger<CopyLinksJob>();
            var dcJob = new CopyLinksJob(ctx, copyLinksLogger, _config, _s3Client);
            await dcJob.CopyLinksAsync(
                oJob.Id,
                parms.SrcDocId,
                parms.DstDocId,
                parms.SrcPageStart,
                parms.SrcPageEnd,
                parms.DstPageStart,
                parms.LinkTypesStr ?? string.Empty);

            return true;
        }
    }
}
