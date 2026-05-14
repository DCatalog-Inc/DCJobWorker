using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;

namespace JobWorker.Jobs
{
    public class OdlJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<OdlJobWorker> _log;
        private readonly IEmailSender _emailSender;
        private readonly IConfiguration _config;
        private readonly IHostEnvironment _hostEnvironment;

        public OdlJobWorker(IDbContextFactory<ApplicationDbContext> dbFactory, ILogger<OdlJobWorker> log, IEmailSender emailSender, IConfiguration config, IHostEnvironment hostEnvironment)
        { _dbFactory = dbFactory; _log = log; _emailSender = emailSender; _config = config; _hostEnvironment = hostEnvironment; }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
            _log.LogInformation("OdlJob {JobId} method={Desc}", oJob.Id, oJob.Desctiption);
            var bridge = new HostEnvironmentBridge(_hostEnvironment);
            var logger = _log as ILogger<OdlJob> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<OdlJob>.Instance;
            var dcJob = new OdlJob(ctx, _emailSender, logger, _config, bridge);

            switch (oJob.Desctiption?.Trim().ToLowerInvariant())
            {
                case "nocaulk":           await dcJob.addProductsNoCaulk(); break;
                case "canada":            await dcJob.addProductsCanada(); break;
                case "canadafrench":      await dcJob.addProductsCanadaFrench(); break;
                case "westernreflections":await dcJob.addProductsWesternReflections(); break;
                case "severeweather":     await dcJob.addProductsSevereWeather(); break;
                case "westernreflectionssevereweather": await dcJob.addProductsWesternReflectionsSevereWeather(); break;
                case "usdoorstaging":     await dcJob.addProductsUsDoorStaging(); break;
                case "validation":        dcJob.odlGenerateValidationProductsReport(); break;
                default:                  await dcJob.addProductsUSA(); break;
            }
            return true;
        }
    }

    internal class HostEnvironmentBridge : Microsoft.AspNetCore.Hosting.IWebHostEnvironment
    {
        private readonly IHostEnvironment _env;
        public HostEnvironmentBridge(IHostEnvironment env) => _env = env;
        public string WebRootPath { get => string.Empty; set { } }
        public IFileProvider WebRootFileProvider { get => new NullFileProvider(); set { } }
        public string EnvironmentName { get => _env.EnvironmentName; set { } }
        public string ApplicationName { get => _env.ApplicationName; set { } }
        public string ContentRootPath { get => _env.ContentRootPath; set { } }
        public IFileProvider ContentRootFileProvider { get => _env.ContentRootFileProvider; set { } }
    }
}
