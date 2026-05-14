using Amazon.SQS;
using core;
using Core.Models;
using Core.Services;
using DCatalogCommon.Data;
using DCJobs;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JobWorker.Jobs
{
    public class FreeTrialJobWorker : IJobExecution
    {
        private readonly IDbContextFactory<ApplicationDbContext> _dbFactory;
        private readonly ILogger<FreeTrialJobWorker> _log;
        private readonly IEmailSender _emailSender;
        private readonly ISmsSender _smsSender;
        private readonly IConfiguration _configuration;
        private readonly IAmazonSQS _sqsClient;
        private readonly IServiceProvider _sp;

        public FreeTrialJobWorker(
            IDbContextFactory<ApplicationDbContext> dbFactory,
            ILogger<FreeTrialJobWorker> log,
            IEmailSender emailSender,
            ISmsSender smsSender,
            IConfiguration configuration,
            IAmazonSQS sqsClient,
            IServiceProvider sp)
        {
            _dbFactory = dbFactory;
            _log = log;
            _emailSender = emailSender;
            _smsSender = smsSender;
            _configuration = configuration;
            _sqsClient = sqsClient;
            _sp = sp;
        }

        public async Task<bool> ExecuteAsync(job oJob, CancellationToken ct = default)
        {
            _log.LogInformation("FreeTrialJobWorker job {JobId}", oJob.Id);

            await using var context = await _dbFactory.CreateDbContextAsync(ct);

            UserManager<ApplicationUser>? userManager = null;
            try
            {
                userManager = _sp.GetService<UserManager<ApplicationUser>>();
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "FreeTrialJobWorker: could not resolve UserManager<ApplicationUser> from service provider; proceeding with null");
            }

            if (userManager == null)
            {
                _log.LogWarning("FreeTrialJobWorker: UserManager<ApplicationUser> is not registered in this host. " +
                    "The trial account check (which uses IsInRoleAsync) will be skipped. " +
                    "Register UserManager via AddIdentity/AddDefaultIdentity to enable full functionality.");
            }

            var dcJobLogger = _log as ILogger<RubiesJob>
                ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RubiesJob>.Instance;

            var dcJob = new FreeTrialJob(
                userManager,
                context,
                _emailSender,
                _smsSender,
                dcJobLogger,
                _configuration,
                _sqsClient);

            await dcJob.ExecuteJobAsync();

            return true;
        }
    }
}
