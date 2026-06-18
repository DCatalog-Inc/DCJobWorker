using Amazon.S3;
using Amazon.SQS;
using Core;
using Core.Services;
using DCatalogCommon.Data;
using JobWorker.Jobs;
using JobWorker.Services;
using Microsoft.EntityFrameworkCore;
using iText.Licensing.Base;
using Serilog;


Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File("logs/jobworker-.log",
        rollingInterval: RollingInterval.Day)
    .CreateLogger();
//
// 1. Fetch secrets first (async before Host is built)
//
Log.Information("Starting JobWorker...");
SecretsManagerService.Initialize("us-east-1");
var secrets = SecretsManagerService.Instance;

MySqlConfig mysqlConfig = await secrets.GetMySqlConfigAsync("MySqlDCServices");

var SecretString = await secrets.GetSecretValueAsync("IRONPDFKey", "IRONPDFKEY");
IronPdf.License.LicenseKey = SecretString;

try
{
    var itextJson = await secrets.GetRawSecretAsync("iTextKey");
    using var ms = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(itextJson));
    LicenseKey.LoadLicenseFile(ms);
    Log.Information("iText license loaded from Secrets Manager.");
}
catch (Exception ex)
{
    var fallbackFile = new FileInfo("secrets/itextkey.json");
    if (fallbackFile.Exists)
    {
        try { LicenseKey.LoadLicenseFile(fallbackFile); Log.Information("iText license loaded from local file."); }
        catch (Exception ex2) { Log.Warning(ex2, "iText license file found but could not be loaded."); }
    }
    else
    {
        Log.Warning(ex, "iText license not available — PDF operations may be limited.");
    }
}

string connectionString =
    $"Server={mysqlConfig.Host};Port={mysqlConfig.Port};Database={mysqlConfig.Database};" +
    $"User Id={mysqlConfig.Username};Password={mysqlConfig.Password};" +
    "convert zero datetime=True;CharSet=utf8;Allow User Variables=true";

DCCommon.Instance.DefaultDBConnection = connectionString;

//
// 2. Build the host
//
var builder = Host.CreateDefaultBuilder(args)
    .ConfigureServices((ctx, services) =>
    {
        // Options
        services.Configure<QueueOptions>(ctx.Configuration.GetSection("Queues"));
        services.Configure<WorkerOptions>(ctx.Configuration.GetSection("Worker"));

        // DbContext
        services.AddDbContext<ApplicationDbContext>(options =>
            options.UseMySQL(connectionString,
                o => o.EnableRetryOnFailure(5, TimeSpan.FromSeconds(30), null)));
        // instead of AddDbContext<ApplicationDbContext>

        // factory shares the same options; just change lifetime to Scoped
        services.AddDbContextFactory<ApplicationDbContext>(
            lifetime: ServiceLifetime.Scoped);
        // Repository location
        try
        {
            var repositoryLocation1 = ctx.Configuration["RepositoryLocation"];

            string repositoryLocation2 = null;
            using (var scope = services.BuildServiceProvider().CreateScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
                repositoryLocation2 = db.serversettings
                    .FirstOrDefault(x => x.Name == "RepositoryLocation")?.Value;
            }

            DCCommon.Instance.RepositoryLocation =
                !string.IsNullOrEmpty(repositoryLocation1) ? repositoryLocation1 : repositoryLocation2;

            DCCommon.Instance.RepositoryLocationDB = repositoryLocation2;
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "Could not read RepositoryLocation from DB — using config fallback.");
            var fallback = ctx.HostingEnvironment.IsDevelopment()
                ? @"D:\DCatalog\Docs"
                : @"C:\DCatalog\Docs";

            DCCommon.Instance.RepositoryLocation ??= fallback;
            DCCommon.Instance.RepositoryLocationDB ??= DCCommon.Instance.RepositoryLocation;
        }

        // OpenSearch — initialize the static search endpoint used by document indexing
        // (DocumentConvertor.indexDocument in the convert pipeline). Without this,
        // ElasticSearchEngine builds new Uri(null) -> "Value cannot be null (uriString)".
        var searchUri = ctx.Configuration["OpenSearch:Endpoint"];
        if (!string.IsNullOrEmpty(searchUri))
        {
            ElasticSearchEngine.Init(searchUri);
            Log.Information("ElasticSearchEngine initialized: {Uri}", searchUri);
        }
        else
        {
            Log.Warning("OpenSearch:Endpoint not configured — document indexing will fail.");
        }

        // Email / SMS
        services.Configure<AuthMessageSenderOptions>(ctx.Configuration.GetSection("Email"));
        services.AddTransient<IEmailSender, JobWorkerMessageSender>();
        services.AddTransient<ISimpleEmailSender, JobWorkerMessageSender>();
        services.AddTransient<ISmsSender, JobWorkerMessageSender>();

        // AWS
        services.AddSingleton<IAmazonSQS>(_ => new AmazonSQSClient());
        services.AddSingleton<IAmazonS3>(_ => new AmazonS3Client());

        // Core job handlers (original)
        services.AddTransient<JobExecutionConvertPDF>();
        services.AddTransient<JobExecutionSaveLinksToCSV>();
        services.AddTransient<JobExecutionIntroPageWorker>();

        // Document management handlers
        services.AddTransient<ActivatePagesJobWorker>();
        services.AddTransient<ActivateEditionsJobWorker>();
        services.AddTransient<HtmlGenerateJobWorker>();
        services.AddTransient<ValidateIndexDocumentJobWorker>();
        services.AddTransient<CopyLinksJobWorker>();
        services.AddTransient<FreeTrialJobWorker>();
        services.AddTransient<AddToAILibraryJobWorker>();
        services.AddTransient<GeneratePDFJobWorker>();

        services.AddTransient<JobExecutionGenerateGifFlipbookWorker>();

        // Product import handlers
        services.AddTransient<AdessoJobWorker>();
        services.AddTransient<AirgasImportJobWorker>();
        services.AddTransient<CCMPJobWorker>();
        services.AddTransient<CeratizitJobWorker>();
        services.AddTransient<EverflowJobWorker>();
        services.AddTransient<HedelProductJobWorker>();
        services.AddTransient<JFProductImportJobWorker>();
        services.AddTransient<MaysZLProductJobWorker>();
        services.AddTransient<OdlJobWorker>();
        services.AddTransient<RubiesJobWorker>();
        services.AddTransient<SummitImportProductJobWorker>();

        // App services
        services.AddScoped<JobUtil>();

        services.AddScoped<JobProcessor>();
        services.AddHostedService<SqsWorker>();
    })
    .ConfigureLogging(lb => lb.ClearProviders().AddConsole());

await builder.Build().RunAsync();

//
// 3. Options classes
//
public sealed class QueueOptions
{
    public string? DistributedHighPriority { get; set; }
    public string? Distributed { get; set; }
    public string? Clients { get; set; }
    public string? Demos { get; set; }
}

public sealed class WorkerOptions
{
    public int MaxConcurrency { get; set; } = 3;
    public int VisibilityTimeoutSeconds { get; set; } = 120;
    public int LongPollSeconds { get; set; } = 10;
    public int EmptyPollDelayMs { get; set; } = 100;
    public int ExtendVisibilityEverySeconds { get; set; } = 45;

    // When a page-op handler can't get the per-document lock (another job is operating on the same
    // document), the job is deferred and its SQS message re-queued with this delay so it retries
    // once the document is free. SQS DelaySeconds max is 900.
    public int DocLockDeferRetrySeconds { get; set; } = 30;
}
