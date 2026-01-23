using Amazon.SQS;
using Core;
using DCatalogCommon.Data;
using JobWorker.Jobs;
using Microsoft.EntityFrameworkCore;
using iText.Licensing.Base;

//
// 1. Fetch secrets first (async before Host is built)
//
SecretsManagerService.Initialize("us-east-1");
var secrets = SecretsManagerService.Instance;

MySqlConfig mysqlConfig = await secrets.GetMySqlConfigAsync("MySqlDCServices");

var SecretString = await secrets.GetSecretValueAsync("IRONPDFKey", "IRONPDFKEY");
IronPdf.License.LicenseKey = SecretString;

string connectionString =
    $"Server={mysqlConfig.Host};Port={mysqlConfig.Port};Database={mysqlConfig.Database};" +
    $"User Id={mysqlConfig.Username};Password={mysqlConfig.Password};" +
    "convert zero datetime=True;CharSet=utf8;Allow User Variables=true";

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
        try
        {
            LicenseKey.LoadLicenseFile(new FileInfo("secrets/itextkey.json"));
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
        catch (Exception)
        {
            var fallback = ctx.HostingEnvironment.IsDevelopment()
                ? @"D:\DCatalog\Docs"
                : @"C:\DCatalog\Docs";

            DCCommon.Instance.RepositoryLocation ??= fallback;
            DCCommon.Instance.RepositoryLocationDB ??= DCCommon.Instance.RepositoryLocation;
        }

        // App services
        services.AddScoped<JobUtil>();
        services.AddTransient<JobExecutionConvertPDF>();
        services.AddTransient<ReplacePagesJob>();

        // AWS SQS
        services.AddSingleton<IAmazonSQS>(_ => new AmazonSQSClient());

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
}
