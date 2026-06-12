using core;
using Core.Models;
using JobWorker.Jobs;

public sealed class JobUtil
{
    private readonly ILogger<JobUtil> _log;
    private readonly IServiceProvider _sp;
    private readonly IReadOnlyDictionary<string, Type> _map;

    public JobUtil(ILogger<JobUtil> log, IServiceProvider sp)
    {
        _log = log; _sp = sp;
        _map = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase)
        {
            // Original handlers
            ["JobExecutionConvertPDF"]          = typeof(JobExecutionConvertPDF),
            // NOTE: the old ["ReplacePages"] mapping pointed at a do-nothing stub that
            // marked jobs Completed — removed; unknown types now forward to legacy.
            ["JobExecutionSaveLinksToCSV"]      = typeof(JobExecutionSaveLinksToCSV),

            // Document management
            ["JobExecutionReplacePage"]         = typeof(JobExecutionReplacePage),
            ["JobExecutionIntroPage"]           = typeof(JobExecutionIntroPageWorker),
            ["ActivateEditionsJob"]             = typeof(ActivateEditionsJobWorker),
            ["JobHtmlGenerate"]                 = typeof(HtmlGenerateJobWorker),
            ["ValidateIndexDocumentJob"]        = typeof(ValidateIndexDocumentJobWorker),
            ["JobExecutionIndexDocument"]       = typeof(ValidateIndexDocumentJobWorker),
            ["JobExecutionCopyLinks"]           = typeof(CopyLinksJobWorker),
            ["FreeTrialJob"]                    = typeof(FreeTrialJobWorker),
            ["JobAddToAILibrary"]               = typeof(AddToAILibraryJobWorker),
            ["JobExecutionSavePageLabels"]      = typeof(GeneratePDFJobWorker),
            ["JobExecutionCreateDownloadAllPDF"]= typeof(HDUpdateDownloadPDFJobWorker),
            ["JobExecutionGenerateGifFlipbook"] = typeof(JobExecutionGenerateGifFlipbookWorker),
            ["JobExecutionRecognizeLinks"]      = typeof(JobExecutionRecognizeLinks),
            ["JobExecutionSaveLinksToPDF"]      = typeof(JobExecutionSaveLinksToPDF),
            ["JobExecutionSearchProductsInPublication"] = typeof(JobExecutionSearchProductsInPublication),
            ["JobSearchProductsInDocument"]     = typeof(JobExecutionSearchProductsInDocument),
            ["JobExecutionAddPages"]            = typeof(JobExecutionAddPages),
            ["JobExecutionRemovePages"]         = typeof(JobExecutionRemovePages),
            ["JobExecutionImportCSV"]           = typeof(JobExecutionImportCSV),

            // Product imports
            ["AdessoJob"]                       = typeof(AdessoJobWorker),
            ["AirgasImportJob"]                 = typeof(AirgasImportJobWorker),
            ["CCMPJob"]                         = typeof(CCMPJobWorker),
            ["CeratizitJob"]                    = typeof(CeratizitJobWorker),
            ["EverflowJob"]                     = typeof(EverflowJobWorker),
            ["HedelProductJob"]                 = typeof(HedelProductJobWorker),
            ["JFProductImportJob"]              = typeof(JFProductImportJobWorker),
            ["MayZLProductJob"]                 = typeof(MaysZLProductJobWorker),
            ["OdlJob"]                          = typeof(OdlJobWorker),
            ["RubiesJob"]                       = typeof(RubiesJobWorker),
            ["SummitImportProductJob"]          = typeof(SummitImportProductJobWorker),
        };
    }

    public async Task<bool> ExecuteJobAsync(job oJob, CancellationToken ct = default)
    {
        if (oJob?.JobType == null) { _log.LogError("JobType is null for {JobId}", oJob?.Id); return false; }
        if (!_map.TryGetValue(oJob.JobType.Name, out var type))
        {
            // Not ported to this worker yet (e.g. JobExecutionIntroPage) — hand it
            // to the legacy DocProcessor instead of failing the job.
            _log.LogWarning("No handler for job type {Type} — forwarding to legacy queue", oJob.JobType.Name);
            type = typeof(ForwardToLegacyQueue);
        }

        var handler = (IJobExecution)ActivatorUtilities.CreateInstance(_sp, type);
        _log.LogInformation("Processing Job {JobId} ({Type})", oJob.Id, oJob.JobType.Name);
        return await handler.ExecuteAsync(oJob, ct);
    }
}
