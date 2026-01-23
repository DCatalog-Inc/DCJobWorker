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
            ["JobExecutionConvertPDF"] = typeof(JobExecutionConvertPDF),
            ["ReplacePages"] = typeof(ReplacePagesJob),
            ["JobExecutionSaveLinksToCSV"] = typeof(JobExecutionSaveLinksToCSV)
            // add more mappings…
        };
    }

    public async Task<bool> ExecuteJobAsync(job oJob, CancellationToken ct = default)
    {
        if (oJob?.JobType == null) { _log.LogError("JobType is null for {JobId}", oJob?.Id); return false; }
        if (!_map.TryGetValue(oJob.JobType.Name, out var type))
        {
            _log.LogError("No handler for job type {Type}", oJob.JobType.Name);
            return false;
        }

        var handler = (IJobExecution)ActivatorUtilities.CreateInstance(_sp, type);
        _log.LogInformation("Processing Job {JobId} ({Type})", oJob.Id, oJob.JobType.Name);
        return await handler.ExecuteAsync(oJob, ct);
    }
}
