// JobProcessor.cs
using System;
using System.Xml;
using Amazon.SQS;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

public sealed class JobProcessor
{
    private readonly ApplicationDbContext _db;
    private readonly ILogger<JobProcessor> _log;
    private readonly JobUtil _jobUtil;   // add this

    public JobProcessor(ApplicationDbContext db, ILogger<JobProcessor> log, JobUtil jobUtil)
    { 
        _db = db; 
        _log = log;
        _jobUtil = jobUtil;

    }

    public async Task<bool> HandleMessageAsync(
        Amazon.SQS.Model.Message msg, string queueUrl, CancellationToken ct,
        IAmazonSQS sqs, WorkerOptions cfg)
    {
        // parse XML: //job/id (exactly like you do) :contentReference[oaicite:10]{index=10}
        Guid jobId;
        try
        {
            var xml = new XmlDocument();
            xml.LoadXml(msg.Body);
            jobId = Guid.Parse(xml.SelectSingleNode("//job/id")!.InnerText);
        }
        catch
        {
            _log.LogWarning("Malformed job XML; deleting. Msg={Id}", msg.MessageId);
            return true;
        }

        // fetch (with short retry like your loop) :contentReference[oaicite:11]{index=11}
        job? currentjob = null;

        for (int i = 0; i < 5 && currentjob is null; i++)
        {
            currentjob = await _db.job
                .Include(j => j.JobType)
                .FirstOrDefaultAsync(j => j.Id == jobId.ToString(), ct);

            if (currentjob is null)
                await Task.Delay(200, ct);
        }

        // only process Waiting / WaitingInQueue (your logic) :contentReference[oaicite:12]{index=12}
        if (currentjob.Status is not "Waiting" and not "WaitingInQueue")
            return true;

        // mark Processing (optimistic concurrency)
        currentjob.Status = Constants.JobProcessingStatus.Processing.ToString();
        currentjob.Desctiption = "Start Processing";
        currentjob.CreationTime = DateTime.Now;
        await _db.SaveChangesAsync(ct);

        // extend visibility periodically for long jobs
        using var visCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var visLoop = Task.Run(() => ExtendVisibilityAsync(sqs, queueUrl, msg.ReceiptHandle, cfg, visCts.Token), visCts.Token);

        try
        {
            await _jobUtil.ExecuteJobAsync(currentjob, ct);
            currentjob.Status = Constants.JobProcessingStatus.Completed.ToString();
            currentjob.Desctiption = "Completed";
            currentjob.Progress = 100;
            currentjob.CreationTime = DateTime.Now;
            await _db.SaveChangesAsync(ct);

            return true; // delete message
        }
        catch (Exception ex)
        {
            currentjob.Status = Constants.JobProcessingStatus.Failed.ToString();
            currentjob.Desctiption = ex.Message.Length > 512 ? ex.Message[..512] : ex.Message;
            currentjob.CreationTime = DateTime.Now;
            try { await _db.SaveChangesAsync(ct); } catch { /* best-effort */ }
            return false; // keep → SQS retry/DLQ
        }
        finally
        {
            visCts.Cancel();
            try { await visLoop; } catch { }
        }
    }

    private static async Task ExtendVisibilityAsync(IAmazonSQS sqs, string q, string rh, WorkerOptions cfg, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(cfg.ExtendVisibilityEverySeconds), ct);
            try { await sqs.ChangeMessageVisibilityAsync(q, rh, cfg.VisibilityTimeoutSeconds, ct); }
            catch (OperationCanceledException) { }
            catch { /* transient errors ok; next tick retries */ }
        }
    }
}
