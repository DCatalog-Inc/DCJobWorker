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

        // Only one worker may own a job. SQS standard queues deliver at-least-once, and a long
        // conversion can outlive the visibility window, so two workers can hold the same job.
        // A plain read-check-then-update races (both read Waiting, both run) — that is what let
        // two boxes convert the same Jafra catalog into the same folder and collide on the PDF
        // (File.Delete "used by another process"). Claim atomically: one conditional UPDATE,
        // trust the affected-row count.
        if (currentjob.Status is not "Waiting" and not "WaitingInQueue")
            return true; // already claimed/terminal — drop the duplicate message

        string workerId = WorkerIdentity.Value;
        int claimed = await _db.Database.ExecuteSqlRawAsync(
            "UPDATE job SET Status={0}, Desctiption={1}, ProcessedBy={2}, CreationTime={3} " +
            "WHERE Id={4} AND Status IN ('Waiting','WaitingInQueue')",
            new object[] { "Processing", "Start Processing", workerId, DateTime.Now, jobId.ToString() }, ct);

        if (claimed == 0)
        {
            // Another worker/thread won the race and already owns this job. Drop the duplicate.
            _log.LogInformation("Job {JobId} already claimed by another worker — skipping duplicate", jobId);
            return true;
        }

        // Reflect the committed claim on the tracked entity the handlers read/save below.
        await _db.Entry(currentjob).ReloadAsync(ct);

        // extend visibility periodically for long jobs
        using var visCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var visLoop = Task.Run(() => ExtendVisibilityAsync(sqs, queueUrl, msg.ReceiptHandle, cfg, visCts.Token), visCts.Token);

        try
        {
            bool ok = await _jobUtil.ExecuteJobAsync(currentjob, ct);
            if (!ok)
            {
                // Handler reported failure (or no handler registered). It may have already
                // written Failed + reason to the job row — don't overwrite that with
                // "Completed" (which used to hide real errors from the admin UI).
                if (currentjob.Status != Constants.JobProcessingStatus.Failed.ToString())
                {
                    currentjob.Status = Constants.JobProcessingStatus.Failed.ToString();
                    currentjob.Desctiption = "Job handler reported failure (see worker logs).";
                    currentjob.CreationTime = DateTime.Now;
                    await _db.SaveChangesAsync(ct);
                }
                return true; // delete message — failure is recorded on the job row
            }

            // A handler may have re-queued the job for the legacy worker (unported
            // job types are forwarded to the HP queue with status reset to Waiting).
            // Stamping Completed here would make the legacy worker's Waiting-only
            // guard skip it — the replace-pages forwarding bug all over again.
            if (currentjob.Status == "Waiting" || currentjob.Status == "WaitingInQueue")
                return true; // delete message; the legacy worker owns the job now

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

    // Stable per-process identity. Both prod boxes were cloned from one AMI and share the
    // Windows hostname (EC2AMAZ-KR2D7R4), so MachineName alone can't tell them apart in the
    // job row / logs — append a short per-process suffix so each worker is distinguishable.
    private static class WorkerIdentity
    {
        public static readonly string Value =
            $"DCJobWorker@{Environment.MachineName}#{Guid.NewGuid().ToString("N").Substring(0, 8)}";
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
