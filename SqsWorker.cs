using Amazon.SQS;
using Amazon.SQS.Model;
using core.Models;
using DCatalogCommon;
using DCatalogCommon.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections;

public sealed class SqsWorker : BackgroundService
{
    private readonly IAmazonSQS _sqs;
    private readonly QueueOptions _queues;
    private readonly WorkerOptions _cfg;
    private readonly IServiceProvider _sp;
    private readonly ILogger<SqsWorker> _log;
    private readonly SemaphoreSlim _gate;

    public SqsWorker(
        IAmazonSQS sqs,
        IOptions<QueueOptions> q,
        IOptions<WorkerOptions> cfg,
        IServiceProvider sp,
        ILogger<SqsWorker> log)
    {
        _sqs = sqs;
        _queues = q.Value;
        _cfg = cfg.Value;
        _sp = sp;
        _log = log;
        _gate = new SemaphoreSlim(_cfg.MaxConcurrency);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("Starting SQS worker. MaxConcurrency={Max}", _cfg.MaxConcurrency);
        using (var scope = _sp.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
            DCSQS oDCSQS = new DCSQS(context, _sqs);
            string sDistributedHPQueueName = await oDCSQS.getDistributedQueueNewUrl();
            //string sDistributedQueueName = await oDCSQS.getDistributedQueueUrl();
            //string sDemoQueueName = await oDCSQS.getDemosURL();
            //string sClientsQueueName = await oDCSQS.getClientsURL();

            string[] order = new string[4];
            order[0] = sDistributedHPQueueName;
            //order[1] = sDistributedQueueName;
            //order[2] = sDemoQueueName;
            //order[3] = sClientsQueueName;

            while (!stoppingToken.IsCancellationRequested)
            {
                var processed = false;

                foreach (string q in order)
                {
                    if (string.IsNullOrWhiteSpace(q)) continue;
                    if (_gate.CurrentCount == 0) break;

                    var req = new ReceiveMessageRequest
                    {
                        QueueUrl = q,
                        MaxNumberOfMessages = 1,
                        VisibilityTimeout = _cfg.VisibilityTimeoutSeconds,
                        WaitTimeSeconds = _cfg.LongPollSeconds
                    };

                    ReceiveMessageResponse resp;
                    try
                    {
                        resp = await _sqs.ReceiveMessageAsync(req, stoppingToken);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "ReceiveMessage failed for {Queue}", q);
                        continue;
                    }

                    if (resp?.Messages == null || resp.Messages.Count == 0)
                        continue;
                    var msg = resp.Messages[0];
                    await _gate.WaitAsync(stoppingToken);
                    _ = Task.Run(() => ProcessOneAsync(q, msg, stoppingToken), stoppingToken);
                    processed = true;
                    break; // restart from highest priority
                }

                if (!processed)
                    await Task.Delay(_cfg.EmptyPollDelayMs, stoppingToken);
            }

            _log.LogInformation("Stopping… waiting for in-flight tasks.");
            for (int i = 0; i < _cfg.MaxConcurrency; i++)
                await _gate.WaitAsync(TimeSpan.FromSeconds(1));
            _log.LogInformation("Stopped.");
        }
        
    }

   

    private async Task ProcessOneAsync(string queueUrl, Message msg, CancellationToken ct)
    {
        try
        {
            using var scope = _sp.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
            var processor = scope.ServiceProvider.GetRequiredService<JobProcessor>();

            var ok = await processor.HandleMessageAsync(msg, queueUrl, ct, _sqs, _cfg);

            if (ok)
                await _sqs.DeleteMessageAsync(queueUrl, msg.ReceiptHandle, ct);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Unhandled error for MessageId={Id}", msg.MessageId);
        }
        finally
        {
            _gate.Release();
        }
    }
}
