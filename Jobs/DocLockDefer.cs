using core;
using Core;
using Core.Models;
using DCatalogCommon.Data;
using Microsoft.EntityFrameworkCore;
using System.Threading;
using System.Threading.Tasks;

namespace JobWorker.Jobs
{
    // Shared handling for "another job is already operating on this document".
    //
    // Page-op handlers (replace / add / remove pages, intro page, create images) serialize on a
    // per-document MySQL GET_LOCK so two jobs can't mutate the same files at once. They USED to
    // block up to 600s waiting for that lock. Under concurrent same-document load (e.g. several
    // page replaces for one large catalog claimed at once across the worker fleet) those blocked
    // waiters piled up open connections and starved the worker thread pool until the job HOLDING
    // the lock could no longer finish either — a hard deadlock (frozen at 0%, lock held by an idle
    // connection, SQS messages stuck in-flight).
    //
    // Instead, handlers now try the lock briefly (LockWaitSeconds) and, if the document is busy,
    // DEFER: leave the job Waiting and let JobProcessor re-drive its SQS message after a short
    // delay. Only one job per document ever runs; the rest poll back in. No blocking, no pile-up.
    public static class DocLockDefer
    {
        // Effectively non-blocking: long enough to absorb a micro-race between two messages that
        // arrive at the same instant, short enough never to accumulate blocked waiters.
        public const int LockWaitSeconds = 5;

        // Marker written to job.Desctiption (and mirrored onto JobProcessor's tracked instance) so
        // the processor knows to re-queue the message rather than stamp Completed/Failed.
        public const string Sentinel = "DEFER_DOC_BUSY";

        // Leave the job re-claimable (Waiting) and tag it as deferred. jobRow is the handler's
        // tracked entity (DB write); oJob is the SAME instance JobProcessor reads after the handler
        // returns, so mirror onto it too.
        public static async Task MarkAsync(ApplicationDbContext context, job jobRow, job oJob, CancellationToken ct)
        {
            if (jobRow != null)
            {
                jobRow.Status = "Waiting";
                jobRow.Progress = 0;
                jobRow.Desctiption = Sentinel;
                await context.SaveChangesAsync(ct);
            }
            oJob.Status = "Waiting";
            oJob.Desctiption = Sentinel;
        }
    }
}
