using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using DCatalogCommon.Data;

namespace JobWorker
{
    // Cross-worker, per-document advisory lock backed by MySQL GET_LOCK. Page-operations
    // (replace / add / remove pages, intro page) all mutate the same files under
    // C:\DCatalog\Docs\<pub>\<publ>\<docId>\. When two workers process two DIFFERENT jobs for the
    // SAME document at once, they collide ("The process cannot access the file ... because it is
    // being used by another process"). The job-level atomic claim only prevents same-JOB double
    // processing; this serializes per-DOCUMENT across all workers.
    //
    // Held on a dedicated connection for the duration of the op; released (or freed when the
    // connection closes) on dispose. Acquire blocks up to timeoutSeconds for a busy document.
    public sealed class DocumentLock : IAsyncDisposable
    {
        private readonly MySqlConnection _conn;
        private readonly string _key;
        private bool _held;

        private DocumentLock(MySqlConnection conn, string key, bool held)
        {
            _conn = conn; _key = key; _held = held;
        }

        public bool Acquired => _held;

        public static async Task<DocumentLock> AcquireAsync(ApplicationDbContext ctx, string documentId,
            int timeoutSeconds, CancellationToken ct = default)
        {
            string connString = ctx.Database.GetConnectionString();
            string key = "dcdoc:" + documentId;
            var conn = new MySqlConnection(connString);
            await conn.OpenAsync(ct);
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT GET_LOCK(@k, @t)";
                cmd.Parameters.AddWithValue("@k", key);
                cmd.Parameters.AddWithValue("@t", timeoutSeconds);
                object res = await cmd.ExecuteScalarAsync(ct);
                bool got = res != null && res != DBNull.Value && Convert.ToInt64(res) == 1L;
                return new DocumentLock(conn, key, got);
            }
            catch
            {
                try { await conn.CloseAsync(); } catch { }
                conn.Dispose();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_held)
                {
                    using var cmd = _conn.CreateCommand();
                    cmd.CommandText = "SELECT RELEASE_LOCK(@k)";
                    cmd.Parameters.AddWithValue("@k", _key);
                    await cmd.ExecuteScalarAsync();
                    _held = false;
                }
            }
            catch { /* lock auto-frees when the connection closes */ }
            try { await _conn.CloseAsync(); } catch { }
            _conn.Dispose();
        }
    }
}
