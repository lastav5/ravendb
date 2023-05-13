using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Utils;
using Sparrow.Utils;
using Xunit;

namespace Tests.Infrastructure
{
    public class ReplicationInstance : IReplicationManager
    {
        private readonly DocumentDatabase _database;
        public readonly string DatabaseName;
        private IDocumentStore _src;
        private readonly IDocumentStore _dst;
        private ManualResetEventSlim _replicateOnceMre;
        private bool _replicateOnceInitialized = false;
        
        public ReplicationInstance(DocumentDatabase database, string databaseName, bool breakReplication, IDocumentStore src, IDocumentStore dst)
        {
            _database = database;
            DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));

            if (breakReplication)
            {
                _database.ReplicationLoader.DebugWaitAndRunReplicationOnce ??= new ManualResetEventSlim(true);
                _replicateOnceMre = _database.ReplicationLoader.DebugWaitAndRunReplicationOnce;
            }
            _src = src;
            _dst = dst;
        }

        public IDocumentStore Source
        {
            get
            {
                if (_src != null)
                    return _src;

                _src = new DocumentStore()
                {
                    Urls = new []{ _database.ServerStore.Server.WebUrl },
                    Database = ShardHelper.ToDatabaseName(DatabaseName)
                }.Initialize();

                return _src;
            }
        }

        public void Break()
        {
            var mre = new ManualResetEventSlim(false);
            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre;
        }

        public void Mend()
        {
            var mre = _database.ReplicationLoader.DebugWaitAndRunReplicationOnce;
            Assert.NotNull(mre);
            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
            _database.Configuration.Replication.MaxItemsCount = null;
            mre.Set();
        }

        private void InitializeReplicateOnce()
        {
            _database.Configuration.Replication.MaxItemsCount = 1;

            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce ??= new ManualResetEventSlim(true);
            _replicateOnceMre = _database.ReplicationLoader.DebugWaitAndRunReplicationOnce;

            _replicateOnceInitialized = true;
        }
        
        public async Task EnsureReplicatingAsync(string markerId)
        {
            AssertDestination(_dst);

            markerId ??= "marker/" + Guid.NewGuid();
            using (var s = _src.OpenSession())
            {
                s.Store(new { }, markerId);
                s.SaveChanges();
            }
            Assert.NotNull(await ClusterTestBase.WaitForDocumentToReplicateAsync<object>(_dst, markerId, 15 * 1000));
        }

        public Task EnsureReplicatingForDocIdAsync(string docId)
        {
            return EnsureReplicatingAsync(docId);
        }

        public void ReplicateOnce(string docId)
        {
            if (_replicateOnceInitialized == false)
                InitializeReplicateOnce();

            WaitForReset(); //wait for server to block and wait
            _replicateOnceMre.Set(); //let threads pass
        }

        //wait to reach reset and wait point in server
        private void WaitForReset(int timeout = 15_000)
        {
            var sp = Stopwatch.StartNew();
            while (sp.ElapsedMilliseconds < timeout)
            {
                if (_replicateOnceMre.IsSet == false)
                    return;

                Thread.Sleep(16);
            }

            throw new TimeoutException();
        }

        public virtual async Task EnsureNoReplicationLoopAsync()
        {
            using (var collector = new LiveReplicationPulsesCollector(_database))
            {
                var etag1 = _database.DocumentsStorage.GenerateNextEtag();

                await Task.Delay(3000);

                var etag2 = _database.DocumentsStorage.GenerateNextEtag();

                Assert.True(etag1 + 1 == etag2, "Replication loop found :(");

                var groups = collector.Pulses.GetAll().GroupBy(p => p.Direction);
                foreach (var group in groups)
                {
                    var key = group.Key;
                    var count = group.Count();
                    Assert.True(count < 50, $"{key} seems to be excessive ({count})");
                }
            }
        }

        public static void AssertDestination(IDocumentStore dst)
        {
            if(dst == null)
                throw new ArgumentNullException($"Destination document store is null. External replication functions must have a destination set");
        }

        public virtual void Dispose()
        {
            if (_replicateOnceInitialized)
            {
                WaitForReset();
                _replicateOnceMre.Set();
            }

            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
            _database.Configuration.Replication.MaxItemsCount = null;
        }

        internal static async ValueTask<ReplicationInstance> GetReplicationInstanceAsync(RavenServer server, string databaseName, bool breakReplication = false)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Make this func private when legacy BreakReplication() is removed");
            return new ReplicationInstance(await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName), databaseName, breakReplication);
        }
    }
}
