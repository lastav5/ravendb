using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public class ShardedReplicationTestBase
    {
        internal readonly RavenTestBase _parent;

        public ShardedReplicationTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public class ShardedReplicationManager : IReplicationManager
        {
            public readonly Dictionary<int, ReplicationManager> ShardReplications;
            public readonly string DatabaseName;
            private readonly ShardingConfiguration _config;
            //private readonly IDocumentStore _src;
            private readonly IDocumentStore _dst;

            protected ShardedReplicationManager(Dictionary<int, ReplicationManager> shardReplications, string databaseName, ShardingConfiguration config, IDocumentStore src, IDocumentStore dst)
            {
                ShardReplications = shardReplications;
                DatabaseName = databaseName;
                _config = config;
                //_src = src;
                _dst = dst;
            }

            public void Mend()
            {
                foreach (var (shardNumber, brokenReplication) in ShardReplications)
                {
                    brokenReplication.Mend();
                }
            }

            public void Break()
            {
                foreach (var (shardNumber, shardReplication) in ShardReplications)
                {
                    shardReplication.Break();
                }
            }

            public void ReplicateOnce(string docId)
            {
                int shardNumber;
                using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
                    shardNumber = ShardHelper.GetShardNumberFor(_config, allocator, docId);

                ShardReplications[shardNumber].ReplicateOnce(docId);
            }

            public Task EnsureReplicatingForDocIdAsync(string docId)
            {
                return EnsureReplicatingAsync($"marker/{Guid.NewGuid()}${docId}");
            }

            public async Task EnsureReplicatingAsync(string markerId = null)
            {
                var manager = ShardReplications.Values.First();
                if (markerId != null)
                {
                    await manager.EnsureReplicatingAsync(markerId);
                    return;
                }
                
                //get ids for each shard in dst
                var ids = new Dictionary<int, string>(); //shard to id

                var dstConfig = (await _dst.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(_dst.Database))).Sharding;

                var guid = Guid.NewGuid();
                int i = 1;
                while (ids.Count < dstConfig.Shards.Count)
                {
                    var idForShard = "marker/" + guid + "/" + i;
                    var shardNumber = ShardHelper.GetShardNumberFor(dstConfig, idForShard);
                    if (ids.ContainsKey(shardNumber) == false)
                        ids.Add(shardNumber, idForShard);
                    i++;
                }
                
                var waitForDocTasks = new List<Task>();

                //send a marker to each shard in destination
                
                foreach (var (_, shardId) in ids)
                {
                    waitForDocTasks.Add(manager.EnsureReplicatingAsync(shardId));
                }

                await Task.WhenAll(waitForDocTasks);
            }

            public async Task EnsureNoReplicationLoopAsync()
            {
                foreach (var (node, replicationInstance) in ShardReplications)
                {
                    await replicationInstance.EnsureNoReplicationLoopAsync();
                }
            }

            public void Dispose()
            {
                foreach (var manager in ShardReplications.Values)
                {
                    manager.Dispose();
                }
            }

            internal static async ValueTask<ShardedReplicationManager> GetShardedReplicationManager(ShardingConfiguration configuration, List<RavenServer> servers,
                string databaseName, bool breakReplication, IDocumentStore dst = null)
            {
                Dictionary<int, ReplicationManager> shardReplications = new();
                foreach (var shardNumber in configuration.Shards.Keys)
                {
                    shardReplications.Add(shardNumber, await ReplicationManager.GetReplicationManagerAsync(servers, ShardHelper.ToShardName(databaseName, shardNumber), breakReplication));

                    Assert.True(shardReplications.ContainsKey(shardNumber), $"Couldn't find document database of shard {shardNumber} in any of the servers.");
                }

                return new ShardedReplicationManager(shardReplications, databaseName, configuration, dst);
            }
        }
    }
}
