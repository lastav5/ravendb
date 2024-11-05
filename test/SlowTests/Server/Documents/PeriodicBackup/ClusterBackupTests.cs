using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class ClusterBackupTests : ClusterTestBase
    {
        public ClusterBackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task DontRunFullBackupAgainIfComingBackFromAnotherNode()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = leader
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Stav" }, "users/1");
                    await session.SaveChangesAsync();
                    var exists = await WaitForDocumentInClusterAsync<User>(nodes, store.Database, "users/1", x => x.Name == "Stav", TimeSpan.FromSeconds(10));
                    Assert.True(exists);
                }

                // A runs backup
                var originalNode = nodes[0].ServerStore.NodeTag;
                var config = Backup.CreateBackupConfiguration(backupPath, mentorNode: originalNode);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leader, config, store, isFullBackup: false);

                // assert number of directories
                var dirs = Directory.GetDirectories(backupPath);
                Console.WriteLine($"dirs first backup:\n {string.Join("\n", dirs)}");
                Assert.Equal(1, dirs.Length);

                // change responsible node
                var command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            { store.Database, new List<ResponsibleNodeInfo>() { new () { TaskId = taskId, ResponsibleNode = nodes[1].ServerStore.NodeTag } } }
                        }
                    }, RaftIdGenerator.NewId());
                await leader.ServerStore.Engine.SendToLeaderAsync(command);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, originalNode);

                // wait for backup to finish on that node
                var op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, taskId));
                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                // assert number of directories grew
                var newDirs = Directory.GetDirectories(backupPath).Except(dirs).ToList();
                Console.WriteLine($"dirs after second backup:\n {string.Join("\n", newDirs)}");
                Assert.Equal(1, newDirs.Count);

                //change responsible node back to original node
                command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            { store.Database, new List<ResponsibleNodeInfo>() { new () { TaskId = taskId, ResponsibleNode = originalNode } } }
                        }
                    }, RaftIdGenerator.NewId());
                await leader.ServerStore.Engine.SendToLeaderAsync(command);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, nodes[1].ServerStore.NodeTag);

                // wait for backup to finish on that node
                op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, taskId));
                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                // assert no new dirs for full backup
                var dirsAfterSwitchBack = Directory.GetDirectories(backupPath);
                Console.WriteLine($"DirsAfterSwitchBack:\n {string.Join("\n", dirsAfterSwitchBack)}");
                Assert.Equal(2, dirsAfterSwitchBack.Length);
            }
        }
        //TODO stav: test for deleting node A, then creating it again -> backup status should be deleted and start from scratch

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task BackupWillNotRunAgainIfNodeTagReplaced()
        {
            // If we have a backup status for node A and then we replace node A
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = leader
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Stav" }, "users/1");
                    await session.SaveChangesAsync();
                    var exists = await WaitForDocumentInClusterAsync<User>(nodes, store.Database, "users/1", x => x.Name == "Stav", TimeSpan.FromSeconds(10));
                    Assert.True(exists);
                }

                // A runs backup
                var originalNode = nodes[0].ServerStore.NodeTag;
                var config = Backup.CreateBackupConfiguration(backupPath, mentorNode: originalNode);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leader, config, store, isFullBackup: false);

                
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task DontDeleteCompareExchangeBelongingToBackupIfResponsibleNodeRemoved()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "10" },
            };
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, customSettings: settings);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = leader
            }))
            {
                // Create compare exchange
                var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/1", new User(){ Name = "Jane" }, 0));
                
                // Delete it to create a tombstone
                res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", res.Index));
                Assert.True(res.Successful);
                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(res.Index, nodes, TimeSpan.FromSeconds(15));
                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(0, stats.CountOfCompareExchange);
                Assert.Equal(1, stats.CountOfCompareExchangeTombstones);

                // Run full backup
                var responsibleNode = nodes[2].ServerStore.NodeTag;
                var config = Backup.CreateBackupConfiguration(backupPath, mentorNode: responsibleNode);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(nodes[2], config, store, isFullBackup: false);

                // assert number of directories
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(1, dirs.Length);

                // Make sure there is backup status for this node
                GetPeriodicBackupStatusOperationResult backupStatus = null;
                var noStatus = await WaitForValueAsync(async () =>
                {
                    backupStatus = await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId)); // this will fetch by responsible node
                    return backupStatus?.Status != null;
                }, true);
                Assert.True(noStatus);
                
                Assert.NotNull(backupStatus?.Status);
                Assert.Equal(responsibleNode, backupStatus.Status.NodeTag);

                // remove the backup's responsible node from the cluster
                await store.Operations.SendAsync(new RemoveClusterNodeOperation(responsibleNode));

                // Make sure there is no backup status for this node anymore
                var statusExists = await WaitForValueAsync(async () =>
                {
                    backupStatus = await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId)); // this will fetch by responsible node
                    return backupStatus?.Status == null;
                }, true);
                Assert.True(statusExists);

                var timeBeforeCxDeletion = DateTime.UtcNow;

                // execute the compare exchange cleanup
                leader.ServerStore.Observer._lastTombstonesCleanupTimeInTicks = 0;

                // wait for tombstone cleaner to finish
                await WaitAndAssertForValueAsync(() => leader.ServerStore.Observer._lastTombstonesCleanupTimeInTicks > timeBeforeCxDeletion.Ticks, true);


                // make sure the compare exchange tombstone hasn't been deleted
                await AssertWaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.ForDatabase(store.Database).SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange == 0 && stats.CountOfCompareExchangeTombstones != 0;
                }, true);
            }
        }
    }
}
