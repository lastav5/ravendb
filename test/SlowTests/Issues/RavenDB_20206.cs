﻿using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using SlowTests.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20206 : RavenTestBase
{
    public RavenDB_20206(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Should_Not_Clear_Compare_Exchange_Tombstones_Of_A_Database_With_Identical_Prefix()
    {
        using (var server = GetNewServer())
        using (var store1 = GetDocumentStore(new Options { Server = server }))
        using (var store2 = GetDocumentStore(new Options { Server = server, ModifyDatabaseName = _ => store1.Database + "_123" }))
        {
            WaitForFirstCompareExchangeTombstonesClean(server);
            var indexesList1 = new Dictionary<string, long>();
            var indexesList2 = new Dictionary<string, long>();
            // create 3 unique values
            for (int i = 0; i < 3; i++)
            {
                var res1 = await store1.Operations.SendAsync(new PutCompareExchangeValueOperation<int>($"{i}", i, 0));
                var res2 = await store2.Operations.SendAsync(new PutCompareExchangeValueOperation<int>($"{i}", i, 0));
                indexesList1.Add($"{i}", res1.Index);
                indexesList2.Add($"{i}", res2.Index);
            }

            // incremental backup on store2 db so observer won't delete its tombstones
            var config = Backup.CreateBackupConfiguration("backupFolder", incrementalBackupFrequency: "0 0 1 * *");
            var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store2, isFullBackup: false);

            // delete 1 unique value
            var del1 = await store1.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>("2", indexesList1["2"]));
            Assert.NotNull(del1.Value);
            indexesList1.Remove("2");

            var del2 = await store2.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>("2", indexesList2["2"]));
            Assert.NotNull(del2.Value);
            indexesList2.Remove("2");

            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store1.Database);
                Assert.Equal(1, numOfCompareExchangeTombstones);
            }

            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store2.Database);
                Assert.Equal(1, numOfCompareExchangeTombstones);
            }

            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                // clean tombstones
                await Cluster.RunCompareExchangeTombstoneCleaner(server, simulateClusterTransactionIndex: true);
            }

            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store1.Database);
                var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store1.Database);
                Assert.Equal(0, numOfCompareExchangeTombstones);
                Assert.Equal(2, numOfCompareExchanges);

                numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store2.Database);
                numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store2.Database);
                Assert.Equal(1, numOfCompareExchangeTombstones);
                Assert.Equal(2, numOfCompareExchanges);
            }

            await Backup.RunBackupAsync(server, taskId, store2, isFullBackup: false);

            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                // clean tombstones
                await Cluster.RunCompareExchangeTombstoneCleaner(server, simulateClusterTransactionIndex: true);
            }

            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store1.Database);
                var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store1.Database);
                Assert.Equal(0, numOfCompareExchangeTombstones);
                Assert.Equal(2, numOfCompareExchanges);

                numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store2.Database);
                numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store2.Database);
                Assert.Equal(0, numOfCompareExchangeTombstones);
                Assert.Equal(2, numOfCompareExchanges);
            }
        }
    }

    private static void WaitForFirstCompareExchangeTombstonesClean(RavenServer server)
    {
        Assert.True(WaitForValue(() =>
        {
            // wait for compare exchange tombstone cleaner run
            if (server.ServerStore.Observer == null)
                return false;

            if (server.ServerStore.Observer._lastTombstonesCleanupTimeInTicks == 0)
                return false;

            return true;
        }, true));
    }
}
