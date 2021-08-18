using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Orders;
using Raven.Client.Documents.BulkInsert;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17126 : ReplicationTestBase
    {
        public RavenDB_17126(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CheckRetryTimeGrowsOnReplicationError()
        {
            var dbName1 = GetDatabaseName();
            var dbName2 = GetDatabaseName();

            using (var store1 = GetDocumentStore(new Options {ModifyDatabaseName = s => dbName1}))
            using (var store2 = GetDocumentStore(new Options {ModifyDatabaseName = s => dbName2}))
            {
                await SetupReplicationAsync(store1, store2);

                await EnsureReplicatingAsync(store1, store2);

                var database1 = await GetDocumentDatabaseInstanceFor(store1);

                using (BulkInsertOperation bulkInsert = store1.BulkInsert())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        bulkInsert.Store(new Employee
                        {
                            FirstName = "FirstName #" + i,
                            LastName = "LastName #" + i
                        });
                    }
                }

                //try
                //{
                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            await session.StoreAsync(new Employee());
                        }

                        await session.StoreAsync(new Employee() {FirstName = "fail"}, "make/this/fail");
                        await session.SaveChangesAsync();
                    }

                    WaitForDocument(store2, "nonexistent", 30000);
                //}
                //catch (Exception e)
                //{
                //    string error = "";
                //    foreach (var item in database1.ReplicationLoader.OutgoingFailureInfo)
                //    {

                //        error += $"ErrorsCount: {item.Value.Errors.Count}. Exception: ";
                //        foreach (var err in item.Value.Errors)
                //        {
                //            error += $"{err.Message} , ";
                //        }

                //        error += $"NextTimeout: {item.Value.NextTimeout}. " +
                //                 $"RetryOn: {item.Value.RetryOn}. " +
                //                 $"External: {item.Value.RetryOn}." +
                //                 $"DestinationDbId: {item.Value.DestinationDbId}." +
                //                 $"LastHeartbeatTicks: {item.Value.LastHeartbeatTicks}. ";

                //        error += Environment.NewLine;
                //    }
                    
                //    Console.WriteLine(error);
                //}
            }
        }
    }
}
