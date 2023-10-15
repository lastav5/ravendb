﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12867 : RavenTestBase
    {
        public RavenDB_12867(ITestOutputHelper output) : base(output)
        {
        }

        private class JsonDeserialization : JsonDeserializationBase
        {
            public static readonly Func<BlittableJsonReaderObject, Container> Container = GenerateJsonDeserializationRoutine<Container>();
        }

        private class Container
        {
            public object A { get; set; }
            public BlittableJsonReaderObject B { get; set; }
            public Dictionary<string, object> C { get; set; }
            public Dictionary<string, BlittableJsonReaderObject> D { get; set; }
        }

        [RavenFact(RavenTestCategory.None)]
        public void CanDeserializeToBlittableDictionary()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var str = @" {  ""A"" : {  ""P1"": true  }, ""B"": { ""P2"": false } , ""C"" : {  ""P1"": { ""aa"": false}  }, ""D"": { ""P2"": { ""aa"": false} } }";
                var blittable = context.Sync.ReadForMemory(str, "test");
                var parsedObject = JsonDeserialization.Container(blittable);

                Assert.IsType<BlittableJsonReaderObject>(parsedObject.A);
                Assert.IsType<BlittableJsonReaderObject>(parsedObject.B);
                Assert.IsType<BlittableJsonReaderObject>(parsedObject.C["P1"]);
                Assert.IsType<BlittableJsonReaderObject>(parsedObject.D["P2"]);
            }
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanRestoreSubscriptions(Options options)
        {
            var backupPath = NewDataPath(suffix: $"{options.DatabaseMode}_BackupFolder");

            using (var store = GetDocumentStore(options))
            {
                store.Subscriptions.Create<User>(x => x.Name == "Marcin");
                store.Subscriptions.Create<User>();

                var config = Backup.CreateBackupConfiguration(backupPath);
                
                if (options.DatabaseMode == RavenDatabaseMode.Single)
                    Backup.UpdateConfigAndRunBackup(Server, config, store);
                else
                {
                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));
                }

                // restore the database with a different name
                var restoredDatabaseName = GetDatabaseName();
                
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = restoredDatabaseName
                }))
                {
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    })
                    {
                        restoredStore.Initialize();
                        var subscriptions = restoredStore.Subscriptions.GetSubscriptions(0, 10);

                        Assert.Equal(2, subscriptions.Count);

                        foreach (var subscription in subscriptions)
                        {
                            Assert.NotNull(subscription.SubscriptionName);
                            Assert.NotNull(subscription.Query);
                        }
                    }
                }
            }
        }
    }
}
