﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.Subscriptions
{
    public class RavenDB_3193 : RavenTestBase
    {
        public RavenDB_3193(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldRespectCollectionCriteria(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new Company());
                        await session.StoreAsync(new User());
                        await session.StoreAsync(new Address());
                    }

                    await session.SaveChangesAsync();
                }

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from Users"
                };
                var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (var subscription = store.Subscriptions.GetSubscriptionWorker(
                    new SubscriptionWorkerOptions(id)
                    {
                        MaxDocsPerBatch = 31,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                {
                    var ids = new List<string>();

                    GC.KeepAlive(subscription.Run(batch =>
                    {
                        foreach (var item in batch.Items)
                        {
                            ids.Add(item.Id);
                        }
                    }));

                    Assert.True(SpinWait.SpinUntil(() => ids.Count >= 100, TimeSpan.FromSeconds(60)));
                    Assert.Equal(100, ids.Count);
                    foreach (var i in ids)
                    {
                        Assert.True(i.StartsWith("users/"));
                    }
                }
            }
        }
    }
}
