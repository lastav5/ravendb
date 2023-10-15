﻿using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_9294 : RavenTestBase
    {
        public RavenDB_9294(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task NewConnectionWithNoWorkShouldNotResetClient(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                var sn = store.Subscriptions.Create<User>();
                var worker = store.Subscriptions.GetSubscriptionWorker<User>(sn);

                var firstBatchHappened = new AsyncManualResetEvent();

                worker.AfterAcknowledgment += x =>
                {
                    firstBatchHappened.Set();
                    return Task.CompletedTask;
                };

                _ = worker.Run(x => { });
                Assert.True(await firstBatchHappened.WaitAsync(_reasonableWaitTime));
                worker.Dispose();
                worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sn)
                {
                    Strategy = SubscriptionOpeningStrategy.WaitForFree,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500)
                });

                var reconnectHappened = new AsyncManualResetEvent();

                firstBatchHappened.Reset();
                worker.OnSubscriptionConnectionRetry += x => reconnectHappened.Set();
                _ = worker.Run(x => firstBatchHappened.Set());
                Assert.False(await reconnectHappened.WaitAsync(TimeSpan.FromSeconds(6)),"Client reconnected");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                Assert.True(await firstBatchHappened.WaitAsync(_reasonableWaitTime), "First batch did not happen");
                Assert.False(await reconnectHappened.WaitAsync(TimeSpan.FromSeconds(1)), "Client reconnected");
            }
        }

    }
}
