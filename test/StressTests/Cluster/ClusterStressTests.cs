﻿using System.Threading.Tasks;
using FastTests;
using SlowTests.Cluster;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Cluster
{
    public class ClusterStressTests : NoDisposalNeeded
    {
        public ClusterStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.ClusterTransactions, RavenArchitecture.AllX64)]
        public async Task ParallelClusterTransactions()
        {
            using (var test = new ParallelClusterTransactionsTests(Output))
            {
                await test.ParallelClusterTransactions(7);
            }
        }
    }
}
