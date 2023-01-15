using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue
{
    public class ShardedKafkaEtlTests : KafkaEtlTestBase
    {
        public ShardedKafkaEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        [RequiresKafkaRetryFact]
        //[RavenFact(RavenTestCategory.Sharding)]
        public async Task EnsureKafkaDoesNotProcessSameDocTwiceWhenResharding()
        {
            DoNotReuseServer();
            using var store = Sharding.GetDocumentStore();

            var config = SetupQueueEtlToKafka(store, DefaultScript, DefaultCollections);
            //var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
            
            var id = "orders/1-A";
            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = id,
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2},
                        new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                });
                session.SaveChanges();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = Sharding.GetBucket(id);
            var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, shardNumber);

            await Task.Delay(TimeSpan.FromSeconds(20));
            //AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            using IConsumer<string, byte[]> consumer = CreateKafkaConsumer(DefaultTopics.Select(x => x.Name));

            //consume the document
            var consumeResult = consumer.Consume();
            var bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            var order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(order);
            Assert.Equal(order.Id, "orders/1-A");
            Assert.Equal(order.OrderLinesCount, 2);
            Assert.Equal(order.TotalCost, 10);

            //reshard the document to a different shard
            var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard, RaftIdGenerator.NewId());
            var exists = WaitForDocument<Order>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, toShard));
            Assert.True(exists);

            //consume in kafka again
            consumeResult = consumer.Consume();

            //ensure we did not consume this document again
            Assert.Null(consumeResult.Message);
            
            consumer.Close();
            //etlDone.Reset();
        }

        private class Order
        {
            public string Id { get; set; }
            public List<OrderLine> OrderLines { get; set; }
        }

        private class OrderData
        {
            public string Id { get; set; }
            public int OrderLinesCount { get; set; }
            public int TotalCost { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
            public int Quantity { get; set; }
            public int Cost { get; set; }
        }
    }
}
