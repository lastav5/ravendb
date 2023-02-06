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
            var etlDoneShard0 = WaitForEtl(ShardHelper.ToShardName(store.Database, 0), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard1 = WaitForEtl(ShardHelper.ToShardName(store.Database, 1), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard2 = WaitForEtl(ShardHelper.ToShardName(store.Database, 2), (n, statistics) => statistics.LoadSuccesses != 0);

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
                Console.WriteLine("TEST: creating orders/1-A");
                session.SaveChanges();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = Sharding.GetBucket(id);
            var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, shardNumber);
            Assert.Equal(1, toShard);

            Assert.Equal(0, shardNumber);
            Assert.Equal(1, toShard);
            
            AssertEtlDone(etlDoneShard0, TimeSpan.FromMinutes(1), ShardHelper.ToShardName(store.Database, 0), config);

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
            Console.WriteLine("TEST: resharding orders/1-A");
            var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard, RaftIdGenerator.NewId());
            var exists = WaitForDocument<Order>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, toShard));
            Assert.True(exists);
            
            //consume kafka tombstone?
            consumeResult = consumer.Consume();
            Assert.Null(consumeResult.Message);

            using (var session = store.OpenSession())
            {
                //var or = session.Load<Order>("orders/1-A");
                //or.OrderLines[0].Cost = 1;
                session.Store(new Order
                {
                    Id = "orders/2-A",
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2},
                        new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                });
                Console.WriteLine("TEST: creating orders/2-A");
                session.SaveChanges();
            }

            //ensure we consume the new doc and that the resharded doc has been filtered
            consumeResult = consumer.Consume();
            bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(order);
            Assert.Equal(order.Id, "orders/2-A");

            consumer.Close();
            etlDoneShard0.Reset();
            etlDoneShard1.Reset();
            etlDoneShard2.Reset();
        }

        [RequiresKafkaRetryFact]
        //[RavenFact(RavenTestCategory.Sharding)]
        public async Task EnsureKafkaDoesNotProcessSameDocTwiceSameId()
        {
            DoNotReuseServer();
            using var store = Sharding.GetDocumentStore();

            var config = SetupQueueEtlToKafka(store, DefaultScript, DefaultCollections);
            var etlDoneShard0 = WaitForEtl(ShardHelper.ToShardName(store.Database, 0), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard1 = WaitForEtl(ShardHelper.ToShardName(store.Database, 1), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard2 = WaitForEtl(ShardHelper.ToShardName(store.Database, 2), (n, statistics) => statistics.LoadSuccesses != 0);

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
                Console.WriteLine("TEST: creating orders/1-A");
                session.SaveChanges();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = Sharding.GetBucket(id);
            var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, shardNumber);
            Assert.Equal(1, toShard);

            Assert.Equal(0, shardNumber);
            Assert.Equal(1, toShard);

            AssertEtlDone(etlDoneShard0, TimeSpan.FromMinutes(1), ShardHelper.ToShardName(store.Database, 0), config);

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
            Console.WriteLine("TEST: resharding orders/1-A");
            var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard, RaftIdGenerator.NewId());
            var exists = WaitForDocument<Order>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, toShard));
            Assert.True(exists);

            //consume kafka tombstone?
            consumeResult = consumer.Consume();
            Assert.Null(consumeResult.Message);

            using (var session = store.OpenSession())
            {
                //var or = session.Load<Order>("orders/1-A");
                //or.OrderLines[0].Cost = 1;
                session.Store(new Order
                {
                    Id = "orders/2-A",
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2},
                        new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                });
                Console.WriteLine("TEST: creating orders/2-A");
                session.SaveChanges();
            }

            //ensure we consume the new doc and that the resharded doc has been filtered
            consumeResult = consumer.Consume();
            bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(order);
            Assert.Equal(order.Id, "orders/2-A");

            consumer.Close();
            etlDoneShard0.Reset();
            etlDoneShard1.Reset();
            etlDoneShard2.Reset();
        }

        [RequiresKafkaRetryFact]
        //[RavenFact(RavenTestCategory.Sharding)]
        public async Task EnsureKafkaDoesNotProcessSameDocTwiceWhenReshardingManyTimes()
        {
            DoNotReuseServer();
            using var store = Sharding.GetDocumentStore();

            var config = SetupQueueEtlToKafka(store, DefaultScript, DefaultCollections);
            var etlDoneShard0 = WaitForEtl(ShardHelper.ToShardName(store.Database, 0), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard1 = WaitForEtl(ShardHelper.ToShardName(store.Database, 1), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard2 = WaitForEtl(ShardHelper.ToShardName(store.Database, 2), (n, statistics) => statistics.LoadSuccesses != 0);

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
                Console.WriteLine("TEST: creating orders/1-A");
                session.SaveChanges();
            }
            
            var bucket = Sharding.GetBucket(id);

            AssertEtlDone(etlDoneShard0, TimeSpan.FromMinutes(1), ShardHelper.ToShardName(store.Database, 0), config);

            using IConsumer<string, byte[]> consumer = CreateKafkaConsumer(DefaultTopics.Select(x => x.Name));

            //consume the document
            var consumeResult = consumer.Consume();
            var bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            var order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(order);
            Assert.Equal(order.Id, "orders/1-A");
            Assert.Equal(order.OrderLinesCount, 2);
            Assert.Equal(order.TotalCost, 10);

            //reshard the document to shard 1
            Console.WriteLine("TEST: resharding orders/1-A");
            var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, 1, RaftIdGenerator.NewId());
            var exists = WaitForDocument<Order>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, 1));
            Assert.True(exists);
            using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, 1)))
            {
                var o = session.Load<Order>("orders/1-A");
                Assert.NotNull(o);
            }

            //consume kafka tombstone?
            consumeResult = consumer.Consume(); //TODO stav: get rid?
            Assert.Null(consumeResult.Message);

            //reshard the document back to shard 0
            Console.WriteLine("TEST: resharding orders/1-A back to shard 0");
            result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, 0, RaftIdGenerator.NewId());
            exists = WaitForDocument<Order>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, 0));
            Assert.True(exists);
            using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, 0)))
            {
                var o = session.Load<Order>("orders/1-A");
                Assert.NotNull(o);
            }

            //consume kafka tombstone?
            consumeResult = consumer.Consume();
            Assert.Null(consumeResult.Message);

            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "orders/2-A",
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2},
                        new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                });
                Console.WriteLine("TEST: creating orders/2-A");
                session.SaveChanges();
            }

            //ensure we consume the new doc and that the resharded doc has been filtered
            consumeResult = consumer.Consume();
            bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(order);
            Assert.Equal(order.Id, "orders/2-A");

            consumer.Close();
            etlDoneShard0.Reset();
            etlDoneShard1.Reset();
            etlDoneShard2.Reset();
        }

        [RequiresKafkaRetryFact]
        //[RavenFact(RavenTestCategory.Sharding)]
        public async Task EnsureKafkaDoesNotProcessSameDocTwiceWhenReshardingManyTimes_PopulateAllShardsBeforehand()
        {
            DoNotReuseServer();
            using var store = Sharding.GetDocumentStore();

            var config = SetupQueueEtlToKafka(store, DefaultScript, DefaultCollections);
            var etlDoneShard0 = WaitForEtl(ShardHelper.ToShardName(store.Database, 0), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard1 = WaitForEtl(ShardHelper.ToShardName(store.Database, 1), (n, statistics) => statistics.LoadSuccesses != 0);
            var etlDoneShard2 = WaitForEtl(ShardHelper.ToShardName(store.Database, 2), (n, statistics) => statistics.LoadSuccesses != 0);

            var id0 = "orders/1-A";
            Assert.Equal(0, await Sharding.GetShardNumberFor(store, id0));
            Assert.Equal(1, await Sharding.GetShardNumberFor(store, "orders/4-A"));
            Assert.Equal(2, await Sharding.GetShardNumberFor(store, "orders/2-A"));

            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = id0,
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                }, id0);
                session.Store(new Order
                {
                    Id = "orders/4-A",
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                }, "orders/4-A");
                session.Store(new Order
                {
                    Id = "orders/2-A",
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                }, "orders/2-A");
                Console.WriteLine("TEST: creating orders/1-A");
                session.SaveChanges();
            }

            var bucket = Sharding.GetBucket(id0);

            AssertEtlDone(etlDoneShard0, TimeSpan.FromMinutes(1), ShardHelper.ToShardName(store.Database, 0), config);

            using IConsumer<string, byte[]> consumer = CreateKafkaConsumer(DefaultTopics.Select(x => x.Name));

            //consume the document
            var consumeResult = consumer.Consume();
            var bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            var orderRes = JsonConvert.DeserializeObject<OrderData>(bytesAsString);
            Assert.NotNull(orderRes);
            Assert.Equal(orderRes.Id, id0);
            Assert.Equal(orderRes.OrderLinesCount, 2);
            Assert.Equal(orderRes.TotalCost, 10);

            //consume the document
            consumeResult = consumer.Consume();
            bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            orderRes = JsonConvert.DeserializeObject<OrderData>(bytesAsString);
            Assert.NotNull(orderRes);
            Assert.Equal(orderRes.Id, "orders/4-A");
            
            //consume the document
            consumeResult = consumer.Consume();
            bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            orderRes = JsonConvert.DeserializeObject<OrderData>(bytesAsString);
            Assert.NotNull(orderRes);
            Assert.Equal(orderRes.Id, "orders/2-A");
            
            //reshard the document to shard 1
            Console.WriteLine("TEST: resharding orders/1-A");
            var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, 1, RaftIdGenerator.NewId());
            var exists = WaitForDocument<Order>(store, id0, predicate: null, database: ShardHelper.ToShardName(store.Database, 1));
            Assert.True(exists);
            using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, 0)))
            {
                var o = session.Load<Order>(id0);
                Assert.Null(o);
            }

            //consume kafka tombstone?
            Console.WriteLine("TEST: consume kafka tombstone");
            consumeResult = consumer.Consume(); //TODO stav: get rid?
            Assert.Null(consumeResult.Message);

            //reshard the document back to shard 0
            Console.WriteLine("TEST: resharding orders/1-A back to shard 0");
            result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, 0, RaftIdGenerator.NewId());
            exists = WaitForDocument<Order>(store, id0, predicate: null, database: ShardHelper.ToShardName(store.Database, 0));
            Assert.True(exists);
            using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, 1)))
            {
                var o = session.Load<Order>(id0);
                Assert.Null(o);
            }

            //consume kafka tombstone?
            Console.WriteLine("TEST: consume kafka tombstone"); //TODO stav: test is stuck here and doesn't consume anymore
            consumeResult = consumer.Consume();
            Assert.Null(consumeResult.Message);

            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "orders/2-A",
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine {Cost = 3, Product = "Milk", Quantity = 2},
                        new OrderLine {Cost = 4, Product = "Bear", Quantity = 1},
                    }
                });
                Console.WriteLine("TEST: creating orders/2-A");
                session.SaveChanges();
            }

            //ensure we consume the new doc and that the resharded doc has been filtered
            consumeResult = consumer.Consume();
            bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            orderRes = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(orderRes);
            Assert.Equal(orderRes.Id, "orders/2-A");

            consumer.Close();
            etlDoneShard0.Reset();
            etlDoneShard1.Reset();
            etlDoneShard2.Reset();
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
