﻿using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing.Counters
{
    public class BasicCountersIndexes_StrongSyntax : RavenTestBase
    {
        public BasicCountersIndexes_StrongSyntax(ITestOutputHelper output) : base(output)
        {
        }

        private class MyCounterIndex : AbstractCountersIndexCreationTask<Company>
        {
            public MyCounterIndex()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                select new
                                                {
                                                    HeartBeat = counter.Value,
                                                    Name = counter.Name,
                                                    User = counter.DocumentId
                                                });
            }
        }

        private class MyCounterIndex_Load : AbstractCountersIndexCreationTask<Company>
        {
            public MyCounterIndex_Load()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                let company = LoadDocument<Company>(counter.DocumentId)
                                                let employee = LoadDocument<Employee>(company.Desc)
                                                select new
                                                {
                                                    HeartBeat = counter.Value,
                                                    User = counter.DocumentId,
                                                    Employee = employee.FirstName
                                                });
            }
        }

        private class AverageHeartRate : AbstractCountersIndexCreationTask<User, AverageHeartRate.Result>
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public string Name { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRate()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                select new Result
                                                {
                                                    HeartBeat = counter.Value,
                                                    Count = 1,
                                                    Name = counter.Name
                                                });

                Reduce = results => from r in results
                                    group r by r.Name into g
                                    let sumHeartBeat = g.Sum(x => x.HeartBeat)
                                    let sumCount = g.Sum(x => x.Count)
                                    select new Result
                                    {
                                        HeartBeat = sumHeartBeat / sumCount,
                                        Name = g.Key,
                                        Count = sumCount
                                    };
            }
        }

        private class AverageHeartRate_WithLoad : AbstractCountersIndexCreationTask<User, AverageHeartRate_WithLoad.Result>
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public string City { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRate_WithLoad()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                let user = LoadDocument<User>(counter.DocumentId)
                                                let address = LoadDocument<Address>(user.AddressId)
                                                select new Result
                                                {
                                                    HeartBeat = counter.Value,
                                                    Count = 1,
                                                    City = address.City
                                                });

                Reduce = results => from r in results
                                    group r by r.City into g
                                    let sumHeartBeat = g.Sum(x => x.HeartBeat)
                                    let sumCount = g.Sum(x => x.Count)
                                    select new Result
                                    {
                                        HeartBeat = sumHeartBeat / sumCount,
                                        City = g.Key,
                                        Count = sumCount
                                    };
            }
        }

        private class MyCounterIndex_AllCounters : AbstractCountersIndexCreationTask<Company>
        {
            public MyCounterIndex_AllCounters()
            {
                AddMapForAll(counters => from counter in counters
                                         select new
                                         {
                                             HeartBeat = counter.Value,
                                             Name = counter.Name,
                                             User = counter.DocumentId
                                         });
            }
        }

        private class MyCounterIndex_AllDocs : AbstractCountersIndexCreationTask<object>
        {
            public MyCounterIndex_AllDocs()
            {
                AddMapForAll(counters => from counter in counters
                                         select new
                                         {
                                             HeartBeat = counter.Value,
                                             Name = counter.Name,
                                             User = counter.DocumentId
                                         });
            }
        }

        [Fact]
        public void BasicMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("HeartRate", 7);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex();
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("counters.Companies.HeartRate.Select(counter => new {\r\n    HeartBeat = counter.Value,\r\n    Name = counter.Name,\r\n    User = counter.DocumentId\r\n})", indexDefinition.Maps.First());

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                var terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("7", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                using (var session = store.OpenSession())
                {
                    var company1 = session.Load<Company>("companies/1");
                    session.CountersFor(company1).Increment("HeartRate", 3);

                    var company2 = new Company();
                    session.Store(company2, "companies/2");
                    session.CountersFor(company2).Increment("HeartRate", 4);

                    var company3 = new Company();
                    session.Store(company3, "companies/3");
                    session.CountersFor(company3).Increment("HeartRate", 6);

                    var company999 = new Company();
                    session.Store(company999, "companies/999");
                    session.CountersFor(company999).Increment("HeartRate_Different", 999);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("10", terms);
                Assert.Contains("4", terms);
                Assert.Contains("6", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("companies/2", terms);
                Assert.Contains("companies/3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                // delete counter

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("HeartRate");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("4", terms);
                Assert.Contains("6", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/2", terms);
                Assert.Contains("companies/3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // delete document

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("6", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                // delete document - live

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.Delete("companies/3");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                // add document with counter - live

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company, "companies/4");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("5", terms);
            }
        }

        [Fact]
        public async Task BasicMapIndexWithLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var employee = new Employee
                    {
                        FirstName = "John"
                    };
                    session.Store(employee, "employees/1");

                    var company = new Company
                    {
                        Desc = employee.Id
                    };
                    session.Store(company, "companies/1");

                    session.CountersFor(company).Increment("HeartRate", 7);

                    var company2 = new Company
                    {
                        Desc = employee.Id
                    };
                    session.Store(company2, "companies/11");

                    session.CountersFor(company2).Increment("HeartRate", 11);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex_Load();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("counters.Companies.HeartRate.Select(counter => new {\r\n    counter = counter,\r\n    company = this.LoadDocument(counter.DocumentId, \"Companies\")\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    employee = this.LoadDocument(this0.company.Desc, \"Employees\")\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.counter.Value,\r\n    User = this1.this0.counter.DocumentId,\r\n    Employee = this1.employee.FirstName\r\n})", indexDefinition.Maps.First());

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("john", terms);

                using (var session = store.OpenSession())
                {
                    var employee = session.Load<Employee>("employees/1");
                    employee.FirstName = "Bob";

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("bob", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(0, terms.Length);

                // delete source document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.All(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexName);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/11");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [Fact]
        public void BasicMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var user = new User();
                        session.Store(user, $"users/{i}");

                        session.CountersFor(user).Increment("HeartRate", 180 + i);
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new AverageHeartRate();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("counters.Users.HeartRate.Select(counter => new {\r\n    HeartBeat = ((double) counter.Value),\r\n    Count = 1,\r\n    Name = counter.Name\r\n})", indexDefinition.Maps.First());
                RavenTestHelper.AssertEqualRespectingNewLines("results.GroupBy(r => r.Name).Select(g => new {\r\n    g = g,\r\n    sumHeartBeat = Enumerable.Sum(g, x => ((double) x.HeartBeat))\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    sumCount = Enumerable.Sum(this0.g, x0 => ((long) x0.Count))\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.sumHeartBeat / ((double) this1.sumCount),\r\n    Name = this1.this0.g.Key,\r\n    Count = this1.sumCount\r\n})", indexDefinition.Reduce);

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("184.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("10", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // add more heart rates
                using (var session = store.OpenSession())
                {
                    for (int i = 10; i < 20; i++)
                    {
                        var user = new User();
                        session.Store(user, $"users/{i}");

                        session.CountersFor(user).Increment("HeartRate", 200 + i);
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("199.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("20", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                //// delete some counters

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var user = session.Load<User>($"users/{i}");

                        session.CountersFor(user).Delete("HeartRate");
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("214.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("10", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                //// delete documents

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 20; i++)
                        session.Delete($"users/{i}");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public async Task BasicMapReduceIndexWithLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var address = new Address { City = "NY" };
                        session.Store(address, $"addresses/{i}");

                        var user = new User { AddressId = address.Id };
                        session.Store(user, $"users/{i}");

                        session.CountersFor(user).Increment("HeartRate", 180 + i);
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new AverageHeartRate_WithLoad();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("counters.Users.HeartRate.Select(counter => new {\r\n    counter = counter,\r\n    user = this.LoadDocument(counter.DocumentId, \"Users\")\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    address = this.LoadDocument(this0.user.AddressId, \"Addresses\")\r\n}).Select(this1 => new {\r\n    HeartBeat = ((double) this1.this0.counter.Value),\r\n    Count = 1,\r\n    City = this1.address.City\r\n})", indexDefinition.Maps.First());
                RavenTestHelper.AssertEqualRespectingNewLines("results.GroupBy(r => r.City).Select(g => new {\r\n    g = g,\r\n    sumHeartBeat = Enumerable.Sum(g, x => ((double) x.HeartBeat))\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    sumCount = Enumerable.Sum(this0.g, x0 => ((long) x0.Count))\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.sumHeartBeat / ((double) this1.sumCount),\r\n    City = this1.this0.g.Key,\r\n    Count = this1.sumCount\r\n})", indexDefinition.Reduce);

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("184.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("10", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("ny", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var address = session.Load<Address>($"addresses/{i}");
                        address.City = "LA";
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("ny", terms);
                Assert.Contains("la", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 5; i < 10; i++)
                        session.Delete($"addresses/{i}");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("la", terms);
                Assert.Contains("NULL_VALUE", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                        session.Delete($"users/{i}");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.All(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(0, terms.Length);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexName);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [Fact]
        public void CanMapAllCountersFromCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("HeartRate", 7);
                    session.CountersFor(company).Increment("Likes", 3);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex_AllCounters();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("counters.Companies.Select(counter => new {\r\n    HeartBeat = counter.Value,\r\n    Name = counter.Name,\r\n    User = counter.DocumentId\r\n})", indexDefinition.Maps.First());

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/2");
                    session.CountersFor(company).Increment("HeartRate", 13);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("companies/2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = new User();
                    session.Store(company, "users/1");
                    session.CountersFor(company).Increment("HeartRate", 13);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void CanMapAllCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("HeartRate", 7);
                    session.CountersFor(company).Increment("Likes", 3);

                    var employee = new Employee();
                    session.Store(employee, "employees/1");
                    session.CountersFor(employee).Increment("Dislikes", 1);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex_AllDocs();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("counters.Select(counter => new {\r\n    HeartBeat = counter.Value,\r\n    Name = counter.Name,\r\n    User = counter.DocumentId\r\n})", indexDefinition.Maps.First());

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("employees/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Increment("HeartRate", 2);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("9", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("employees/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());
            }
        }
    }
}
