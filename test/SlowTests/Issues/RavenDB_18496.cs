﻿using System.Threading.Tasks;
using FastTests.Server.Replication;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18496 : ReplicationTestBase
    {
        public RavenDB_18496(ITestOutputHelper output) : base(output)
        {
        }
        

        [Fact]
        public async Task DeletingMasterKeyForExistedEncryptedDatabaseShouldFail_2()
        {
            EncryptedServer(out var certificates, out var databaseName);

            using (var encryptedStore = GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = _ => databaseName,
                       ClientCertificate = certificates.ServerCertificate.Value,
                       AdminCertificate = certificates.ServerCertificate.Value,
                       Encrypted = true
                   }))
            using (var store2 = GetDocumentStore(new Options { ClientCertificate = certificates.ServerCertificate.Value }))
            {
                var db = await GetDocumentDatabaseInstanceFor(encryptedStore);
                db.Configuration.Replication.MaxSizeToSend = new Size(16, SizeUnit.Kilobytes);

                await SetupReplicationAsync(encryptedStore, store2);

                await EnsureReplicatingAsync(encryptedStore, store2);

                await EnsureNoReplicationLoop(Server, databaseName);
            }
        }
    }
}
