using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public interface IReplicationManager : IDisposable
    {
        public void Break();
        public void Mend();
        public void ReplicateOnce(string docId);
        public Task EnsureNoReplicationLoopAsync();
        public Task EnsureReplicatingAsync(string markerId = null);
        public Task EnsureReplicatingForDocIdAsync(string docId);
    }
}
