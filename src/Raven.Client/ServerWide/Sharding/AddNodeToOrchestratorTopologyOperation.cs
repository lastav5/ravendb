﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding
{
    public class AddNodeToOrchestratorTopologyOperation : IServerOperation<ModifyOrchestratorTopologyResult>
    {
        private readonly string _databaseName;
        private readonly string _node;
        
        internal AddNodeToOrchestratorTopologyOperation(string databaseName, string node = null)
        {
            _databaseName = databaseName;
            _node = node;
        }

        public RavenCommand<ModifyOrchestratorTopologyResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddNodeToOrchestratorTopologyCommand(_databaseName, _node);
        }

        private class AddNodeToOrchestratorTopologyCommand : RavenCommand<ModifyOrchestratorTopologyResult>, IRaftCommand
        {
            private readonly string _databaseName;
            private readonly string _node;

            public AddNodeToOrchestratorTopologyCommand(string databaseName, string node)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);

                _databaseName = databaseName;
                _node = node;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/orchestrator?name={Uri.EscapeDataString(_databaseName)}";
                if (string.IsNullOrEmpty(_node) == false)
                {
                    url += $"&node={Uri.EscapeDataString(_node)}";
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyOrchestratorTopologyResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class ModifyOrchestratorTopologyResult
    {
        public string DatabaseName { get; set; }
        public string OrchestratorTopology { get; set; }
        public long RaftCommandIndex { get; set; }
    }
}
