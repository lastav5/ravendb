﻿using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    // This command is for updating the cluster backup status only. We do not want this to touch the local backup status
    public class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        public PeriodicBackupStatus PeriodicBackupStatus;

        // ReSharper disable once UnusedMember.Local
        private UpdatePeriodicBackupStatusCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupStatusCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            return PeriodicBackupStatus.GenerateItemName(DatabaseName, PeriodicBackupStatus.TaskId);
        }

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId()));
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }
    }
}
