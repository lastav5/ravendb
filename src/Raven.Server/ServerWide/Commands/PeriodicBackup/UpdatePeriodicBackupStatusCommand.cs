using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Utils.BackupUtils;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        public string Base64DbId;
        public PeriodicBackupStatus PeriodicBackupStatus;

        // ReSharper disable once UnusedMember.Local
        private UpdatePeriodicBackupStatusCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupStatusCommand(string base64DbId, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Base64DbId = base64DbId;
        }
        
        public override string GetItemId()
        {
            if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= SeparateBackupStatusVersion)
                return BackupStatusKeys.GenerateItemNameForBackupStatusPerDbId(DatabaseName, PeriodicBackupStatus.TaskId, Base64DbId);

            return BackupStatusKeys.GenerateItemNameLegacy(DatabaseName, PeriodicBackupStatus.TaskId);
        }

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId()));
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Base64DbId)] = Base64DbId;
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }
    }
}
