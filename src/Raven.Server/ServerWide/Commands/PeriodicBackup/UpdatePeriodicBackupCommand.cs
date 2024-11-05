using System;
using Nest;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Utils.BackupUtils;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupCommand : UpdateDatabaseCommand
    {
        public PeriodicBackupConfiguration Configuration;
        private bool _shouldRemoveBackupStatus;

        public UpdatePeriodicBackupCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupCommand(PeriodicBackupConfiguration configuration, string databaseName, string uniqueRequestId) 
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            bool newTask = false;
            if (Configuration.TaskId == 0)
            {
                // this is a new backup configuration
                newTask = true;
                Configuration.TaskId = etag;
                Configuration.CreatedAt = DateTime.UtcNow;
            }
            else
            {
                // modified periodic backup, remove the old one
                var previousBackupConfiguration = record.DeletePeriodicBackupConfiguration(Configuration.TaskId);
                if (previousBackupConfiguration != null && BackupHelper.BackupTypeChanged(previousBackupConfiguration, Configuration))
                    _shouldRemoveBackupStatus = true;
                
            }
            
            if (string.IsNullOrEmpty(Configuration.Name))
            {
                Configuration.Name = record.EnsureUniqueTaskName(Configuration.GetDefaultTaskName());
            }
            else if (Configuration.Name.StartsWith(ServerWideBackupConfiguration.NamePrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Can't {(newTask ? "create" : "update")} task: '{Configuration.Name}'. " +
                                                             $"A regular (non server-wide) backup task name can't start with prefix '{ServerWideBackupConfiguration.NamePrefix}'");
            }

            EnsureTaskNameIsNotUsed(record, Configuration.Name);

            record.PeriodicBackups.Add(Configuration);
        }

        public override void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, Logger clusterAuditLog)
        {
            if (_shouldRemoveBackupStatus == false)
                return;
            
            var taskNameOld = BackupStatusKeys.GenerateItemNameLegacy(DatabaseName, Configuration.TaskId);
            var taskName = BackupStatusKeys.GenerateItemNamePrefix(DatabaseName, Configuration.TaskId);

            if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= SeparateBackupStatusVersion)
            {
                using (Slice.From(ctx.Allocator, taskName.ToLowerInvariant(), out Slice backupStatusPrefix))
                {
                    items.DeleteByPrimaryKeyPrefix(backupStatusPrefix);
                }
            }
            else
            {
                using (Slice.From(ctx.Allocator, taskNameOld.ToLowerInvariant(), out Slice backupStatusLegacyKey))
                {
                    // for backwards compatibility, old backup status was stored without closing slash
                    items.DeleteByKey(backupStatusLegacyKey);
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
