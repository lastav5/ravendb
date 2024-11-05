using System;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Utils.BackupUtils;

namespace Raven.Server.ServerWide.Commands;

public class DelayBackupCommand : UpdateValueForDatabaseCommand
{
    public long TaskId;
    public DateTime DelayUntil;
    public DateTime OriginalBackupTime;

    // ReSharper disable once UnusedMember.Local
    private DelayBackupCommand()
    {
        // for deserialization
    }

    public DelayBackupCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
    }

    public override string GetItemId() //TODO stav: can delete
    {
        throw new InvalidOperationException();
    }

    public override unsafe void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
    {
        result = null;

        if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= 54_000)
        {
            // update every backup status for every db-id
            var backupStatusPrefix = BackupStatusKeys.GenerateItemNamePrefix(DatabaseName, TaskId);
            using (Slice.From(context.Allocator, backupStatusPrefix, out var backupStatusPrefixSlice))
            {
                var statuses = items.SeekByPrimaryKeyPrefix(backupStatusPrefixSlice, Slices.Empty, 0).ToList();

                foreach (var status in statuses)
                {
                    using var backupStatusBlittable = new BlittableJsonReaderObject(status.Value.Reader.Read(0, out var size), size, context);

                    var updatedValue = GetUpdatedValue(index, record, context, backupStatusBlittable);

                    Debug.Assert(updatedValue.Value != null);

                    using (Slice.From(context.Allocator, status.Key.ToString().ToLowerInvariant(), out Slice valueNameLowered))
                    using (updatedValue)
                    {
                        ClusterStateMachine.UpdateValue(index, items, valueNameLowered, status.Key, updatedValue.Value);
                    }
                }
            }
        }
        else
        {
            //backwards compatibility
            var legacyKey = BackupStatusKeys.GenerateItemNameLegacy(DatabaseName, TaskId);
            UpdatedValue updatedValueOld;

            using (Slice.From(context.Allocator, legacyKey.ToLowerInvariant(), out Slice legacyKeyLowered))
            {
                BlittableJsonReaderObject itemBlittable = null;
                if (items.ReadByKey(legacyKeyLowered, out TableValueReader reader))
                {
                    var ptr = reader.Read(2, out int size);
                    itemBlittable = new BlittableJsonReaderObject(ptr, size, context);
                }

                updatedValueOld = GetUpdatedValue(index, record, context, itemBlittable);
            }

            Debug.Assert(updatedValueOld.ActionType == UpdatedValueActionType.Update && updatedValueOld.Value != null);

            using (Slice.From(context.Allocator, legacyKey, out Slice valueName))
            using (Slice.From(context.Allocator, legacyKey.ToLowerInvariant(), out Slice valueNameLowered))
            using (updatedValueOld)
            {
                ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, updatedValueOld.Value);
            }
        }

        AfterExecuteCommand(context, index, items);
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(TaskId)] = TaskId;
        json[nameof(DelayUntil)] = DelayUntil;
        json[nameof(OriginalBackupTime)] = OriginalBackupTime;
    }

    protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
    {
        if (existingValue != null)
        {
            existingValue.Modifications = new DynamicJsonValue
            {
                [nameof(DelayUntil)] = DelayUntil,
                [nameof(OriginalBackupTime)] = OriginalBackupTime
            };

            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(existingValue, GetItemId())); //TODO stav: does doc id here matter?
        }

        var status = new PeriodicBackupStatus
        {
            DelayUntil = DelayUntil,
            OriginalBackupTime = OriginalBackupTime
        };

        return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(status.ToJson(), GetItemId()));
    }

    public override object GetState()
    {
        return new DelayBackupCommandState
        {
            TaskId = TaskId,
            DelayUntil = DelayUntil,
            OriginalBackupTime = OriginalBackupTime
        };
    }

    public class DelayBackupCommandState
    {
        public long TaskId;
        public DateTime DelayUntil;
        public DateTime OriginalBackupTime;
    }
}
