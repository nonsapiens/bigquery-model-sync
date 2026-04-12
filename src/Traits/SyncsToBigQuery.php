<?php

namespace Nonsapiens\BigqueryModelSync\Traits;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Str;
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;
use Nonsapiens\BigqueryModelSync\Models\BigQuerySync;
use Nonsapiens\BigqueryModelSync\Strategies\BatchSyncStrategy;
use Nonsapiens\BigqueryModelSync\Strategies\OnInsertSyncStrategy;
use Nonsapiens\BigqueryModelSync\Strategies\ReplaceSyncStrategy;

/** @var array<string>|null $fieldsToSync Fields that should be synced to BigQuery. If empty, all non-batch fields are synced. */
/** @var bool|null $hasGeodata Whether the model has geodata fields to be mapped. */
/** @var array<string>|null $geodataFields The source latitude and longitude fields. Defaults to ['latitude', 'longitude']. */
/** @var string|null $mappedGeographyField The target geography field name in BigQuery. Defaults to 'geolocation'. */
/** @var BigQuerySyncStrategy|null $syncStrategy The strategy to use for syncing. Defaults to BATCH. */
/** @var string|null $batchField The database field used to track batches. Defaults to 'sync_batch_uuid'. */
/** @var string|null $bigQueryTableName The target BigQuery table name. Defaults to the model's table name. */
/** @var string|null $syncSchedule The cron expression for sync schedule. Defaults to every 5 minutes. */
/** @var int|null $batchSize The number of records to process per batch. Defaults to 10000. */
trait SyncsToBigQuery
{
    public function bigQueryFieldsToSync(): array
    {
        return $this->fieldsToSync ?? [];
    }

    public function bigQueryHasGeodata(): bool
    {
        return $this->hasGeodata ?? false;
    }

    public function bigQueryGeodataFields(): array
    {
        return $this->geodataFields ?? ['latitude', 'longitude'];
    }

    public function bigQueryMappedGeographyField(): string
    {
        return $this->mappedGeographyField ?? 'geolocation';
    }

    public function bigQuerySyncStrategy(): BigQuerySyncStrategy
    {
        return $this->syncStrategy ?? BigQuerySyncStrategy::BATCH;
    }

    public function bigQueryBatchField(): string
    {
        return $this->batchField ?? 'sync_batch_uuid';
    }

    public function bigQueryTableName(): ?string
    {
        if (property_exists($this, 'bigQueryTableName')) {
            return $this->bigQueryTableName;
        }
        return null;
    }

    public function bigQuerySyncSchedule(): ?string
    {
        return $this->syncSchedule ?? null;
    }

    public function bigQueryBatchSize(): int
    {
        return $this->batchSize ?? 10000;
    }

    public function filterForBigQuerySync(Builder $query): void
    {
        // To be overridden by model class if needed
    }

    /**
     * Sync the model's data to BigQuery.
     *
     * @return BigQuerySync|null
     */
    public function sync(): ?BigQuerySync
    {
        $strategyType = $this->bigQuerySyncStrategy();
        $syncStrategy = match ($strategyType) {
            BigQuerySyncStrategy::BATCH => new BatchSyncStrategy(),
            BigQuerySyncStrategy::REPLACE => new ReplaceSyncStrategy(),
            BigQuerySyncStrategy::ON_INSERT => new OnInsertSyncStrategy(),
            default => null,
        };

        if (!$syncStrategy) {
            return null;
        }

        $syncBatchUuid = $strategyType === BigQuerySyncStrategy::REPLACE ? null : Str::uuid()->toString();

        $syncRecord = BigQuerySync::create([
            'model' => get_class($this),
            'sync_batch_uuid' => $syncBatchUuid,
            'sync_type' => $strategyType->value,
            'status' => 'in_progress',
            'started_at' => now(),
        ]);

        try {
            $recordsSynced = $syncStrategy->execute($this, $syncRecord);

            if ($recordsSynced === 0) {
                $syncRecord->delete();
                return null;
            }

            $syncRecord->update([
                'status' => 'completed',
                'records_synced' => $recordsSynced,
                'completed_at' => now(),
            ]);
        } catch (\Exception $e) {
            $syncRecord->update([
                'status' => 'failed',
                'error_message' => Str::limit($e->getMessage(), 65000),
                'completed_at' => now(),
            ]);
            throw $e;
        }

        return $syncRecord;
    }

    /**
     * Boot the trait to listen for model events.
     */
    public static function bootSyncsToBigQuery(): void
    {
        static::created(function (Model $model) {
            /** @var SyncsToBigQuery $model */
            if ($model->bigQuerySyncStrategy() === BigQuerySyncStrategy::ON_INSERT) {
                \Nonsapiens\BigqueryModelSync\Jobs\SyncRecordJob::dispatch($model);
            }
        });
    }
}