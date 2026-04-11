<?php

namespace Nonsapiens\BigqueryModelSync\Traits;

use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;

trait SyncsToBigQuery
{
    /** @var array<string>|null $fieldsToSync Fields that should be synced to BigQuery. If empty, all non-batch fields are synced. */

    /** @var bool|null $hasGeodata Whether the model has geodata fields to be mapped. */

    /** @var array<string>|null $geodataFields The source latitude and longitude fields. Defaults to ['latitude', 'longitude']. */

    /** @var string|null $mappedGeographyField The target geography field name in BigQuery. Defaults to 'geolocation'. */

    /** @var BigQuerySyncStrategy|null $syncStrategy The strategy to use for syncing. Defaults to BATCH. */

    /** @var string|null $batchField The database field used to track batches. Defaults to 'sync_batch_uuid'. */

    /** @var string|null $bigQueryTableName The target BigQuery table name. Defaults to the model's table name. */

    /** @var string|null $syncSchedule The cron expression for sync schedule. Defaults to every 5 minutes. */

    /** @var int|null $batchSize The number of records to process per batch. Defaults to 10000. */

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

    public function bigQuerySyncSchedule(): string
    {
        return $this->syncSchedule ?? '*/5 * * * *';
    }

    public function bigQueryBatchSize(): int
    {
        return $this->batchSize ?? 10000;
    }

}