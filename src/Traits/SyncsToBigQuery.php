<?php

namespace Nonsapiens\BigqueryModelSync\Traits;

use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;

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

    public function bigQuerySyncSchedule(): string
    {
        return $this->syncSchedule ?? '*/5 * * * *';
    }

    public function bigQueryBatchSize(): int
    {
        return $this->batchSize ?? 10000;
    }

}