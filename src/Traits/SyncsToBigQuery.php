<?php

namespace Nonsapiens\BigqueryModelSync\Traits;

use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;

trait SyncsToBigQuery
{

    public array $fieldsToSync = [];

    public bool $hasGeodata = false;

    public array $geodataFields = [
        'latitude', 'longitude'
    ];

    public string $mappedGeographyField = 'geolocation';

    public BigQuerySyncStrategy $syncStrategy = BigQuerySyncStrategy::BATCH;

    public string $batchField = 'sync_batch_uuid';

    public ?string $bigQueryTableName = null;

    public string $syncSchedule = '*/5 * * * *';

    public int $batchSize = 10000;

}