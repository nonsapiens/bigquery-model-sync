<?php

namespace Nonsapiens\BigqueryModelSync\Traits;

trait SyncsToBigQuery
{

    protected array $fieldsToSync = [];

    protected bool $hasGeodata = false;

    protected array $geodataFields = [
        'latitude', 'longitude'
    ];

    protected string $mappedGeographyField = 'geolocation';

    protected bool $syncOnCreate = false;

    protected string $batchField = 'sync_batch_uuid';

    protected ?string $bigQueryTableName = null;

}