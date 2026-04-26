<?php

namespace Nonsapiens\BigqueryModelSync\Strategies;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;

class BatchSyncStrategy extends SyncStrategy
{
    public function execute(Model $model, \Nonsapiens\BigqueryModelSync\Models\BigQuerySync $syncRecord): int
    {
        $batchField = $model->bigQueryBatchField();
        $batchSize = $model->bigQueryBatchSize();
        $syncBatchUuid = $syncRecord->sync_batch_uuid;

        $query = $model->newQuery()
            ->whereNull($batchField);

        $model->filterForBigQuerySync($query);

        $affected = $query->update([$batchField => $syncBatchUuid]);

        if ($affected === 0) {
            return 0;
        }

        $datasetId = config('bigquery.dataset');
        $tableName = $model->bigQueryTableName() ?? $model->getTable();
        $table = $this->bigQuery->dataset($datasetId)->table($tableName);

        $totalSynced = 0;

        // 2. Select those records with the UUID and bulk insert into BigQuery in batches
        $query = DB::table($model->getTable())
            ->where($batchField, $syncBatchUuid);

        // Mitigation for tables without 'id' field
        $orderBy = $model->getKeyName() ?: $batchField;
        if ($model->getKeyName() && $model->incrementing === false && $model->getKeyType() === 'string' && $model->getKeyName() === 'id') {
            // This is a common case for pivot models that haven't been fully configured
            // We check if the 'id' column actually exists to be safe
            $columnExists = false;
            try {
                $columnExists = \Illuminate\Support\Facades\Schema::hasColumn($model->getTable(), $model->getKeyName());
            } catch (\Exception $e) {
                // If we can't check, assume it doesn't and fallback to batchField
            }
            if (!$columnExists) {
                $orderBy = $batchField;
            }
        }
        $query->orderBy($orderBy);

        $query->chunk($batchSize, function ($records) use ($table, $model, $batchField, &$totalSynced) {
                $rows = [];
                foreach ($records as $record) {
                    $data = $this->prepareRow($record, $model, $batchField);

                    if (!empty($data)) {
                        $rows[] = ['data' => $data];
                    }
                }

                $this->insertRows($table, $rows);

                $totalSynced += count($rows);
            });

        return $totalSynced;
    }
}
