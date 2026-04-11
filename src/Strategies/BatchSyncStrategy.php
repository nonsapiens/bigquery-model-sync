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

        // 1. Claim the records to sync
        $affected = DB::table($model->getTable())
            ->whereNull($batchField)
            ->update([$batchField => $syncBatchUuid]);

        if ($affected === 0) {
            return 0;
        }

        $bigQuery = new BigQueryClient([
            'projectId' => config('bigquery.projectId'),
        ]);

        $datasetId = config('bigquery.dataset');
        $tableName = $model->bigQueryTableName() ?? $model->getTable();
        $table = $bigQuery->dataset($datasetId)->table($tableName);

        $totalSynced = 0;

        // 2. Select those records with the UUID and bulk insert into BigQuery in batches
        DB::table($model->getTable())
            ->where($batchField, $syncBatchUuid)
            ->orderBy($model->getKeyName())
            ->chunk($batchSize, function ($records) use ($table, $model, $batchField, &$totalSynced) {
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
