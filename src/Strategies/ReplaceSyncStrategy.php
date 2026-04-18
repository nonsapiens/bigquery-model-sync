<?php

namespace Nonsapiens\BigqueryModelSync\Strategies;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;

class ReplaceSyncStrategy extends SyncStrategy
{
    public function execute(Model $model, \Nonsapiens\BigqueryModelSync\Models\BigQuerySync $syncRecord): int
    {
        $batchField = $model->bigQueryBatchField();
        $batchSize = $model->bigQueryBatchSize();

        $datasetId = config('bigquery.dataset');
        $tableName = $model->bigQueryTableName() ?? $model->getTable();
        $table = $this->bigQuery->dataset($datasetId)->table($tableName);

        // 1. Truncate BigQuery table
        // We use a query to delete all rows. Another option is $table->delete() then re-create, 
        // but DELETE is safer if we don't want to manage schema here.
        $queryConfig = $this->bigQuery->query("DELETE FROM `{$datasetId}.{$tableName}` WHERE 1=1");
        $this->bigQuery->runQuery($queryConfig);

        $totalSynced = 0;

        // 2. Select all records and bulk insert into BigQuery in batches
        DB::table($model->getTable())
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
