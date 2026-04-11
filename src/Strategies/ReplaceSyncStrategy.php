<?php

namespace Nonsapiens\BigqueryModelSync\Strategies;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Nonsapiens\BigqueryModelSync\Models\BigQuerySync;
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;

class ReplaceSyncStrategy extends SyncStrategy
{
    public function sync(Model $model): void
    {
        $batchSize = $model->bigQueryBatchSize();
        $syncBatchUuid = Str::uuid()->toString();

        // 1. Create sync record
        $syncRecord = BigQuerySync::create([
            'model' => get_class($model),
            'sync_batch_uuid' => $syncBatchUuid,
            'sync_type' => BigQuerySyncStrategy::REPLACE->value,
            'status' => 'in_progress',
            'started_at' => now(),
        ]);

        try {
            $bigQuery = new BigQueryClient([
                'projectId' => config('bigquery.projectId'),
            ]);

            $datasetId = config('bigquery.dataset');
            $tableName = $model->bigQueryTableName() ?? $model->getTable();
            $table = $bigQuery->dataset($datasetId)->table($tableName);

            // 2. Truncate BigQuery table
            // We use a query to delete all rows. Another option is $table->delete() then re-create, 
            // but DELETE is safer if we don't want to manage schema here.
            $queryConfig = $bigQuery->query("DELETE FROM `{$datasetId}.{$tableName}` WHERE 1=1");
            $bigQuery->runQuery($queryConfig);

            $totalSynced = 0;

            // 3. Select all records and bulk insert into BigQuery in batches
            DB::table($model->getTable())
                ->orderBy($model->getKeyName())
                ->chunk($batchSize, function ($records) use ($table, $model, &$totalSynced) {
                    $rows = [];
                    foreach ($records as $record) {
                        $data = $this->prepareRow($record, $model);

                        if (!empty($data)) {
                            $rows[] = ['data' => $data];
                        }
                    }

                    $this->insertRows($table, $rows);

                    $totalSynced += count($rows);
                });

            // 4. Update sync record as completed
            $syncRecord->update([
                'status' => 'completed',
                'records_synced' => $totalSynced,
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
    }
}
