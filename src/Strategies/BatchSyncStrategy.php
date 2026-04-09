<?php

namespace Nonsapiens\BigqueryModelSync\Strategies;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Nonsapiens\BigqueryModelSync\Models\BigQuerySync;
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;

class BatchSyncStrategy extends SyncStrategy
{
    public function sync(Model $model): void
    {
        $batchField = $model->batchField ?? 'sync_batch_uuid';
        $batchSize = $model->batchSize ?? 1000;
        $syncBatchUuid = Str::uuid()->toString();

        // 1. Claim the records to sync
        $affected = DB::table($model->getTable())
            ->whereNull($batchField)
            ->update([$batchField => $syncBatchUuid]);

        if ($affected === 0) {
            return;
        }

        // 2. Create sync record
        $syncRecord = BigQuerySync::create([
            'model' => get_class($model),
            'sync_batch_uuid' => $syncBatchUuid,
            'sync_type' => BigQuerySyncStrategy::BATCH->value,
            'status' => 'in_progress',
            'started_at' => now(),
        ]);

        try {
            $bigQuery = new BigQueryClient([
                'projectId' => config('bigquery.project_id'),
                'keyFilePath' => config('bigquery.key_file_path'),
            ]);

            $datasetId = config('bigquery.dataset_id');
            $tableName = $model->bigQueryTableName ?? $model->getTable();
            $table = $bigQuery->dataset($datasetId)->table($tableName);

            $totalSynced = 0;

            // 3. Select those records with the UUID and bulk insert into BigQuery in batches
            DB::table($model->getTable())
                ->where($batchField, $syncBatchUuid)
                ->orderBy($model->getKeyName())
                ->chunk($batchSize, function ($records) use ($table, $model, &$totalSynced) {
                    $rows = [];
                    foreach ($records as $record) {
                        $data = [];
                        $recordArray = (array) $record;

                        foreach ($model->fieldsToSync as $field) {
                            $data[$field] = $recordArray[$field] ?? null;
                        }

                        if ($model->hasGeodata && isset($model->mappedGeographyField)) {
                            $lat = $recordArray[$model->geodataFields[0]] ?? null;
                            $lon = $recordArray[$model->geodataFields[1]] ?? null;

                            if ($lat !== null && $lon !== null) {
                                $data[$model->mappedGeographyField] = "POINT($lon $lat)";
                            }
                        }

                        $rows[] = ['data' => $data];
                    }

                    $response = $table->insertRows($rows);

                    if (!$response->isSuccessful()) {
                        $errors = [];
                        foreach ($response->failedRows() as $row) {
                            foreach ($row['errors'] as $error) {
                                $errors[] = $error['reason'] . ': ' . $error['message'];
                            }
                        }
                        throw new \Exception('BigQuery Insert Failed: ' . implode(', ', array_unique($errors)));
                    }

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
                'error_message' => $e->getMessage(),
                'completed_at' => now(),
            ]);
            throw $e;
        }
    }
}
