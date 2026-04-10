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
        $batchField = $model->bigQueryBatchField();
        $batchSize = $model->bigQueryBatchSize();
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
                'projectId' => config('bigquery.projectId'),
            ]);

            $datasetId = config('bigquery.dataset');
            $tableName = $model->bigQueryTableName() ?? $model->getTable();
            $table = $bigQuery->dataset($datasetId)->table($tableName);

            $totalSynced = 0;

            // 3. Select those records with the UUID and bulk insert into BigQuery in batches
            DB::table($model->getTable())
                ->where($batchField, $syncBatchUuid)
                ->orderBy($model->getKeyName())
                ->chunk($batchSize, function ($records) use ($table, $model, $batchField, &$totalSynced) {
                    $rows = [];
                    foreach ($records as $record) {
                        $data = [];
                        $recordArray = (array) $record;

                        $fields = !empty($model->bigQueryFieldsToSync())
                            ? $model->bigQueryFieldsToSync()
                            : array_diff(array_keys($recordArray), [$batchField]);

                        foreach ($fields as $field) {
                            $value = $recordArray[$field] ?? null;

                            if (is_string($value)) {
                                if ($this->isDateTime($value)) {
                                    $value = new \DateTime($value);
                                }
                            }

                            $data[$field] = $value;
                        }

                        if ($model->bigQueryHasGeodata()) {
                            $geodataFields = $model->bigQueryGeodataFields();
                            $mappedGeographyField = $model->bigQueryMappedGeographyField();
                            $lat = $recordArray[$geodataFields[0]] ?? null;
                            $lon = $recordArray[$geodataFields[1]] ?? null;

                            if ($lat !== null && $lon !== null) {
                                $data[$mappedGeographyField] = "POINT($lon $lat)";
                            }
                        }

                        if (!empty($data)) {
                            $rows[] = ['data' => $data];
                        }
                    }

                    if (empty($rows)) {
                        return;
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
                'error_message' => Str::limit($e->getMessage(), 65000),
                'completed_at' => now(),
            ]);
            throw $e;
        }
    }

    private function isDateTime($string): bool
    {
        if (!is_string($string)) {
            return false;
        }

        // Check for common SQL date formats
        if (!preg_match('/^\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?$/', $string)) {
            return false;
        }

        try {
            new \DateTime($string);
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }
}
