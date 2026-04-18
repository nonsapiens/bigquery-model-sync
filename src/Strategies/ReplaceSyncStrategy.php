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

        $totalSynced = 0;
        $isFirstBatch = true;

        // Select all records and bulk insert into BigQuery in batches
        DB::table($model->getTable())
            ->orderBy($model->getKeyName())
            ->chunk($batchSize, function ($records) use ($table, $model, $batchField, &$totalSynced, &$isFirstBatch) {
                $rows = [];
                foreach ($records as $record) {
                    $data = $this->prepareRow($record, $model, $batchField);

                    if (!empty($data)) {
                        $rows[] = $data;
                    }
                }

                if (!empty($rows)) {
                    $this->loadRows($table, $rows, $isFirstBatch);
                    $totalSynced += count($rows);
                    $isFirstBatch = false;
                }
            });

        // If no records were found, we still want to truncate the table
        if ($isFirstBatch) {
            $this->loadRows($table, [], true);
        }

        return $totalSynced;
    }

    /**
     * Load rows into BigQuery using a load job.
     *
     * @param \Google\Cloud\BigQuery\Table $table
     * @param array $rows
     * @param bool $truncate
     * @throws \Exception
     */
    protected function loadRows(\Google\Cloud\BigQuery\Table $table, array $rows, bool $truncate = false): void
    {
        $data = '';
        foreach ($rows as $row) {
            $data .= json_encode($row) . "\n";
        }

        $options = [
            'configuration' => [
                'load' => [
                    'sourceFormat' => 'NEWLINE_DELIMITED_JSON',
                    'writeDisposition' => $truncate ? 'WRITE_TRUNCATE' : 'WRITE_APPEND',
                ],
            ],
        ];

        // If no rows, we only truncate if $truncate is true
        if (empty($rows) && !$truncate) {
            return;
        }

        $jobConfig = $table->load($data, $options);
        $job = $this->bigQuery->startJob($jobConfig);

        // Wait for the job to complete
        $backoff = new \Google\Cloud\Core\ExponentialBackoff(10);
        $backoff->execute(function () use ($job) {
            $job->reload();
            if (!$job->isComplete()) {
                throw new \Exception('Job not yet complete');
            }
        });

        if (!$job->isComplete()) {
            throw new \Exception('BigQuery load job timed out.');
        }

        $stats = $job->info();
        if (isset($stats['status']['errorResult'])) {
            $errors = [];
            if (isset($stats['status']['errors'])) {
                foreach ($stats['status']['errors'] as $error) {
                    $errors[] = sprintf('Reason: %s, Message: %s', $error['reason'], $error['message']);
                }
            } else {
                $errors[] = sprintf('Reason: %s, Message: %s', $stats['status']['errorResult']['reason'], $stats['status']['errorResult']['message']);
            }
            throw new \Exception('BigQuery Load Job Failed: ' . implode(' | ', array_unique($errors)));
        }
    }
}
