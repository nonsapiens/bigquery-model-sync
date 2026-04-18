<?php

namespace Nonsapiens\BigqueryModelSync\Strategies;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Database\Eloquent\Model;

class OnInsertSyncStrategy extends SyncStrategy
{
    /**
     * Execute the sync for the given model instance (single record).
     *
     * @param Model $model
     * @param \Nonsapiens\BigqueryModelSync\Models\BigQuerySync $syncRecord
     * @return int
     */
    public function execute(Model $model, \Nonsapiens\BigqueryModelSync\Models\BigQuerySync $syncRecord): int
    {
        $datasetId = config('bigquery.dataset');
        $tableName = $model->bigQueryTableName() ?? $model->getTable();
        $table = $this->bigQuery->dataset($datasetId)->table($tableName);

        // 1. Prepare the single record for BigQuery insertion
        $data = $this->prepareRow($model, $model);

        if (!empty($data)) {
            $rows = [['data' => $data]];
            $this->insertRows($table, $rows);
            return 1;
        }

        return 0;
    }
}
