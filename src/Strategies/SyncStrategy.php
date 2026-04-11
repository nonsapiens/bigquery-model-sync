<?php

namespace Nonsapiens\BigqueryModelSync\Strategies;

use Google\Cloud\BigQuery\Table;
use Illuminate\Database\Eloquent\Model;
use Nonsapiens\BigqueryModelSync\Models\BigQuerySync;

abstract class SyncStrategy
{
    /**
     * Execute the sync for the given model.
     *
     * @param Model $model Instance of a model using SyncsToBigQuery trait
     * @param BigQuerySync $syncRecord
     * @return int Number of records synced
     */
    abstract public function execute(Model $model, BigQuerySync $syncRecord): int;


    /**
     * Prepare a single record for BigQuery insertion.
     *
     * @param object|array $record
     * @param Model $model
     * @param string|null $batchField Field to exclude from sync (used in Batch strategy)
     * @return array
     */
    protected function prepareRow(object|array $record, Model $model, ?string $batchField = null): array
    {
        $data = [];
        $recordArray = (array) $record;

        $fields = !empty($model->bigQueryFieldsToSync())
            ? $model->bigQueryFieldsToSync()
            : array_diff(array_keys($recordArray), (array) $batchField);

        $casts = $model->getCasts();

        foreach ($fields as $field) {
            $value = $recordArray[$field] ?? null;

            if ($value !== null) {
                $castType = $casts[$field] ?? null;
                if ($castType === 'boolean' || $castType === 'bool') {
                    $value = (bool) $value;
                } elseif ($castType === 'array' || $castType === 'json' || $castType === 'object') {
                    if (!is_string($value)) {
                        $value = json_encode($value);
                    }
                } elseif (is_string($value)) {
                    if ($this->isDateTime($value)) {
                        $value = new \DateTime($value);
                    }
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

        return $data;
    }

    /**
     * Insert rows into BigQuery and handle errors.
     *
     * @param Table $table
     * @param array $rows
     * @throws \Exception
     */
    protected function insertRows(Table $table, array $rows): void
    {
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
    }

    /**
     * Check if a string is a valid date/datetime format.
     *
     * @param mixed $string
     * @return bool
     */
    protected function isDateTime($string): bool
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
