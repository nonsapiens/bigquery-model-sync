<?php

namespace Nonsapiens\BigqueryModelSync\Commands;

use Illuminate\Console\Command;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use ReflectionClass;
use ReflectionProperty;

class MakeBigQueryTableCommand extends Command
{
    protected $signature = 'make:bigquery-table {--class= : The model class name}';

    protected $description = 'Generate BigQuery CREATE TABLE SQL for a given model';

    public function handle(): int
    {
        $className = $this->option('class');

        if (!$className) {
            $this->error('Please provide a model class name using the --class option.');
            return self::FAILURE;
        }

        if (!class_exists($className)) {
            // Try to prepend app namespace if not found
            $appNamespace = trim(app()->getNamespace(), '\\');
            $potentialClassName = $appNamespace . '\\Models\\' . $className;
            if (class_exists($potentialClassName)) {
                $className = $potentialClassName;
            } elseif (class_exists($appNamespace . '\\' . $className)) {
                $className = $appNamespace . '\\' . $className;
            } else {
                $this->error("Class {$className} not found.");
                return self::FAILURE;
            }
        }

        try {
            $reflection = new ReflectionClass($className);
            if (!$reflection->isSubclassOf(Model::class)) {
                $this->error("Class {$className} is not an Eloquent model.");
                return self::FAILURE;
            }

            if (!in_array(SyncsToBigQuery::class, class_uses_recursive($className))) {
                $this->error("Class {$className} does not use the SyncsToBigQuery trait.");
                return self::FAILURE;
            }
        } catch (\Exception $e) {
            $this->error("Error reflecting class: " . $e->getMessage());
            return self::FAILURE;
        }

        /** @var Model $model */
        $model = new $className();
        
        // 1. Determine BigQuery Table Name
        $bigQueryTableName = $this->getPropertyValue($model, 'bigQueryTableName') ?: $model->getTable();

        // 2. Determine fields to sync
        $fieldsToSync = $this->getPropertyValue($model, 'fieldsToSync') ?: [];
        $hasGeodata = (bool) $this->getPropertyValue($model, 'hasGeodata', false);
        $geodataFields = $this->getPropertyValue($model, 'geodataFields') ?: ['latitude', 'longitude'];
        $mappedGeographyField = $this->getPropertyValue($model, 'mappedGeographyField') ?: 'geolocation';

        // 3. Get database columns
        $columns = $this->getTableColumns($model->getTable());
        if (empty($columns)) {
            $this->error("Could not retrieve columns for table: {$model->getTable()}");
            return self::FAILURE;
        }

        // Filter columns based on fieldsToSync
        if (!empty($fieldsToSync)) {
            $columns = array_filter($columns, function($col) use ($fieldsToSync) {
                return in_array($col['name'], $fieldsToSync);
            });
        }

        // Handle Geodata
        if ($hasGeodata) {
            // Exclude geodata fields
            $columns = array_filter($columns, function($col) use ($geodataFields) {
                return !in_array($col['name'], $geodataFields);
            });
            // Add mapped geography field
            $columns[] = [
                'name' => $mappedGeographyField,
                'type' => 'GEOGRAPHY',
                'nullable' => true, // Geography is usually nullable in this context
                'bq_type' => 'GEOGRAPHY'
            ];
        }

        // Map MariaDB types to BigQuery types
        foreach ($columns as &$column) {
            if (isset($column['bq_type'])) continue;
            $column['bq_type'] = $this->mapToBigQueryType($column['type']);
        }

        // 4. Partitioning
        $partitionField = null;
        $eligiblePartitionFields = array_filter($columns, function($col) {
            // BigQuery supports partitioning by DATE, DATETIME, TIMESTAMP, or INT64
            return in_array($col['bq_type'], ['DATE', 'DATETIME', 'TIMESTAMP', 'INT64']);
        });

        if (!empty($eligiblePartitionFields)) {
            $partitionChoices = array_map(fn($col) => $col['name'], $eligiblePartitionFields);
            $partitionChoices[] = 'None';
            $selectedPartition = $this->choice('Select a field to partition by (optional):', $partitionChoices, 'None');
            if ($selectedPartition !== 'None') {
                $partitionField = $selectedPartition;
            }
        }

        // 5. Clustering
        $clusterFields = [];
        $eligibleClusterFields = array_filter($columns, function($col) {
            // BigQuery supports clustering on most types except GEOGRAPHY and JSON
            return !in_array($col['bq_type'], ['GEOGRAPHY', 'JSON', 'RECORD']);
        });

        if (!empty($eligibleClusterFields)) {
            $clusterChoices = array_map(fn($col) => $col['name'], $eligibleClusterFields);
            $selectedClusters = $this->choice(
                'Select up to 4 fields to cluster by (comma separated numbers, optional):',
                $clusterChoices,
                null,
                null,
                true
            );

            if (!empty($selectedClusters)) {
                if (count($selectedClusters) > 4) {
                    $this->warn('BigQuery supports up to 4 clustering fields. Only the first 4 will be used.');
                    $selectedClusters = array_slice($selectedClusters, 0, 4);
                }
                $clusterFields = $selectedClusters;
            }
        }

        // 6. Generate SQL
        $sql = $this->generateCreateSql($bigQueryTableName, $columns, $partitionField, $clusterFields);

        $this->info("\nGenerated SQL:\n");
        $this->line("<fg=yellow>{$sql}</>");
        $this->info("");

        if ($this->confirm('Do you want to proceed with this SQL generation?', true)) {
            $this->info("SQL generation approved.");
            $this->line($sql);
            return self::SUCCESS;
        }

        $this->info("SQL generation aborted.");
        return self::SUCCESS;
    }

    protected function getPropertyValue(Model $model, string $property, $default = null)
    {
        try {
            $reflection = new ReflectionClass($model);
            if ($reflection->hasProperty($property)) {
                $prop = $reflection->getProperty($property);
                $prop->setAccessible(true);
                return $prop->getValue($model);
            }
        } catch (\Exception $e) {
            // Fallback to checking if it's public
        }

        // Some traits might define them as protected but accessible via __get if implemented, 
        // but here we just check if it's set on the instance
        if (isset($model->{$property})) {
            return $model->{$property};
        }

        return $default;
    }

    protected function getTableColumns(string $table): array
    {
        $columns = [];

        try {
            $dbColumns = DB::select("SHOW COLUMNS FROM `{$table}`");
            foreach ($dbColumns as $col) {
                $columns[] = [
                    'name' => $col->Field,
                    'type' => $col->Type,
                    'nullable' => strtolower($col->Null) === 'yes',
                ];
            }
        } catch (\Throwable $e) {
            // Fallback for non-MySQL or if SHOW COLUMNS fails
            try {
                $schemaColumns = Schema::getColumns($table);
                foreach ($schemaColumns as $col) {
                    $columns[] = [
                        'name' => $col['name'],
                        'type' => $col['type_name'] ?? $col['type'] ?? 'unknown',
                        'nullable' => $col['nullable'] ?? false,
                    ];
                }
            } catch (\Throwable $e2) {
                // Could not retrieve columns
            }
        }

        return $columns;
    }

    protected function mapToBigQueryType(string $mariaDbType): string
    {
        $mariaDbType = strtolower($mariaDbType);
        $length = null;
        if (preg_match('/\(([\d,]+)\)/', $mariaDbType, $matches)) {
            $length = (int)$matches[1];
        }

        if (str_contains($mariaDbType, 'int')) {
            return 'INT64';
        }

        if (str_contains($mariaDbType, 'decimal') || str_contains($mariaDbType, 'numeric') || str_contains($mariaDbType, 'double') || str_contains($mariaDbType, 'float')) {
            return 'NUMERIC';
        }

        if (str_contains($mariaDbType, 'varchar') || str_contains($mariaDbType, 'char') || str_contains($mariaDbType, 'text') || str_contains($mariaDbType, 'string')) {
            if ($length && $length > 0) {
                return "STRING({$length})";
            }
            return 'STRING';
        }

        if (str_contains($mariaDbType, 'timestamp')) {
            return 'TIMESTAMP';
        }

        if (str_contains($mariaDbType, 'datetime')) {
            return 'DATETIME';
        }

        if (str_contains($mariaDbType, 'date')) {
            return 'DATE';
        }

        if (str_contains($mariaDbType, 'time')) {
            return 'TIME';
        }

        if (str_contains($mariaDbType, 'bool') || str_contains($mariaDbType, 'tinyint(1)')) {
            return 'BOOL';
        }

        if (str_contains($mariaDbType, 'blob') || str_contains($mariaDbType, 'binary') || str_contains($mariaDbType, 'varbinary')) {
            if ($length && $length > 0) {
                return "BYTES({$length})";
            }
            return 'BYTES';
        }

        if (str_contains($mariaDbType, 'json')) {
            return 'JSON';
        }

        return 'STRING';
    }

    protected function generateCreateSql(string $tableName, array $columns, ?string $partitionField, array $clusterFields): string
    {
        $lines = [];
        foreach ($columns as $column) {
            $type = $column['bq_type'];
            $nullable = $column['nullable'] ? '' : ' NOT NULL';
            $lines[] = "  `{$column['name']}` {$type}{$nullable}";
        }

        $sql = "CREATE TABLE `{$tableName}` (\n" . implode(",\n", $lines) . "\n)";

        if ($partitionField) {
            // Check if it's INT64 for range partitioning, otherwise assume time-unit partitioning
            $col = collect($columns)->firstWhere('name', $partitionField);
            if ($col && $col['bq_type'] === 'INT64') {
                $sql .= "\nPARTITION BY RANGE_BUCKET({$partitionField}, GENERATE_ARRAY(0, 100000, 1000))";
                // Note: Range bucket needs parameters, these are placeholders.
            } else {
                $sql .= "\nPARTITION BY DATE({$partitionField})";
            }
        }

        if (!empty($clusterFields)) {
            $sql .= "\nCLUSTER BY " . implode(', ', $clusterFields);
        }

        return $sql . ";";
    }
}
