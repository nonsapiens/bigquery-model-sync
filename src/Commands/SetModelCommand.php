<?php

namespace Nonsapiens\BigqueryModelSync\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use ReflectionClass;
use Symfony\Component\Finder\Finder;

class SetModelCommand extends Command
{
    protected $signature = 'bigquery:set-model';

    protected $description = 'Configure an Eloquent model to sync with BigQuery';

    public function handle(): int
    {
        // Step 1: Find all Eloquent models without the SyncsToBigQuery trait
        $models = $this->findModelsWithoutTrait();

        if (empty($models)) {
            $this->info('All models already have the SyncsToBigQuery trait, or no models were found.');
            return self::SUCCESS;
        }

        // Step 2: Present list and let user select a model
        $modelLabels = array_keys($models);
        $selectedLabel = $this->choice('Select the model you want to make syncable:', $modelLabels);
        $modelInfo = $models[$selectedLabel];
        $modelClass = $modelInfo['class'];
        $modelPath = $modelInfo['path'];

        // Step 3: Retrieve fields from the database
        $instance = new $modelClass();
        $table = $instance->getTable();
        $columns = $this->getTableColumns($table);

        if (empty($columns)) {
            $this->error("Could not retrieve columns for table: {$table}");
            return self::FAILURE;
        }

        // Step 4: Multiselect fields (all selected by default)
        $columnChoices = [];
        foreach ($columns as $column) {
            $nullable = $column['nullable'] ? 'nullable' : 'not null';
            $columnChoices[] = "{$column['name']} ({$column['type']}, {$nullable})";
        }

        $selectedChoices = $this->choice(
            'Select the fields to sync (all selected by default). Use comma-separated numbers to deselect:',
            $columnChoices,
            implode(',', array_keys($columnChoices)),
            null,
            true
        );

        // Map selected choices back to column names
        $selectedColumns = [];
        foreach ($selectedChoices as $choice) {
            foreach ($columns as $column) {
                $nullable = $column['nullable'] ? 'nullable' : 'not null';
                if ($choice === "{$column['name']} ({$column['type']}, {$nullable})") {
                    $selectedColumns[] = $column['name'];
                    break;
                }
            }
        }

        // Step 5: Check for latitude/longitude and offer GEOGRAPHY mapping
        $hasGeodata = false;
        $mappedGeographyField = 'geolocation';

        if (in_array('latitude', $selectedColumns) && in_array('longitude', $selectedColumns)) {
            $hasGeodata = $this->confirm('Fields latitude and longitude are selected. Do you want to map them into a single GEOGRAPHY field in BigQuery?', true);

            if ($hasGeodata) {
                $mappedGeographyField = $this->ask('What should the GEOGRAPHY field be named?', 'geolocation');
            }
        }

        // Step 6: Sync on create or batch
        $syncOnCreate = $this->confirm('Do you want the model to sync on create? (No = batch sync)', false);

        // Step 7: Batch field name
        $batchField = $this->ask('What should the batch field name be?', 'sync_batch_uuid');

        // Step 8: Generate migration for batch field?
        $generateMigration = $this->confirm("Do you want a migration generated to add '{$batchField}' to the '{$table}' table?", true);

        // Step 9: Confirm to proceed
        $this->info('');
        $this->info('Summary:');
        $this->line("  Model:              {$modelClass}");
        $this->line("  Table:              {$table}");
        $this->line("  Fields to sync:     " . implode(', ', $selectedColumns));
        $this->line("  Has geodata:        " . ($hasGeodata ? "Yes (mapped to '{$mappedGeographyField}')" : 'No'));
        $this->line("  Sync on create:     " . ($syncOnCreate ? 'Yes' : 'No (batch)'));
        $this->line("  Batch field:        {$batchField}");
        $this->line("  Generate migration: " . ($generateMigration ? 'Yes' : 'No'));
        $this->info('');

        if (!$this->confirm('Do you want to proceed?', true)) {
            $this->info('Aborted.');
            return self::SUCCESS;
        }

        // Step 10: Generate migration if requested
        if ($generateMigration) {
            $this->createMigration($table, $batchField);
        }

        // Step 11: Update the model class
        $this->updateModelClass($modelPath, $modelClass, $selectedColumns, $hasGeodata, $mappedGeographyField, $syncOnCreate, $batchField);

        $this->info('Done! Model has been configured for BigQuery sync.');
        return self::SUCCESS;
    }

    protected function findModelsWithoutTrait(): array
    {
        $models = [];
        $appPath = app_path();

        if (!is_dir($appPath)) {
            return $models;
        }

        $finder = new Finder();
        $finder->files()->name('*.php')->in($appPath);

        foreach ($finder as $file) {
            $class = $this->getClassFromFile($file->getRealPath());
            if (!$class) {
                continue;
            }

            try {
                $reflection = new ReflectionClass($class);
            } catch (\Throwable $e) {
                continue;
            }

            if (!$reflection->isSubclassOf(\Illuminate\Database\Eloquent\Model::class)) {
                continue;
            }

            if ($reflection->isAbstract()) {
                continue;
            }

            $traits = $this->getTraitsRecursively($reflection);
            if (in_array(SyncsToBigQuery::class, $traits)) {
                continue;
            }

            $shortName = $reflection->getShortName();
            $models[$shortName] = [
                'class' => $class,
                'path'  => $file->getRealPath(),
            ];
        }

        return $models;
    }

    protected function getTraitsRecursively(ReflectionClass $reflection): array
    {
        $traits = array_keys($reflection->getTraits());
        $parent = $reflection->getParentClass();
        if ($parent) {
            $traits = array_merge($traits, $this->getTraitsRecursively($parent));
        }
        return $traits;
    }

    protected function getClassFromFile(string $path): ?string
    {
        $contents = file_get_contents($path);
        if (!$contents) {
            return null;
        }

        $namespace = null;
        $class = null;

        if (preg_match('/^namespace\s+(.+?);/m', $contents, $matches)) {
            $namespace = $matches[1];
        }

        if (preg_match('/^class\s+(\w+)/m', $contents, $matches)) {
            $class = $matches[1];
        }

        if (!$class) {
            return null;
        }

        return $namespace ? "{$namespace}\\{$class}" : $class;
    }

    protected function getTableColumns(string $table): array
    {
        $columns = [];

        try {
            $dbColumns = DB::select("SHOW COLUMNS FROM `{$table}`");
            foreach ($dbColumns as $col) {
                $columns[] = [
                    'name'     => $col->Field,
                    'type'     => $col->Type,
                    'nullable' => strtolower($col->Null) === 'yes',
                ];
            }
        } catch (\Throwable $e) {
            // Try Doctrine/Schema approach as fallback
            try {
                $schemaColumns = Schema::getColumns($table);
                foreach ($schemaColumns as $col) {
                    $columns[] = [
                        'name'     => $col['name'],
                        'type'     => $col['type_name'] ?? $col['type'] ?? 'unknown',
                        'nullable' => $col['nullable'] ?? false,
                    ];
                }
            } catch (\Throwable $e2) {
                // Could not retrieve columns
            }
        }

        return $columns;
    }

    protected function createMigration(string $table, string $batchField): void
    {
        $timestamp = now()->format('Y_m_d_His');
        $migrationName = "add_{$batchField}_to_{$table}_table";
        $filename = database_path("migrations/{$timestamp}_{$migrationName}.php");

        $lastColumn = $this->getLastTableColumn($table);
        $afterClause = $lastColumn ? "->after('{$lastColumn}')" : '';

        $stub = <<<PHP
<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('{$table}', function (Blueprint \$table) {
            \$table->char('{$batchField}', 36)->nullable()->index(){$afterClause};
        });
    }

    public function down(): void
    {
        Schema::table('{$table}', function (Blueprint \$table) {
            \$table->dropIndex(['{$batchField}']);
            \$table->dropColumn('{$batchField}');
        });
    }
};
PHP;

        file_put_contents($filename, $stub);
        $this->info("Migration created: {$filename}");
    }

    protected function getLastTableColumn(string $table): ?string
    {
        try {
            $columns = Schema::getColumnListing($table);
            return !empty($columns) ? end($columns) : null;
        } catch (\Throwable $e) {
            return null;
        }
    }

    protected function updateModelClass(
        string $modelPath,
        string $modelClass,
        array $selectedColumns,
        bool $hasGeodata,
        string $mappedGeographyField,
        bool $syncOnCreate,
        string $batchField
    ): void {
        $contents = file_get_contents($modelPath);

        // Build trait properties to inject
        $fieldsArray = "['" . implode("', '", $selectedColumns) . "']";
        $hasgeoStr   = $hasGeodata ? 'true' : 'false';
        $syncStr     = $syncOnCreate ? 'true' : 'false';

        $traitProperties = <<<PHP

    protected array \$fieldsToSync = {$fieldsArray};

    protected bool \$hasGeodata = {$hasgeoStr};

    protected string \$mappedGeographyField = '{$mappedGeographyField}';

    protected bool \$syncOnCreate = {$syncStr};

    protected string \$batchField = '{$batchField}';
PHP;

        // 1. Add use statement for the trait namespace (if not already present)
        $traitUse = 'use Nonsapiens\\BigqueryModelSync\\Traits\\SyncsToBigQuery;';
        if (!str_contains($contents, $traitUse)) {
            // Insert after the namespace declaration or after the last existing use statement
            if (preg_match('/^(use\s+[^;]+;)(?!.*^use\s)/ms', $contents)) {
                // Insert after the last use statement
                $contents = preg_replace(
                    '/((?:^use\s[^;]+;\n)+)/m',
                    "$1{$traitUse}\n",
                    $contents,
                    1
                );
            } else {
                // Insert after namespace declaration
                $contents = preg_replace(
                    '/^(namespace\s+[^;]+;)/m',
                    "$1\n\n{$traitUse}",
                    $contents,
                    1
                );
            }
        }

        // 2. Add `use SyncsToBigQuery;` inside the class body
        if (!str_contains($contents, 'use SyncsToBigQuery;')) {
            $contents = preg_replace(
                '/(\bclass\s+\w+[^{]*\{)/',
                "$1\n    use SyncsToBigQuery;\n{$traitProperties}\n",
                $contents,
                1
            );
        }

        file_put_contents($modelPath, $contents);
        $this->info("Model updated: {$modelPath}");
    }
}
