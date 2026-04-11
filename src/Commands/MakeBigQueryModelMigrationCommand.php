<?php

namespace Nonsapiens\BigqueryModelSync\Commands;

use Illuminate\Console\Command;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;
use ReflectionClass;

class MakeBigQueryModelMigrationCommand extends Command
{
    protected $signature = 'make:bigquery-model-migration {--class= : The model class to create a migration for}';

    protected $description = 'Create a migration to add sync fields to a model';

    public function handle(): int
    {
        $modelClass = $this->option('class');

        if (!$modelClass) {
            $this->error('The --class option is required.');
            return self::FAILURE;
        }

        if (!class_exists($modelClass)) {
            $this->error("Model class '{$modelClass}' not found.");
            return self::FAILURE;
        }

        $reflection = new ReflectionClass($modelClass);
        if (!$reflection->isSubclassOf(Model::class)) {
            $this->error("Class '{$modelClass}' is not an Eloquent model.");
            return self::FAILURE;
        }

        /** @var Model|SyncsToBigQuery $instance */
        $instance = new $modelClass();
        $table = $instance->getTable();
        $batchField = $instance->bigQueryBatchField();

        if ($instance->bigQuerySyncStrategy() === \Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy::REPLACE) {
            $this->warn("Model '{$modelClass}' uses the REPLACE strategy, which does not require a batch field.");
            if (!$this->confirm("Do you still want to create the migration for '{$batchField}'?", false)) {
                return self::SUCCESS;
            }
        }

        $this->createMigration($table, $batchField);

        return self::SUCCESS;
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

        if (!is_dir(database_path('migrations'))) {
            mkdir(database_path('migrations'), 0755, true);
        }

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
}
