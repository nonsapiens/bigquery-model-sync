<?php

namespace Nonsapiens\BigqueryModelSync\Commands;

use Illuminate\Console\Command;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Process;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use Cron\CronExpression;
use Carbon\Carbon;
use ReflectionClass;
use Symfony\Component\Finder\Finder;

class SyncAllModelsCommand extends Command
{
    protected $signature = 'bigquery:sync-all {--force}';

    protected $description = 'Analyse all SyncsToBigQuery traited classes and run due syncs in parallel. Use --force to run all syncs regardless of schedule.';

    public function handle(): int
    {
        $this->logInfo('Starting bigquery:sync-all command');
        $models = $this->discoverModelsWithSchedule();

        if (empty($models)) {
            $this->info('No models with $syncSchedule found.');
            $this->logInfo('No models with $syncSchedule found.');
            return self::SUCCESS;
        }

        $this->logInfo('Discovered models for sync: ' . implode(', ', $models));

        $now = Carbon::now();
        $modelsToSync = [];

        foreach ($models as $fqcn) {
            $this->logInfo("Checking schedule for {$fqcn}");
            /** @var Model|SyncsToBigQuery $model */
            $model = new $fqcn();
            $schedule = $model->bigQuerySyncSchedule();

            if (!$schedule) {
                $this->logInfo("No schedule found for {$fqcn}, skipping.");
                continue;
            }

            try {
                if ($this->option('force')) {
                    $this->logInfo("Model {$fqcn} is being forced to sync");
                    $modelsToSync[] = $fqcn;
                    continue;
                }

                $cron = CronExpression::factory($schedule);
                if ($cron->isDue($now)) {
                    $this->logInfo("Model {$fqcn} is due for sync (schedule: {$schedule})");
                    $modelsToSync[] = $fqcn;
                } else {
                    $this->logInfo("Model {$fqcn} is not due for sync (schedule: {$schedule})");
                }
            } catch (\Throwable $e) {
                $this->error("Invalid cron expression for {$fqcn}: {$schedule}");
                $this->logError("Invalid cron expression for {$fqcn}: {$schedule}");
            }
        }

        if (empty($modelsToSync)) {
            $this->info('No models due for sync at this time.');
            $this->logInfo('No models due for sync at this time.');
            return self::SUCCESS;
        }

        $this->info('Syncing models: ' . implode(', ', $modelsToSync));
        $this->logInfo('Syncing models: ' . implode(', ', $modelsToSync));

        $results = [];
        foreach ($modelsToSync as $fqcn) {
            $this->logInfo("Starting sync for {$fqcn}");
            $command = "php artisan bigquery:sync --class=\"{$fqcn}\" --force";
            $results[$fqcn] = Process::path(base_path())->run($command);
        }

        $exitCode = self::SUCCESS;
        foreach ($results as $fqcn => $result) {
            if ($result->failed()) {
                $this->error("Failed to sync {$fqcn}: " . $result->errorOutput());
                $this->logError("Failed to sync {$fqcn} during bigquery:sync-all", [
                    'exit_code' => $result->exitCode(),
                    'output' => $result->output(),
                    'error' => $result->errorOutput(),
                ]);
                $exitCode = self::FAILURE;
            } else {
                $this->info("Successfully synced {$fqcn}.");
                $this->logInfo("Successfully synced {$fqcn}.");
            }
        }

        $this->logInfo('Finished bigquery:sync-all command');

        return (int) $exitCode;
    }

    /**
     * Log info if logging is enabled in config.
     */
    protected function logInfo(string $message, array $context = []): void
    {
        if (config('bigquery.logging', true)) {
            Log::info($message, $context);
        }
    }

    /**
     * Log error if logging is enabled in config.
     */
    protected function logError(string $message, array $context = []): void
    {
        if (config('bigquery.logging', true)) {
            Log::error($message, $context);
        }
    }

    /**
     * Discover all models using the SyncsToBigQuery trait.
     * Looks at config('bigquery.syncable-namespaces').
     *
     * @return array<string>
     */
    protected function discoverModelsWithSchedule(): array
    {
        $models = config('bigquery.models');

        if (is_array($models)) {
            return $models;
        }

        $syncableNamespaces = config('bigquery.syncable-namespaces', []);

        if (empty($syncableNamespaces)) {
            $syncableNamespaces = ['App\\Models\\'];
        }

        $models = [];
        $autoloadPath = base_path('vendor/autoload.php');
        if (!file_exists($autoloadPath)) {
            $autoloadPath = __DIR__ . '/../../vendor/autoload.php';
        }

        if (!file_exists($autoloadPath)) {
            return [];
        }

        $composer = require $autoloadPath;
        if (!($composer instanceof \Composer\Autoload\ClassLoader)) {
            // In some environments, requiring autoload.php returns the ClassLoader, in others not.
            // Try to find it in registered autoloaders if not returned directly.
            foreach (spl_autoload_functions() as $function) {
                if (is_array($function) && $function[0] instanceof \Composer\Autoload\ClassLoader) {
                    $composer = $function[0];
                    break;
                }
            }
        }

        if (!($composer instanceof \Composer\Autoload\ClassLoader)) {
            return [];
        }

        $prefixes = $composer->getPrefixesPsr4();

        // Check class map for classes in the target namespaces
        $classMap = $composer->getClassMap();
        foreach ($classMap as $class => $file) {
            foreach ($syncableNamespaces as $ns) {
                if (str_starts_with($class, $ns)) {
                    try {
                        if ($this->usesSyncsToBigQueryTrait($class)) {
                            $models[] = $class;
                        }
                    } catch (\Throwable $e) {}
                    continue 2;
                }
            }
        }

        foreach ($prefixes as $namespace => $paths) {
            $matchedNamespace = false;
            foreach ($syncableNamespaces as $ns) {
                if (str_starts_with($namespace, $ns) || str_starts_with($ns, $namespace)) {
                    $matchedNamespace = true;
                    break;
                }
            }

            if (!$matchedNamespace) {
                continue;
            }

            foreach ($paths as $path) {
                if (!is_dir($path)) {
                    continue;
                }

                $finder = new Finder();
                $finder->files()->in($path)->name('*.php');

                foreach ($finder as $file) {
                    $fqcns = $this->getFullyQualifiedClassNamesFromFile($file->getRealPath());
                    foreach ($fqcns as $fqcn) {
                        // Double check the FQCN matches one of our target namespaces
                        $matchesTarget = false;
                        foreach ($syncableNamespaces as $ns) {
                            if (str_starts_with($fqcn, $ns)) {
                                $matchesTarget = true;
                                break;
                            }
                        }

                        if (!$matchesTarget) {
                            continue;
                        }

                        try {
                            if (class_exists($fqcn) && $this->usesSyncsToBigQueryTrait($fqcn)) {
                                $models[] = $fqcn;
                            }
                        } catch (\Throwable $e) {
                            // Skip classes that fail to load (e.g. missing extensions)
                            continue;
                        }
                    }
                }
            }
        }

        return array_unique($models);
    }

    protected function getFullyQualifiedClassNamesFromFile(string $path): array
    {
        $content = file_get_contents($path);
        $namespace = null;
        if (preg_match('/namespace\s+([^;]+);/', $content, $matches)) {
            $namespace = trim($matches[1]);
        }

        $classes = [];
        if (preg_match_all('/class\s+(\w+)/', $content, $matches)) {
            foreach ($matches[1] as $class) {
                $classes[] = $namespace ? $namespace . '\\' . $class : $class;
            }
        }

        return $classes;
    }

    protected function usesSyncsToBigQueryTrait(string $class): bool
    {
        $reflection = new ReflectionClass($class);
        return in_array(SyncsToBigQuery::class, $this->getTraitsRecursively($reflection), true);
    }

    protected function getTraitsRecursively(ReflectionClass $reflection): array
    {
        $traits = $reflection->getTraitNames();
        if ($parent = $reflection->getParentClass()) {
            $traits = array_merge($traits, $this->getTraitsRecursively($parent));
        }
        return array_unique($traits);
    }
}
