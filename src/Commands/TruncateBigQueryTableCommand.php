<?php

namespace Nonsapiens\BigqueryModelSync\Commands;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Console\Command;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Str;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use ReflectionClass;
use Symfony\Component\Finder\Finder;

class TruncateBigQueryTableCommand extends Command
{
    protected $signature = 'bigquery:truncate {--all} {--class=}';

    protected $description = 'Truncate one or many BigQuery tables associated with SyncsToBigQuery models.';

    public function handle(BigQueryClient $bigQuery): int
    {
        $models = $this->getModelsToTruncate();

        if (empty($models)) {
            $this->warn('No models found to truncate.');
            return self::SUCCESS;
        }

        $this->info('The following models will have their BigQuery tables truncated:');
        foreach ($models as $model) {
            $this->line(" - {$model}");
        }

        if (!$this->confirm('Are you sure you want to truncate these tables?', false)) {
            $this->warn(' ❌ Truncate cancelled.');
            return self::SUCCESS;
        }

        $verificationCode = strtoupper(Str::random(6));
        $this->warn("To confirm, please type the following code: {$verificationCode}");
        
        $inputCode = $this->ask('Verification code');

        if (strtoupper($inputCode) !== $verificationCode) {
            $this->error('Verification code mismatch. Truncate cancelled.');
            return self::FAILURE;
        }

        $projectName = config('bigquery.projectId');
        $datasetId = config('bigquery.dataset');

        foreach ($models as $fqcn) {
            /** @var Model|SyncsToBigQuery $modelInstance */
            $modelInstance = new $fqcn();
            $tableName = $modelInstance->bigQueryTableName() ?? $modelInstance->getTable();

            $this->components->task("Truncating {$tableName}", function () use ($bigQuery, $datasetId, $tableName, $projectName) {
                $queryConfig = $bigQuery->query("TRUNCATE TABLE `{$projectName}.{$datasetId}.{$tableName}`");
                $bigQuery->runQuery($queryConfig);
                return true;
            });
        }

        $this->info('Truncate completed.');

        return self::SUCCESS;
    }

    protected function getModelsToTruncate(): array
    {
        if ($this->option('all')) {
            return $this->discoverAllModels();
        }

        $classOption = $this->option('class');
        if ($classOption) {
            $classes = explode(',', $classOption);
            $validatedClasses = [];
            foreach ($classes as $class) {
                $class = trim($class);
                if (!class_exists($class)) {
                    $this->error("Class {$class} not found.");
                    continue;
                }
                if (!$this->usesSyncsToBigQueryTrait($class)) {
                    $this->error("Class {$class} does not use the SyncsToBigQuery trait.");
                    continue;
                }
                $validatedClasses[] = $class;
            }
            return $validatedClasses;
        }

        $this->error('Please specify either --all or --class=.');
        return [];
    }

    protected function discoverAllModels(): array
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
                        $matchesTarget = false;
                        foreach ($syncableNamespaces as $ns) {
                            if (str_starts_with($fqcn, $ns)) {
                                $matchesTarget = true;
                                break;
                            }
                        }
                        if ($matchesTarget) {
                            try {
                                if ($this->usesSyncsToBigQueryTrait($fqcn)) {
                                    $models[] = $fqcn;
                                }
                            } catch (\Throwable $e) {}
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
        $tokens = token_get_all($content);
        $classes = [];
        $namespace = '';
        for ($i = 0; $i < count($tokens); $i++) {
            if ($tokens[$i][0] === T_NAMESPACE) {
                $i += 2;
                while (isset($tokens[$i]) && is_array($tokens[$i]) && in_array($tokens[$i][0], [T_STRING, T_NS_SEPARATOR, T_NAME_QUALIFIED])) {
                    $namespace .= $tokens[$i][1];
                    $i++;
                }
            }
            if ($tokens[$i][0] === T_CLASS || $tokens[$i][0] === T_TRAIT) {
                $i += 2;
                if (isset($tokens[$i]) && is_array($tokens[$i]) && $tokens[$i][0] === T_STRING) {
                    $classes[] = $namespace . '\\' . $tokens[$i][1];
                }
            }
        }
        return $classes;
    }

    protected function usesSyncsToBigQueryTrait(string $class): bool
    {
        if (!class_exists($class)) {
            return false;
        }
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
