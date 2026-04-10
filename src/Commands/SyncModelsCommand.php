<?php

namespace Nonsapiens\BigqueryModelSync\Commands;

use Illuminate\Console\Command;
use Illuminate\Database\Eloquent\Model;
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;
use Nonsapiens\BigqueryModelSync\Jobs\SyncModelJob;
use Nonsapiens\BigqueryModelSync\Strategies\BatchSyncStrategy;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use Cron\CronExpression;
use Carbon\Carbon;
use ReflectionClass;

class SyncModelsCommand extends Command
{
    protected $signature = 'bigquery:sync {--class=} {--schedule=} {--queue} {--force}';

    protected $description = 'Run BigQuery sync for a model if its $syncSchedule is due (BATCH strategy supported). Use --force to sync even if not scheduled.';

    public function handle(): int
    {
        $fqcn = $this->option('class');

        if (!$fqcn) {
            $this->error('Please provide a model FQCN via --class=Your\\Model\\Class');
            return self::FAILURE;
        }

        if (!class_exists($fqcn)) {
            $this->error("Class {$fqcn} not found");
            return self::FAILURE;
        }

        /** @var Model|SyncsToBigQuery $model */
        $model = new $fqcn();

        if (!$model instanceof Model) {
            $this->error("Class {$fqcn} is not an Eloquent model.");
            return self::FAILURE;
        }

        if (!$this->usesSyncsToBigQueryTrait($fqcn)) {
            $this->error("Class {$fqcn} does not use the SyncsToBigQuery trait.");
            return self::FAILURE;
        }

        // Evaluate cron schedule
        $schedule = $this->option('schedule') ?: $model->bigQuerySyncSchedule();
        $now = Carbon::now();

        try {
            $cron = CronExpression::factory($schedule);
        } catch (\Throwable $e) {
            $this->error("Invalid cron expression on {$fqcn}: {$schedule}");
            return self::FAILURE;
        }

        if (!$this->option('force') && !$cron->isDue($now)) {
            $this->info("Schedule not due for {$fqcn} at {$now->toDateTimeString()} ({$schedule}). Skipping.");
            return self::SUCCESS;
        }

        // Select strategy
        $strategy = $model->bigQuerySyncStrategy();
        if ($strategy !== BigQuerySyncStrategy::BATCH) {
            $this->warn("Strategy {$strategy->value} not implemented yet. Skipping.");
            return self::SUCCESS;
        }

        if ($this->option('queue')) {
            $this->info("Dispatching BATCH sync for {$fqcn} to queue...");
            SyncModelJob::dispatch($fqcn);
            return self::SUCCESS;
        }

        $this->info("Running BATCH sync for {$fqcn}...");
        try {
            (new BatchSyncStrategy())->sync($model);
            $this->info('Sync completed.');
            return self::SUCCESS;
        } catch (\Throwable $e) {
            $this->error('Sync failed: ' . $e->getMessage());
            return self::FAILURE;
        }
    }

    private function usesSyncsToBigQueryTrait(string $class): bool
    {
        $reflection = new ReflectionClass($class);
        return in_array(SyncsToBigQuery::class, $this->getTraitsRecursively($reflection), true);
    }

    private function getTraitsRecursively(ReflectionClass $reflection): array
    {
        $traits = $reflection->getTraitNames();
        if ($parent = $reflection->getParentClass()) {
            $traits = array_merge($traits, $this->getTraitsRecursively($parent));
        }
        return array_unique($traits);
    }
}
