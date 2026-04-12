<?php

namespace Nonsapiens\BigqueryModelSync;

use Illuminate\Support\ServiceProvider;
use Illuminate\Console\Scheduling\Schedule;
use Nonsapiens\BigqueryModelSync\Commands\SetModelCommand;
use Nonsapiens\BigqueryModelSync\Commands\MakeBigQueryTableCommand;
use Nonsapiens\BigqueryModelSync\Commands\SyncModelsCommand;
use Nonsapiens\BigqueryModelSync\Commands\SyncAllModelsCommand;
use Nonsapiens\BigqueryModelSync\Commands\MakeBigQueryModelMigrationCommand;

class BigqueryModelSyncServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/bigquery.php', 'bigquery');
    }

    public function boot(): void
    {
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');

        if ($this->app->runningInConsole()) {
            $this->commands([
                SetModelCommand::class,
                MakeBigQueryTableCommand::class,
                SyncModelsCommand::class,
                SyncAllModelsCommand::class,
                MakeBigQueryModelMigrationCommand::class,
            ]);

            $this->publishes([
                __DIR__ . '/../database/migrations/' => database_path('migrations'),
            ], 'bigquery-model-sync-migrations');

            $this->publishes([
                __DIR__ . '/../config/bigquery.php' => config_path('bigquery.php'),
            ], 'bigquery-model-sync-config');

            $this->app->booted(function () {
                $schedule = $this->app->make(Schedule::class);
                if (config('bigquery.autosync')) {
                    $schedule->command(SyncAllModelsCommand::class)->everyMinute();
                }
            });
        }
    }
}
