<?php

namespace Nonsapiens\BigqueryModelSync;

use Illuminate\Support\ServiceProvider;
use Nonsapiens\BigqueryModelSync\Commands\SetModelCommand;
use Nonsapiens\BigqueryModelSync\Commands\MakeBigQueryTableCommand;
use Nonsapiens\BigqueryModelSync\Commands\SyncModelsCommand;
use Nonsapiens\BigqueryModelSync\Commands\MakeBigQueryModelMigrationCommand;

class BigqueryModelSyncServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');

        if ($this->app->runningInConsole()) {
            $this->commands([
                SetModelCommand::class,
                MakeBigQueryTableCommand::class,
                SyncModelsCommand::class,
                MakeBigQueryModelMigrationCommand::class,
            ]);

            $this->publishes([
                __DIR__ . '/../database/migrations/' => database_path('migrations'),
            ], 'bigquery-model-sync-migrations');
        }
    }
}
