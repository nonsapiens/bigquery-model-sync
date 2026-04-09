<?php

namespace Nonsapiens\BigqueryModelSync;

use Illuminate\Support\ServiceProvider;
use Nonsapiens\BigqueryModelSync\Commands\SetModelCommand;
use Nonsapiens\BigqueryModelSync\Commands\MakeBigQueryTableCommand;
use Nonsapiens\BigqueryModelSync\Commands\SyncModelsCommand;

class BigqueryModelSyncServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                SetModelCommand::class,
                MakeBigQueryTableCommand::class,
                SyncModelsCommand::class,
            ]);

            $this->publishes([
                __DIR__ . '/Migrations/' => database_path('migrations'),
            ], 'bigquery-model-sync-migrations');
        }
    }
}
