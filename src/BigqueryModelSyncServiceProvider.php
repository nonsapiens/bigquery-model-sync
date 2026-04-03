<?php

namespace Nonsapiens\BigqueryModelSync;

use Illuminate\Support\ServiceProvider;
use Nonsapiens\BigqueryModelSync\Commands\SetModelCommand;

class BigqueryModelSyncServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                SetModelCommand::class,
            ]);
        }
    }
}
