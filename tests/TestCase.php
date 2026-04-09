<?php

namespace Nonsapiens\BigqueryModelSync\Tests;

use Nonsapiens\BigqueryModelSync\BigqueryModelSyncServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

class TestCase extends Orchestra
{
    protected function getPackageProviders($app)
    {
        return [
            BigqueryModelSyncServiceProvider::class,
        ];
    }

    protected function defineDatabaseMigrations()
    {
        // No migrations needed for now, but we'll mock Schema and DB
    }
}
