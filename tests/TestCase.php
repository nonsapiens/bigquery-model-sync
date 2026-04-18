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

    protected function getEnvironmentSetUp($app)
    {
        // Create a dummy credentials file for BigQueryClient singleton
        $credentialsPath = tempnam(sys_get_temp_dir(), 'bq_creds');
        file_put_contents($credentialsPath, json_encode([
            'type' => 'external_account',
            'audience' => '//iam.googleapis.com/projects/1234567890/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID',
            'subject_token_type' => 'urn:ietf:params:oauth:token-type:jwt',
            'token_url' => 'https://sts.googleapis.com/v1/token',
            'credential_source' => [
                'file' => '/var/run/secrets/token',
            ],
        ]));
        $app['config']->set('bigquery.credentials', $credentialsPath);
    }

    protected function defineDatabaseMigrations()
    {
        // No migrations needed for now, but we'll mock Schema and DB
    }
}
