<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Config;
use Mockery;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;

class TruncateBigQueryTableCommandTest extends TestCase
{
    protected function getEnvironmentSetUp($app)
    {
        $app['config']->set('bigquery.projectId', 'test-project');
        $app['config']->set('bigquery.dataset', 'test_dataset');
    }

    public function test_it_requires_all_or_class_option()
    {
        $this->artisan('bigquery:truncate')
            ->expectsOutput('Please specify either --all or --class=.')
            ->assertExitCode(0);
    }

    public function test_it_truncates_specified_classes()
    {
        Config::set('bigquery.models', [
            TruncateTestModel1::class,
        ]);

        \Illuminate\Support\Str::createRandomStringsUsing(fn () => 'RANDOM');

        $mockBigQuery = Mockery::mock(BigQueryClient::class);
        $this->app->instance(BigQueryClient::class, $mockBigQuery);

        $mockQueryConfig = Mockery::mock(\Google\Cloud\BigQuery\JobConfigurationInterface::class);

        $mockBigQuery->shouldReceive('query')
            ->once()
            ->with("DELETE FROM `test_dataset.test_table_1` WHERE 1=1")
            ->andReturn($mockQueryConfig);

        $mockBigQuery->shouldReceive('runQuery')
            ->once()
            ->with($mockQueryConfig);
        
        $this->artisan('bigquery:truncate', ['--class' => TruncateTestModel1::class])
            ->expectsOutput('The following models will have their BigQuery tables truncated:')
            ->expectsOutput(' - Nonsapiens\BigqueryModelSync\Tests\Feature\TruncateTestModel1')
            ->expectsConfirmation('Are you sure you want to truncate these tables?', 'yes')
            ->expectsOutputToContain('To confirm, please type the following code: RANDOM')
            ->expectsQuestion('Verification code', 'RANDOM') 
            ->assertExitCode(0);
        
        \Illuminate\Support\Str::createRandomStringsNormally();
    }

    public function test_it_cancels_on_confirmation_decline()
    {
        $this->artisan('bigquery:truncate', ['--class' => TruncateTestModel1::class])
            ->expectsOutput('The following models will have their BigQuery tables truncated:')
            ->expectsOutput(' - Nonsapiens\BigqueryModelSync\Tests\Feature\TruncateTestModel1')
            ->expectsConfirmation('Are you sure you want to truncate these tables?', 'no')
            ->expectsOutput('Truncate cancelled.')
            ->assertExitCode(0);
    }

    public function test_it_cancels_on_wrong_verification_code()
    {
        $this->artisan('bigquery:truncate', ['--class' => TruncateTestModel1::class])
            ->expectsConfirmation('Are you sure you want to truncate these tables?', 'yes')
            ->expectsQuestion('Verification code', 'WRONG123')
            ->expectsOutput('Verification code mismatch. Truncate cancelled.')
            ->assertExitCode(1);
    }
}

class TruncateTestModel1 extends Model
{
    use SyncsToBigQuery;
    protected $table = 'test_table_1';
}

class TruncateTestModel2 extends Model
{
    use SyncsToBigQuery;
    protected $table = 'test_table_2';
}
