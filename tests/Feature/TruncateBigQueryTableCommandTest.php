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
        parent::getEnvironmentSetUp($app);
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

        $mockDataset = Mockery::mock(\Google\Cloud\BigQuery\Dataset::class);
        $mockTable = Mockery::mock(\Google\Cloud\BigQuery\Table::class);
        $mockJob = Mockery::mock(\Google\Cloud\BigQuery\Job::class);

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('test_table_1')->andReturn($mockTable);

        $mockTable->shouldReceive('load')->once()->with('', Mockery::on(function ($options) {
            return $options['configuration']['load']['writeDisposition'] === 'WRITE_TRUNCATE';
        }))->andReturn($mockJob);

        $mockJob->shouldReceive('reload')->atLeast()->once();
        $mockJob->shouldReceive('isComplete')->andReturn(true);
        $mockJob->shouldReceive('info')->andReturn(['status' => ['state' => 'DONE']]);
        
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
