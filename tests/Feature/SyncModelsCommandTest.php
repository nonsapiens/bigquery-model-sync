<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Carbon\Carbon;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Mockery;
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;

class SyncModelsCommandTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('cmd_test_models', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->string('sync_batch_uuid', 36)->nullable();
            $table->timestamps();
        });

        $migration = include __DIR__ . '/../../database/migrations/2026_04_03_000000_create_bigquery_syncs_table.php';
        $migration->up();
    }

    protected function getEnvironmentSetUp($app)
    {
        $app['config']->set('bigquery.projectId', 'test-project');
        $app['config']->set('bigquery.dataset', 'test_dataset');
        $app['config']->set('bigquery.key_file_path', 'test-key.json');
        $app['config']->set('database.default', 'testbench');
        $app['config']->set('database.connections.testbench', [
            'driver'   => 'sqlite',
            'database' => ':memory:',
            'prefix'   => '',
        ]);
    }

    public function test_skips_when_schedule_not_due()
    {
        // Set schedule to run at specific minute; now set to a non-matching minute
        Carbon::setTestNow(Carbon::create(2026, 4, 9, 1, 1, 0));

        // Ensure there is some data
        DB::table('cmd_test_models')->insert([
            ['name' => 'A'],
        ]);

        $this->artisan('bigquery:sync', ['--class' => CmdTestModel::class, '--schedule' => '0 0 * * *'])
            ->expectsOutputToContain('Schedule not due')
            ->assertExitCode(0);

        Carbon::setTestNow();
    }

    public function test_runs_when_schedule_due_and_inserts()
    {
        DB::table('cmd_test_models')->insert([
            ['name' => 'One'],
            ['name' => 'Two'],
        ]);

        // Schedule every minute
        Carbon::setTestNow(Carbon::create(2026, 4, 9, 1, 0, 0));

        $mockBigQuery = Mockery::mock('overload:Google\\Cloud\\BigQuery\\BigQueryClient');
        $mockDataset = Mockery::mock('Google\\Cloud\\BigQuery\\Dataset');
        $mockTable = Mockery::mock('Google\\Cloud\\BigQuery\\Table');
        $mockResponse = Mockery::mock('Google\\Cloud\\BigQuery\\InsertResponse');

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('cmd_test_models')->andReturn($mockTable);
        $mockTable->shouldReceive('insertRows')->andReturn($mockResponse);
        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        $this->artisan('bigquery:sync', ['--class' => CmdTestModel::class])
            ->expectsOutputToContain('Running BATCH sync')
            ->expectsOutputToContain('Sync completed')
            ->assertExitCode(0);

        Carbon::setTestNow();
    }

    public function test_runs_replace_sync_when_strategy_is_replace()
    {
        Schema::create('cmd_replace_models', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->timestamps();
        });

        DB::table('cmd_replace_models')->insert([
            ['name' => 'Replacement'],
        ]);

        Carbon::setTestNow(Carbon::create(2026, 4, 9, 1, 0, 0));

        $mockBigQuery = Mockery::mock('overload:Google\\Cloud\\BigQuery\\BigQueryClient');
        $mockDataset = Mockery::mock('Google\\Cloud\\BigQuery\\Dataset');
        $mockTable = Mockery::mock('Google\\Cloud\\BigQuery\\Table');
        $mockResponse = Mockery::mock('Google\\Cloud\\BigQuery\\InsertResponse');

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('cmd_replace_models')->andReturn($mockTable);
        
        $mockQueryConfig = Mockery::mock(\Google\Cloud\BigQuery\QueryJobConfiguration::class);
        $mockBigQuery->shouldReceive('query')->once()->andReturn($mockQueryConfig);
        $mockBigQuery->shouldReceive('runQuery')->with($mockQueryConfig)->once();
        $mockTable->shouldReceive('insertRows')->andReturn($mockResponse);
        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        $this->artisan('bigquery:sync', ['--class' => CmdReplaceModel::class])
            ->expectsOutputToContain('Running REPLACE sync')
            ->expectsOutputToContain('Sync completed')
            ->assertExitCode(0);

        Carbon::setTestNow();
    }
}

class CmdReplaceModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'cmd_replace_models';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->syncStrategy = BigQuerySyncStrategy::REPLACE;
        $this->fieldsToSync = ['id', 'name'];
        $this->syncSchedule = '* * * * *';
    }
}

class CmdTestModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'cmd_test_models';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->fieldsToSync = ['id', 'name'];
        $this->syncSchedule = '* * * * *';
        $this->batchSize = 50;
    }
}
