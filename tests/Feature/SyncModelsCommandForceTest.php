<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\Table;
use Google\Cloud\BigQuery\InsertResponse;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use Mockery;
use Carbon\Carbon;

class SyncModelsCommandForceTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('force_test_models', function (Blueprint $table) {
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

    public function test_sync_models_command_skips_when_not_due_without_force()
    {
        // Set fixed time
        Carbon::setTestNow(Carbon::create(2026, 4, 10, 16, 21)); // 16:21, not divisible by 5

        DB::table('force_test_models')->insert(['name' => 'Record 1']);

        $this->artisan('bigquery:sync', ['--class' => ForceTestModel::class])
            ->expectsOutput("Schedule not due for " . ForceTestModel::class . " at 2026-04-10 16:21:00 (*/5 * * * *). Skipping.")
            ->assertExitCode(0);

        $this->assertEquals(0, DB::table('force_test_models')->whereNotNull('sync_batch_uuid')->count());
        
        Carbon::setTestNow();
    }

    public function test_sync_models_command_runs_when_not_due_with_force()
    {
        // Set fixed time
        Carbon::setTestNow(Carbon::create(2026, 4, 10, 16, 21)); // 16:21, not divisible by 5

        DB::table('force_test_models')->insert(['name' => 'Record 1']);

        // Mock BigQuery to avoid actual network calls
        $mockBigQuery = Mockery::mock('overload:' . BigQueryClient::class);
        $mockDataset = Mockery::mock(Dataset::class);
        $mockTable = Mockery::mock(Table::class);
        $mockResponse = Mockery::mock(InsertResponse::class);

        $mockBigQuery->shouldReceive('dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->andReturn($mockTable);
        $mockTable->shouldReceive('insertRows')->andReturn($mockResponse);
        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        $this->artisan('bigquery:sync', [
            '--class' => ForceTestModel::class,
            '--force' => true
        ])
            ->expectsOutput("Running BATCH sync for " . ForceTestModel::class . "...")
            ->expectsOutput("Sync completed.")
            ->assertExitCode(0);

        $this->assertEquals(1, DB::table('force_test_models')->whereNotNull('sync_batch_uuid')->count());

        Carbon::setTestNow();
    }
}

class ForceTestModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'force_test_models';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->fieldsToSync = ['id', 'name'];
        $this->syncSchedule = '*/5 * * * *';
    }
}
