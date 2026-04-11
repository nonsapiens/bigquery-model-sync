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
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;
use Nonsapiens\BigqueryModelSync\Models\BigQuerySync;
use Nonsapiens\BigqueryModelSync\Strategies\ReplaceSyncStrategy;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use Mockery;

class ReplaceSyncStrategyTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('test_replace_models', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->timestamps();
        });

        $migration = include __DIR__ . '/../../database/migrations/2026_04_03_000000_create_bigquery_syncs_table.php';
        $migration->up();
    }

    protected function getEnvironmentSetUp($app)
    {
        $app['config']->set('bigquery.projectId', 'test-project');
        $app['config']->set('bigquery.dataset', 'test_dataset');
        $app['config']->set('database.default', 'testbench');
        $app['config']->set('database.connections.testbench', [
            'driver'   => 'sqlite',
            'database' => ':memory:',
            'prefix'   => '',
        ]);
    }

    public function test_replace_sync_truncates_and_inserts_all_records()
    {
        // 1. Prepare data
        DB::table('test_replace_models')->insert([
            ['name' => 'Record 1'],
            ['name' => 'Record 2'],
        ]);

        $model = new TestReplaceModel();

        // 2. Mock BigQuery
        $mockBigQuery = Mockery::mock('overload:' . BigQueryClient::class);
        $mockDataset = Mockery::mock(Dataset::class);
        $mockTable = Mockery::mock(Table::class);
        $mockResponse = Mockery::mock(InsertResponse::class);

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('test_replace_models')->andReturn($mockTable);

        // Expect truncation
        $mockQueryConfig = Mockery::mock(\Google\Cloud\BigQuery\QueryJobConfiguration::class);
        $mockBigQuery->shouldReceive('query')
            ->with("DELETE FROM `test_dataset.test_replace_models` WHERE 1=1")
            ->once()
            ->andReturn($mockQueryConfig);

        $mockBigQuery->shouldReceive('runQuery')
            ->with($mockQueryConfig)
            ->once();

        // Expect insert
        $mockTable->shouldReceive('insertRows')->withArgs(function ($rows) {
            return count($rows) === 2 &&
                   $rows[0]['data']['name'] === 'Record 1' &&
                   $rows[1]['data']['name'] === 'Record 2';
        })->andReturn($mockResponse);

        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        // 3. Execute Sync
        $model->sync();

        // 4. Assertions
        $this->assertDatabaseHas('bigquery_syncs', [
            'model' => TestReplaceModel::class,
            'sync_type' => BigQuerySyncStrategy::REPLACE->value,
            'status' => 'completed',
            'records_synced' => 2,
        ]);
    }
}

class TestReplaceModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'test_replace_models';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->syncStrategy = BigQuerySyncStrategy::REPLACE;
        $this->fieldsToSync = ['id', 'name'];
    }
}
