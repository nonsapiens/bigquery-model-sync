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

class NoIdBatchSyncTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('no_id_models', function (Blueprint $table) {
            $table->string('role_id');
            $table->string('model_id');
            $table->string('model_type');
            $table->string('sync_batch_uuid', 36)->nullable();
        });

        // Use the migration file for bigquery_syncs
        $migration = include __DIR__ . '/../../database/migrations/2026_04_03_000000_create_bigquery_syncs_table.php';
        $migration->up();
    }

    protected function getEnvironmentSetUp($app)
    {
        parent::getEnvironmentSetUp($app);
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

    public function test_batch_sync_works_on_table_without_id_field()
    {
        DB::listen(function($query) {
             if (str_contains(strtolower($query->sql), 'order by "id"')) {
                 throw new \Exception("Found ORDER BY id in query: " . $query->sql);
             }
             if (str_contains(strtolower($query->sql), 'order by id')) {
                 throw new \Exception("Found ORDER BY id in query: " . $query->sql);
             }
        });

        // 1. Prepare data
        DB::table('no_id_models')->insert([
            [
                'role_id' => '1',
                'model_id' => '100',
                'model_type' => 'App\Models\User',
            ],
        ]);

        $model = new NoIdModel();

        // Test with BATCH strategy
        $model->syncStrategy = \Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy::BATCH;

        // 2. Mock BigQuery
        $mockBigQuery = Mockery::mock('overload:' . \Google\Cloud\BigQuery\BigQueryClient::class);
        $mockDataset = Mockery::mock(\Google\Cloud\BigQuery\Dataset::class);
        $mockTable = Mockery::mock(\Google\Cloud\BigQuery\Table::class);
        $mockResponse = Mockery::mock(\Google\Cloud\BigQuery\InsertResponse::class);

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('no_id_models')->andReturn($mockTable);

        $mockTable->shouldReceive('insertRows')->once()->andReturn($mockResponse);
        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        // 3. Execute Sync
        $model->sync();

        // 4. Assertions
        $this->assertEquals(1, DB::table('no_id_models')->whereNotNull('sync_batch_uuid')->count());
        $this->assertDatabaseHas('bigquery_syncs', [
            'model' => NoIdModel::class,
            'status' => 'completed',
            'records_synced' => 1,
        ]);
    }
}

class NoIdModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'no_id_models';
    public $incrementing = false;
    protected $keyType = 'string';
    protected $primaryKey = 'id';
    public $timestamps = false;

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->batchSize = 100;
    }
}
