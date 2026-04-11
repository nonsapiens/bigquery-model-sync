<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\Table;
use Google\Cloud\BigQuery\InsertResponse;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Schema;
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;
use Nonsapiens\BigqueryModelSync\Jobs\SyncRecordJob;
use Nonsapiens\BigqueryModelSync\Strategies\OnInsertSyncStrategy;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use Mockery;

class OnInsertSyncStrategyTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('test_on_insert_models', function (Blueprint $table) {
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

    public function test_on_insert_strategy_dispatches_job_when_model_is_created()
    {
        Queue::fake();

        TestOnInsertModel::create(['name' => 'New Record']);

        Queue::assertPushed(SyncRecordJob::class, function ($job) {
            return $job->getModelClass() === TestOnInsertModel::class && $job->model->name === 'New Record';
        });
    }

    public function test_on_insert_sync_inserts_single_record()
    {
        // 1. Mock BigQuery FIRST
        $mockBigQuery = Mockery::mock('overload:' . BigQueryClient::class);
        $mockDataset = Mockery::mock(Dataset::class);
        $mockTable = Mockery::mock(Table::class);
        $mockResponse = Mockery::mock(InsertResponse::class);

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('test_on_insert_models')->andReturn($mockTable);

        // Expect insert
        $mockTable->shouldReceive('insertRows')->withAnyArgs()->andReturn($mockResponse)->atLeast()->once();

        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        // 2. Prepare data
        $model = TestOnInsertModel::create(['name' => 'Record to Sync']);

        // 3. Sync manually if needed (actually boot already dispatched it if we use real queue)
        // In this test we want to verify the database record after sync.
        // Let's call $model->sync() directly.
        $model->sync();
        $this->assertDatabaseHas('bigquery_syncs', [
            'model' => TestOnInsertModel::class,
            'sync_type' => BigQuerySyncStrategy::ON_INSERT->value,
            'status' => 'completed',
            'records_synced' => 1,
        ]);
    }
}

class TestOnInsertModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'test_on_insert_models';
    protected $fillable = ['name'];

    protected BigQuerySyncStrategy $syncStrategy = BigQuerySyncStrategy::ON_INSERT;
    protected array $fieldsToSync = ['id', 'name'];
}
