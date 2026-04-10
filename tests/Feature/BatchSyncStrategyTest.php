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
use Nonsapiens\BigqueryModelSync\Models\BigQuerySync;
use Nonsapiens\BigqueryModelSync\Strategies\BatchSyncStrategy;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use Mockery;

class BatchSyncStrategyTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('test_models', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->string('sync_batch_uuid', 36)->nullable();
            $table->decimal('latitude', 10, 6)->nullable();
            $table->decimal('longitude', 10, 6)->nullable();
            $table->date('some_date')->nullable();
            $table->datetime('some_datetime')->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();
        });

        // Use the migration file for bigquery_syncs
        $migration = include __DIR__ . '/../../src/Migrations/2026_04_03_000000_create_bigquery_syncs_table.php';
        $migration->up();
    }

    protected function getEnvironmentSetUp($app)
    {
        $app['config']->set('bigquery.project_id', 'test-project');
        $app['config']->set('bigquery.dataset_id', 'test_dataset');
        $app['config']->set('bigquery.key_file_path', 'test-key.json');
        $app['config']->set('database.default', 'testbench');
        $app['config']->set('database.connections.testbench', [
            'driver'   => 'sqlite',
            'database' => ':memory:',
            'prefix'   => '',
        ]);
    }

    public function test_batch_sync_claims_records_and_inserts_into_bigquery()
    {
        // 1. Prepare data
        DB::table('test_models')->insert([
            [
                'name' => 'Record 1',
                'latitude' => -33.8688,
                'longitude' => 151.2093,
                'some_date' => '2023-01-01',
                'some_datetime' => '2023-01-01 12:00:00',
                'metadata' => json_encode(['key' => 'value']),
            ],
            [
                'name' => 'Non-JSON string',
                'latitude' => null,
                'longitude' => null,
                'some_date' => null,
                'some_datetime' => null,
                'metadata' => 'Just a plain string',
            ],
        ]);

        $model = new TestModel();

        // 2. Mock BigQuery
        $mockBigQuery = Mockery::mock('overload:' . BigQueryClient::class);
        $mockDataset = Mockery::mock(Dataset::class);
        $mockTable = Mockery::mock(Table::class);
        $mockResponse = Mockery::mock(InsertResponse::class);

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('test_models')->andReturn($mockTable);

        $mockTable->shouldReceive('insertRows')->withArgs(function ($rows) {
            $data1 = $rows[0]['data'];
            $data2 = $rows[1]['data'];
            return count($rows) === 2 &&
                   $data1['name'] === 'Record 1' &&
                   $data1['metadata'] === ['key' => 'value'] &&
                   $data2['name'] === 'Non-JSON string' &&
                   $data2['metadata'] === 'Just a plain string';
        })->andReturn($mockResponse);

        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        // 3. Execute Sync
        $strategy = new BatchSyncStrategy();
        $strategy->sync($model);

        // 4. Assertions
        $this->assertEquals(2, DB::table('test_models')->whereNotNull('sync_batch_uuid')->count());
        $this->assertDatabaseHas('bigquery_syncs', [
            'model' => TestModel::class,
            'status' => 'completed',
            'records_synced' => 2,
        ]);

        $syncRecord = BigQuerySync::first();
        $this->assertNotNull($syncRecord->sync_batch_uuid);
        $this->assertNotNull($syncRecord->started_at);
        $this->assertNotNull($syncRecord->completed_at);

        $modelRecord = DB::table('test_models')->first();
        $this->assertEquals($syncRecord->sync_batch_uuid, $modelRecord->sync_batch_uuid);
    }

    public function test_batch_sync_handles_failure()
    {
        // 1. Prepare data
        DB::table('test_models')->insert([
            ['name' => 'Record 1'],
        ]);

        $model = new TestModel();

        // 2. Mock BigQuery Failure
        $mockBigQuery = Mockery::mock('overload:' . BigQueryClient::class);
        $mockDataset = Mockery::mock(Dataset::class);
        $mockTable = Mockery::mock(Table::class);
        $mockResponse = Mockery::mock(InsertResponse::class);

        $mockBigQuery->shouldReceive('dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->andReturn($mockTable);

        $mockTable->shouldReceive('insertRows')->andReturn($mockResponse);
        $mockResponse->shouldReceive('isSuccessful')->andReturn(false);
        $mockResponse->shouldReceive('failedRows')->andReturn([
            ['errors' => [['reason' => 'invalid', 'message' => 'Something went wrong']]]
        ]);

        // 3. Execute Sync and expect Exception
        $strategy = new BatchSyncStrategy();

        try {
            $strategy->sync($model);
            $this->fail('Expected exception was not thrown');
        } catch (\Exception $e) {
            $this->assertStringContainsString('BigQuery Insert Failed', $e->getMessage());
        }

        // 4. Assertions
        $this->assertDatabaseHas('bigquery_syncs', [
            'model' => TestModel::class,
            'status' => 'failed',
            'error_message' => 'BigQuery Insert Failed: invalid: Something went wrong',
        ]);
    }
}

class TestModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'test_models';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->fieldsToSync = ['id', 'name', 'some_date', 'some_datetime', 'metadata'];
        $this->hasGeodata = true;
        $this->geodataFields = ['latitude', 'longitude'];
        $this->mappedGeographyField = 'geolocation';
        $this->batchSize = 100;
    }
}
