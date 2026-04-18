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

class DataFormattingTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('format_test_models', function (Blueprint $table) {
            $table->id();
            $table->string('name')->nullable();
            $table->string('sync_batch_uuid', 36)->nullable();
            $table->json('metadata')->nullable();
            $table->timestamps();
        });

        $migration = include __DIR__ . '/../../database/migrations/2026_04_03_000000_create_bigquery_syncs_table.php';
        $migration->up();
    }

    protected function getEnvironmentSetUp($app)
    {
        parent::getEnvironmentSetUp($app);
        $app['config']->set('bigquery.projectId', 'test-project');
        $app['config']->set('bigquery.dataset', 'test_dataset');
        $app['config']->set('database.default', 'testbench');
        $app['config']->set('database.connections.testbench', [
            'driver'   => 'sqlite',
            'database' => ':memory:',
            'prefix'   => '',
        ]);
    }

    public function test_prepare_row_with_json_and_special_strings()
    {
        $mockBigQuery = Mockery::mock('overload:' . BigQueryClient::class);
        $mockDataset = Mockery::mock(Dataset::class);
        $mockTable = Mockery::mock(Table::class);
        $mockResponse = Mockery::mock(InsertResponse::class);

        $mockBigQuery->shouldReceive('dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->andReturn($mockTable);
        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        $capturedRows = [];
        $mockTable->shouldReceive('insertRows')->withArgs(function ($rows) use (&$capturedRows) {
            $capturedRows = $rows;
            return true;
        })->andReturn($mockResponse);

        // Scenario 1: JSON field with array
        DB::table('format_test_models')->insert([
            'name' => 'JSON Array',
            'metadata' => json_encode(['foo' => 'bar']),
        ]);

        $model = new FormatTestModel();
        $model->sync();

        $this->assertCount(1, $capturedRows);
        // If metadata is cast as array, prepareRow should json_encode it if it's not a string.
        // But wait, DB::table()->insert() inserts a string into the DB.
        // When we retrieve it, if it's not cast in the model, it stays a string.
        
        // Let's check what prepareRow does.
        // It uses (array) $record. $record comes from DB::table()->chunk().
        // So $record is a stdClass object where 'metadata' is likely a string (from sqlite).

        $this->assertEquals('{"foo":"bar"}', $capturedRows[0]['data']['metadata']);

        // Scenario 2: String that looks like a date but isn't quite (or is)
        DB::table('format_test_models')->truncate();
        DB::table('format_test_models')->insert([
            'name' => 'Date-like string',
            'metadata' => '2023-01-01',
        ]);
        
        $model->sync();
        // 2023-01-01 matches the regex ^\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?$
        // We now keep it as a string to avoid misinterpretation or incorrect formatting by the library.
        $this->assertEquals('2023-01-01', $capturedRows[0]['data']['metadata']);

        // Scenario 3: String starting with 'C' that might be misinterpreted
        DB::table('format_test_models')->truncate();
        DB::table('format_test_models')->insert([
            'name' => 'C-string',
            'metadata' => 'Configuration',
        ]);
        $model->sync();
        $this->assertEquals('Configuration', $capturedRows[0]['data']['metadata']);

        // Scenario 4: String that looks like a date but is actually a complex string
        DB::table('format_test_models')->truncate();
        DB::table('format_test_models')->insert([
            'name' => 'Complex string',
            'metadata' => '2023-01-01-Something',
        ]);
        $model->sync();
        $this->assertEquals('2023-01-01-Something', $capturedRows[0]['data']['metadata']);
    }
}

class FormatTestModel extends Model
{
    use SyncsToBigQuery;
    protected $table = 'format_test_models';
    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->fieldsToSync = ['id', 'name', 'metadata'];
        $this->casts = ['metadata' => 'array'];
    }
}
