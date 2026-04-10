<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Carbon\Carbon;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Queue;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;
use Nonsapiens\BigqueryModelSync\Jobs\SyncModelJob;
use Mockery;

class SyncModelsQueueTest extends TestCase
{
    protected function defineDatabaseMigrations()
    {
        Schema::create('queue_test_models', function (Blueprint $table) {
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

    public function test_dispatches_job_to_queue_when_option_is_present()
    {
        Queue::fake();
        Carbon::setTestNow(Carbon::create(2026, 4, 9, 1, 0, 0));

        $this->artisan('bigquery:sync', [
            '--class' => QueueTestModel::class,
            '--queue' => true
        ])
            ->expectsOutputToContain('Dispatching BATCH sync for Nonsapiens\BigqueryModelSync\Tests\Feature\QueueTestModel to queue...')
            ->assertExitCode(0);

        Queue::assertPushed(SyncModelJob::class, function ($job) {
            return $job->getModelClass() === QueueTestModel::class;
        });

        Carbon::setTestNow();
    }

    public function test_job_executes_sync_logic()
    {
        DB::table('queue_test_models')->insert([
            ['name' => 'JobTest'],
        ]);

        $mockBigQuery = Mockery::mock('overload:Google\\Cloud\\BigQuery\\BigQueryClient');
        $mockDataset = Mockery::mock('Google\\Cloud\\BigQuery\\Dataset');
        $mockTable = Mockery::mock('Google\\Cloud\\BigQuery\\Table');
        $mockResponse = Mockery::mock('Google\\Cloud\\BigQuery\\InsertResponse');

        $mockBigQuery->shouldReceive('dataset')->with('test_dataset')->andReturn($mockDataset);
        $mockDataset->shouldReceive('table')->with('queue_test_models')->andReturn($mockTable);
        $mockTable->shouldReceive('insertRows')->andReturn($mockResponse);
        $mockResponse->shouldReceive('isSuccessful')->andReturn(true);

        $job = new SyncModelJob(QueueTestModel::class);
        $job->handle();

        $this->assertEquals(1, DB::table('bigquery_syncs')->where('status', 'completed')->count());
        $this->assertEquals(1, DB::table('bigquery_syncs')->where('records_synced', 1)->count());
    }
}

class QueueTestModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'queue_test_models';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->fieldsToSync = ['id', 'name'];
        $this->syncSchedule = '* * * * *';
    }
}
