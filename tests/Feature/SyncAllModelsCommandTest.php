<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Carbon\Carbon;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Process;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;

class SyncAllModelsCommandTest extends TestCase
{
    protected function getEnvironmentSetUp($app)
    {
        $app['config']->set('bigquery.projectId', 'test-project');
        $app['config']->set('bigquery.dataset', 'test_dataset');
    }

    public function test_sync_all_discovers_and_runs_due_models()
    {
        Carbon::setTestNow(Carbon::create(2026, 4, 12, 10, 0, 0));

        Config::set('bigquery.models', [
            SyncAllTestModel1::class,
            SyncAllTestModel2::class,
            SyncAllTestModelNotDue::class,
        ]);

        Process::fake([
            'php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1" --force' => Process::result('Success 1', '', 0),
            'php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel2" --force' => Process::result('Success 2', '', 0),
        ]);

        $this->artisan('bigquery:sync-all')
            ->expectsOutputToContain('Syncing models: Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1, Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel2')
            ->expectsOutputToContain('Successfully synced Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1.')
            ->expectsOutputToContain('Successfully synced Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel2.')
            ->assertExitCode(0);

        Process::assertRan('php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1" --force');
        Process::assertRan('php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel2" --force');
        Process::assertNotRan('php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModelNotDue" --force');

        Carbon::setTestNow();
    }

    public function test_sync_all_logs_failures()
    {
        Carbon::setTestNow(Carbon::create(2026, 4, 12, 10, 0, 0));

        Config::set('bigquery.models', [
            SyncAllTestModel1::class,
        ]);

        Process::fake([
            'php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1" --force' => Process::result('', 'Error occurred', 1),
        ]);

        Log::shouldReceive('error')
            ->once()
            ->withArgs(function ($message, $context) {
                return str_contains($message, 'Failed to sync Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1')
                    && $context['exit_code'] === 1
                    && trim($context['error']) === 'Error occurred';
            });
        
        Log::shouldReceive('info')->atLeast()->once();

        $this->artisan('bigquery:sync-all')
            ->expectsOutputToContain('Failed to sync Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1: Error occurred')
            ->assertExitCode(1);

        Carbon::setTestNow();
    }

    public function test_sync_all_discovers_models_in_specified_namespace()
    {
        Carbon::setTestNow(Carbon::create(2026, 4, 12, 10, 0, 0));

        // Use this test's namespace for discovery
        Config::set('bigquery.syncable-namespaces', [
            'Nonsapiens\\BigqueryModelSync\\Tests\\Feature\\'
        ]);
        Config::set('bigquery.models', null);

        // We MUST fake all discovered models because they will all be "due" (most don't have schedule property, so default to null and might be skipped, but some do)
        // Actually, only models with $syncSchedule are synced.
        Process::fake([
            'php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel1" --force' => Process::result('Success 1', '', 0),
            'php artisan bigquery:sync --class="Nonsapiens\BigqueryModelSync\Tests\Feature\SyncAllTestModel2" --force' => Process::result('Success 2', '', 0),
            'php artisan bigquery:sync --class="*" --force' => Process::result('Success', '', 0),
        ]);

        $this->artisan('bigquery:sync-all')
            ->expectsOutputToContain('Syncing models:')
            ->assertExitCode(0);

        Carbon::setTestNow();
    }
}

class SyncAllTestModel1 extends Model
{
    use SyncsToBigQuery;
    protected $syncSchedule = '0 * * * *';
}

class SyncAllTestModel2 extends Model
{
    use SyncsToBigQuery;
    protected $syncSchedule = '*/30 * * * *';
}

class SyncAllTestModelNotDue extends Model
{
    use SyncsToBigQuery;
    protected $syncSchedule = '15 * * * *';
}
