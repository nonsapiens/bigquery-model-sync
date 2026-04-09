<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;

class MakeBigQueryTableCommandTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        // Default: allow other DB facade calls to pass-through or be ignored
        DB::shouldReceive('connection')->andReturnSelf()->byDefault();
    }

    public function test_fails_when_class_not_found_without_namespace(): void
    {
        $this->artisan('make:bigquery-table', ['--class' => 'NonExistingModel'])
            ->expectsOutputToContain('Class NonExistingModel not found')
            ->assertExitCode(1);
    }

    public function test_fails_when_not_an_eloquent_model(): void
    {
        $fqcn = NonEloquentDummy::class;
        $this->artisan('make:bigquery-table', ['--class' => $fqcn])
            ->expectsOutputToContain("Class {$fqcn} is not an Eloquent model.")
            ->assertExitCode(1);
    }

    public function test_fails_when_model_missing_trait(): void
    {
        $fqcn = NoTraitModel::class;

        DB::shouldReceive('select')->andReturn($this->columnsForBasicTable())->once();

        $this->artisan('make:bigquery-table', ['--class' => $fqcn])
            ->expectsOutputToContain("Class {$fqcn} does not use the SyncsToBigQuery trait.")
            ->assertExitCode(1);
    }

    public function test_generates_sql_with_geodata_mapping_and_defaults(): void
    {
        $fqcn = GeoModel::class;

        DB::shouldReceive('select')->withArgs(function ($query) {
            return str_contains($query, 'SHOW COLUMNS FROM `geo_table`');
        })->andReturn($this->columnsForGeoTable())->once();

        $this->artisan('make:bigquery-table', ['--class' => $fqcn])
            ->expectsChoice('Select a field to partition by (optional):', 'None', ['id', 'created_at', 'None'])
            ->expectsQuestion('Do you want to proceed with this SQL generation?', true)
            ->expectsOutputToContain('CREATE TABLE `bq_geo_table`')
            ->expectsOutputToContain('`location` GEOGRAPHY')
            ->doesntExpectOutputToContain('`latitude`')
            ->doesntExpectOutputToContain('`longitude`')
            ->assertExitCode(0);
    }

    public function test_generates_sql_with_partition_and_clustering(): void
    {
        $fqcn = PartitionClusterModel::class;

        DB::shouldReceive('select')->withArgs(function ($query) {
            return str_contains($query, 'SHOW COLUMNS FROM `pc_table`');
        })->andReturn($this->columnsForPartitionClusterTable())->once();

        $this->artisan('make:bigquery-table', ['--class' => $fqcn])
            ->expectsChoice('Select a field to partition by (optional):', 'event_date', ['id', 'event_date', 'user_id', 'None'])
            ->expectsChoice('Select up to 4 fields to cluster by (comma separated numbers, optional):', ['user_id', 'name'], ['id', 'event_date', 'user_id', 'name'])
            ->expectsQuestion('Do you want to proceed with this SQL generation?', true)
            ->expectsOutputToContain('PARTITION BY DATE(event_date)')
            ->expectsOutputToContain('CLUSTER BY user_id, name')
            ->assertExitCode(0);
    }

    private function columnsForBasicTable(): array
    {
        return [
            (object)['Field' => 'id', 'Type' => 'int(11)', 'Null' => 'NO'],
            (object)['Field' => 'name', 'Type' => 'varchar(100)', 'Null' => 'YES'],
        ];
    }

    private function columnsForGeoTable(): array
    {
        return [
            (object)['Field' => 'id', 'Type' => 'int(11)', 'Null' => 'NO'],
            (object)['Field' => 'name', 'Type' => 'varchar(100)', 'Null' => 'YES'],
            (object)['Field' => 'created_at', 'Type' => 'timestamp', 'Null' => 'YES'],
            (object)['Field' => 'latitude', 'Type' => 'decimal(10,6)', 'Null' => 'YES'],
            (object)['Field' => 'longitude', 'Type' => 'decimal(10,6)', 'Null' => 'YES'],
        ];
    }

    private function columnsForPartitionClusterTable(): array
    {
        return [
            (object)['Field' => 'id', 'Type' => 'int(11)', 'Null' => 'NO'],
            (object)['Field' => 'event_date', 'Type' => 'date', 'Null' => 'YES'],
            (object)['Field' => 'user_id', 'Type' => 'int(11)', 'Null' => 'YES'],
            (object)['Field' => 'name', 'Type' => 'varchar(50)', 'Null' => 'YES'],
        ];
    }
}

// Dummy classes used in tests
class NonEloquentDummy {}

class NoTraitModel extends Model
{
    protected $table = 'basic_table';
}

class GeoModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'geo_table';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->bigQueryTableName = 'bq_geo_table';
        $this->fieldsToSync = ['id', 'name', 'created_at', 'latitude', 'longitude'];
        $this->hasGeodata = true;
        $this->mappedGeographyField = 'location';
    }
}

class PartitionClusterModel extends Model
{
    use SyncsToBigQuery;

    protected $table = 'pc_table';

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->bigQueryTableName = 'bq_pc_table';
        $this->fieldsToSync = ['id', 'event_date', 'user_id', 'name'];
        $this->hasGeodata = false;
    }
}
