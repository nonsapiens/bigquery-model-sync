<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Nonsapiens\BigqueryModelSync\Tests\TestCase;

class ConfigTest extends TestCase
{
    /** @test */
    public function it_has_default_config_values()
    {
        $this->assertEquals(env('GOOGLE_CLOUD_PROJECT'), config('bigquery.projectId'));
        $this->assertEquals(env('BIGQUERY_DATASET'), config('bigquery.dataset'));
    }

    /** @test */
    public function it_can_be_overridden()
    {
        config(['bigquery.projectId' => 'test-project']);
        $this->assertEquals('test-project', config('bigquery.projectId'));
    }
}
