<?php

namespace Nonsapiens\BigqueryModelSync\Tests\Feature;

use Illuminate\Database\Eloquent\Model;
use Nonsapiens\BigqueryModelSync\Tests\TestCase;
use Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery;

class TraitConflictTest extends TestCase
{
    public function test_trait_property_conflict()
    {
        // This should trigger a PHP Fatal error if the trait and model define the same property
        // But since we are in the same process, we might need to use eval or just try to instantiate a class that we define here.
        
        try {
            $model = new class extends Model {
                use SyncsToBigQuery;
                public array $fieldsToSync = ['id', 'name'];
            };
            $this->assertNotNull($model);
        } catch (\Throwable $e) {
            $this->fail("Failed to instantiate model with trait property: " . $e->getMessage());
        }
    }
}
