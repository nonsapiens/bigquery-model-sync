<?php

namespace Nonsapiens\BigqueryModelSync\Strategies;

use Illuminate\Database\Eloquent\Model;

abstract class SyncStrategy
{
    /**
     * Execute the sync for the given model.
     *
     * @param Model $model Instance of a model using SyncsToBigQuery trait
     */
    abstract public function sync(Model $model): void;
}
