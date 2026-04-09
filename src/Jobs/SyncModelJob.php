<?php

namespace Nonsapiens\BigqueryModelSync\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Database\Eloquent\Model;
use Nonsapiens\BigqueryModelSync\Enums\BigQuerySyncStrategy;
use Nonsapiens\BigqueryModelSync\Strategies\BatchSyncStrategy;

class SyncModelJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(protected string $modelClass)
    {
    }

    public function getModelClass(): string
    {
        return $this->modelClass;
    }

    public function handle(): void
    {
        if (!class_exists($this->modelClass)) {
            return;
        }

        $model = new $this->modelClass();
        if (!$model instanceof Model) {
            return;
        }

        // Select strategy
        $strategy = $model->syncStrategy ?? BigQuerySyncStrategy::BATCH;
        if ($strategy !== BigQuerySyncStrategy::BATCH) {
            // Placeholder for other strategies
            return;
        }

        (new BatchSyncStrategy())->sync($model);
    }
}
