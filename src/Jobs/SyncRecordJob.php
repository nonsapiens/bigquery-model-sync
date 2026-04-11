<?php

namespace Nonsapiens\BigqueryModelSync\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Database\Eloquent\Model;
use Nonsapiens\BigqueryModelSync\Strategies\OnInsertSyncStrategy;

class SyncRecordJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(public Model $model)
    {
    }

    public function getModelClass(): string
    {
        return get_class($this->model);
    }

    public function handle(): void
    {
        /** @var \Nonsapiens\BigqueryModelSync\Traits\SyncsToBigQuery $model */
        $this->model->sync();
    }
}
