<?php

namespace Nonsapiens\BigqueryModelSync\Models;

use Illuminate\Database\Eloquent\Model;

class BigQuerySync extends Model
{
    protected $table = 'bigquery_syncs';

    protected $fillable = [
        'model',
        'sync_batch_uuid',
        'sync_type',
        'records_synced',
        'status',
        'error_message',
        'started_at',
        'completed_at',
    ];

    protected $casts = [
        'started_at' => 'datetime',
        'completed_at' => 'datetime',
    ];
}
