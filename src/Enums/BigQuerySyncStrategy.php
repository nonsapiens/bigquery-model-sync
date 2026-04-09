<?php

namespace Nonsapiens\BigqueryModelSync\Enums;

enum BigQuerySyncStrategy: string
{
    case ON_INSERT = 'create';
    case BATCH = 'batch';

    case REPLACE = 'replace';
}
